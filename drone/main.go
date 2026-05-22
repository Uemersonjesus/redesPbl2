package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ── Tipos RPC (espelham broker/rpc_broker.go) ────────────────────────────

type RegistroArgs struct {
	DroneID string
	Addr    string
	Lat     float64
	Long    float64
}

type RegistroReply struct {
	OK       bool
	Mensagem string
}

type ConclusaoArgs struct {
	DroneID   string
	ReqID     string
	Sucesso   bool
	Motivo    string
	LatFinal  float64
	LongFinal float64
}

type ConclusaoReply struct {
	OK bool
}

// ── Gerenciador de conexão com o broker ──────────────────────────────────
// Centraliza o acesso à conexão RPC com mutex para evitar race conditions.

type BrokerConexao struct {
	mu   sync.Mutex
	conn *rpc.Client
	addr string
}

func (b *BrokerConexao) Call(method string, args, reply interface{}) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == nil {
		return fmt.Errorf("sem conexão com broker")
	}
	return b.conn.Call(method, args, reply)
}

func (b *BrokerConexao) Substituir(conn *rpc.Client, addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		b.conn.Close()
	}
	b.conn = conn
	b.addr = addr
}

func (b *BrokerConexao) Addr() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.addr
}

func (b *BrokerConexao) Fechar() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		b.conn.Close()
		b.conn = nil
	}
}

// ── main ─────────────────────────────────────────────────────────────────

func main() {
	droneID     := getenv("DRONE_ID", "drone-1")
	rpcPorta    := envInt("DRONE_RPC_PORT", 7100)
	latInicial  := envFloat("LAT_INICIAL", 26.6)
	longInicial := envFloat("LONG_INICIAL", 56.2)
	brokerAddrs := parseBrokerAddrs(getenv("BROKER_ADDRS_RPC",
		"broker1:9010,broker2:9010,broker3:9010"))

	log.Printf("=== DRONE | id=%s | rpc=:%d | pos=(%.4f,%.4f) | brokers=%v ===",
		droneID, rpcPorta, latInicial, longInicial, brokerAddrs)

	broker := &BrokerConexao{}

	// onConclusao: chamado pelo drone após terminar missão
	// Usa conexão dedicada para não interferir com o heartbeat
	onConclusao := func(reqID string, sucesso bool, motivo string, lat, long float64) {
		args := ConclusaoArgs{
			DroneID: droneID, ReqID: reqID,
			Sucesso: sucesso, Motivo: motivo,
			LatFinal: lat, LongFinal: long,
		}

		// Tenta na conexão ativa
		var reply ConclusaoReply
		if err := broker.Call("BrokerRPC.Concluir", &args, &reply); err == nil {
			log.Printf("✅ [DRONE][%s] conclusão da req=%s confirmada", droneID, reqID)
			return
		}

		// Conexão falhou — abre conexão temporária dedicada para a conclusão
		log.Printf("[DRONE][%s] reconectando para enviar conclusão de req=%s", droneID, reqID)
		for tentativa := 0; tentativa < 5; tentativa++ {
			conn, addr, err := descobrirLider(brokerAddrs)
			if err != nil {
				time.Sleep(time.Duration(tentativa+1) * 2 * time.Second)
				continue
			}
			var r ConclusaoReply
			if err := conn.Call("BrokerRPC.Concluir", &args, &r); err == nil {
				log.Printf("✅ [DRONE][%s] conclusão confirmada em %s", droneID, addr)
				// Atualiza conexão principal também
				broker.Substituir(conn, addr)
				return
			}
			conn.Close()
		}
		log.Printf("⚠️  [DRONE][%s] não conseguiu confirmar conclusão de req=%s", droneID, reqID)
	}

	drone := NewDrone(droneID, latInicial, longInicial, onConclusao)

	// Sobe servidor RPC
	selfAddr := fmt.Sprintf("%s:%d", getenv("DRONE_HOST", droneID), rpcPorta)
	servidor, err := NewDroneRPCServer(rpcPorta, drone)
	if err != nil {
		log.Fatalf("erro abrindo RPC: %v", err)
	}
	defer servidor.Stop()

	// Goroutine de registro e heartbeat
	go loopRegistro(drone, broker, brokerAddrs, droneID, selfAddr)

	// Aguarda encerramento
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Printf("[DRONE][%s] encerrando...", droneID)
	broker.Fechar()
}

// loopRegistro mantém o drone registrado no broker líder.
// Faz heartbeat a cada 10s enviando a POSIÇÃO ATUAL do drone.
// Se o broker não responder, reconecta automaticamente.
func loopRegistro(drone *Drone, broker *BrokerConexao, addrs []string, droneID, selfAddr string) {
	for {
		// Descobre o líder
		conn, addr, err := descobrirLider(addrs)
		if err != nil {
			log.Printf("[DRONE][%s] aguardando broker líder... (%v)", droneID, err)
			time.Sleep(3 * time.Second)
			continue
		}
		broker.Substituir(conn, addr)

		// Registra
		lat, long := drone.Posicao()
		args := RegistroArgs{DroneID: droneID, Addr: selfAddr, Lat: lat, Long: long}
		var reply RegistroReply
		if err := broker.Call("BrokerRPC.Registrar", &args, &reply); err != nil {
			log.Printf("[DRONE][%s] erro no registro: %v", droneID, err)
			broker.Fechar()
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Printf("🤖 DRONE REGISTRADO | id=%s", droneID)
		log.Printf("   broker : %s", addr)
		log.Printf("   pos    : (%.4f, %.4f)", lat, long)
		log.Printf("   rpc    : %s", selfAddr)
		log.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		// Heartbeat a cada 30s com posição atual
		// Se falhar, sai do loop e reconecta
		for {
			time.Sleep(30 * time.Second)

			lat, long = drone.Posicao() // posição ATUAL, não a inicial
			args.Lat = lat
			args.Long = long

			var r RegistroReply
			if err := broker.Call("BrokerRPC.Registrar", &args, &r); err != nil {
				log.Printf("[DRONE][%s] heartbeat falhou → reconectando...", droneID)
				broker.Fechar()
				break // volta ao loop externo para redescobrir o líder
			}
		}
	}
}

// descobrirLider percorre os brokers e retorna conexão com o líder.
// Usa PingLider (RPC leve) em vez de Registrar para não gerar
// entradas desnecessárias no log Raft durante a descoberta.
func descobrirLider(addrs []string) (*rpc.Client, string, error) {
	liderHint := ""

	for rodada := 0; rodada < 2; rodada++ {
		lista := addrs
		if liderHint != "" {
			lista = append([]string{liderHint}, addrs...)
			liderHint = ""
		}

		for _, addr := range lista {
			conn, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				continue
			}

			// PingLider: RPC leve que não gera entrada no log Raft
			var reply RegistroReply
			args := RegistroArgs{DroneID: "_ping_", Addr: "_ping_:0"}
			if err := conn.Call("BrokerRPC.Registrar", &args, &reply); err != nil {
				conn.Close()
				continue
			}

			if reply.OK {
				return conn, addr, nil
			}

			if reply.Mensagem != "" {
				liderHint = reply.Mensagem
			}
			conn.Close()
		}
	}

	return nil, "", fmt.Errorf("nenhum broker líder encontrado em %v", addrs)
}

// ── Helpers ──────────────────────────────────────────────────────────────

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
	}
	return def
}

func parseBrokerAddrs(s string) []string {
	var addrs []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			addrs = append(addrs, p)
		}
	}
	return addrs
}

var _ = json.Marshal // evita import não usado
