package main

import (
	"REDESPBL22026/broker"
	"REDESPBL22026/config"
	"REDESPBL22026/raft"
	"encoding/json"
	"fmt"
	"math"
	"log"
	"os"
	"net/http"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"sync"
	"time"
)

func main() {
	// ── Config ──────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	self, err := cfg.Self()
	if err != nil {
		log.Fatalf("self: %v", err)
	}

	tcpPort     := envInt("BROKER_TCP_PORT", 9001)
	droneRPCPort := envInt("BROKER_DRONE_RPC_PORT", 9010) // porta onde drones se registram

	log.Printf("=== BROKER | nó=%s | raft=%s | tcp=:%d | drone-rpc=:%d ===",
		self.ID, self.RaftAddr, tcpPort, droneRPCPort)

	// ── Raft ────────────────────────────────────────────────────────
	peers := cfg.PeerAddrs()
	raftNode := raft.NewNode(self.ID, peers, cfg.ElectionTimeoutMs, cfg.HeartbeatMs)
	// Escuta sempre em 0.0.0.0 — o IP real fica no PEERS para os peers conectarem.
	// Se o servidor escutasse no IP do host (ex: 192.168.1.10:7001),
	// o container não teria essa interface e daria "bind: cannot assign requested address".
	raftListenAddr := "0.0.0.0:" + portaDe(self.RaftAddr)
	if err := raft.StartServer(raftNode, raftListenAddr); err != nil {
		log.Fatalf("raft: %v", err)
	}
	// Aguarda peers subirem antes de iniciar eleições
	time.Sleep(1 * time.Second)
	raftNode.Start()

	// ── Estado distribuído ─────────────────────────────────────────
	fila      := broker.NewFilaPrioridade()
	registry  := broker.NewRegistry()
	scheduler := broker.NewScheduler()

	// ── Funções de propose (replicação via Raft) ────────────────────
	proposeCmd := func(cmd raft.Command) error {
		data, _ := json.Marshal(cmd)
		return raftNode.Propose(data)
	}

	propose := func(reqID, sensorID, setorID, tipo string, prio int, lat, long float64) error {
		return proposeCmd(raft.Command{
			Tipo: raft.CmdInserir, ReqID: reqID,
			SensorID: sensorID, SetorID: setorID,
			TipoSensor: tipo, Prioridade: prio,
			Lat: lat, Long: long,
		})
	}

	proposeRegistrarDrone := func(droneID, addr string, lat, long float64) {
		proposeCmd(raft.Command{
			Tipo: raft.CmdRegistrarDrone,
			DroneID: droneID, DroneAddr: addr,
			DroneLat: lat, DroneLong: long,
		})
	}

	proposeDroneStatus := func(droneID, reqID, status string) {
		proposeCmd(raft.Command{
			Tipo: raft.CmdDroneStatus,
			DroneID: droneID, ReqID: reqID, Status: status,
		})
	}

	proposeAlocar := func(reqID, droneID string) {
		proposeCmd(raft.Command{
			Tipo: raft.CmdAlocar, ReqID: reqID, DroneID: droneID,
		})
	}

	proposeConcluir := func(reqID, droneID string, sucesso bool, motivo string, lat, long float64) {
		status := "concluido"
		if !sucesso {
			status = "falha"
		}
		proposeCmd(raft.Command{
			Tipo: raft.CmdConcluir, ReqID: reqID, DroneID: droneID,
			Status: status, Motivo: motivo,
			DroneLat: lat, DroneLong: long,
		})
	}

	// ── Apply loop — aplica log Raft no estado local (todos os brokers) ─
	go func() {
		for entry := range raftNode.ApplyCh() {
			if len(entry.Command) == 0 {
				continue
			}
			var cmd raft.Command
			if err := json.Unmarshal(entry.Command, &cmd); err != nil {
				continue
			}
			switch cmd.Tipo {
			case raft.CmdInserir:
				fila.Aplicar(&broker.Requisicao{
					ID: cmd.ReqID, SensorID: cmd.SensorID,
					SetorID: cmd.SetorID, Tipo: cmd.TipoSensor,
					Prioridade: cmd.Prioridade,
					Lat: cmd.Lat, Long: cmd.Long,
					Timestamp: time.Now(),
				})

			case raft.CmdRegistrarDrone:
				jaExistia := registry.Existe(cmd.DroneID)
				registry.Registrar(cmd.DroneID, cmd.DroneAddr, cmd.DroneLat, cmd.DroneLong)
				if !jaExistia {
					broker.ImprimirRegistroDrone(cmd.DroneID, cmd.DroneAddr, registry.Total())
				}

			case raft.CmdDroneStatus:
				registry.AtualizarStatus(cmd.DroneID, cmd.ReqID, broker.StatusDrone(cmd.Status))

			case raft.CmdAlocar:
				fila.AplicarAlocacao(cmd.ReqID, cmd.DroneID)
				// Busca o setorID da requisição para registrar no drone
				if req, ok := fila.Consultar(cmd.ReqID); ok {
					registry.AtualizarStatusComSetor(cmd.DroneID, cmd.ReqID, req.SetorID, broker.DroneStatusOcupado)
				} else {
					registry.AtualizarStatus(cmd.DroneID, cmd.ReqID, broker.DroneStatusOcupado)
				}

			case raft.CmdConcluir:
				req, _ := fila.Consultar(cmd.ReqID)
				if req != nil {
					registry.AtualizarPosicao(cmd.DroneID, cmd.DroneLat, cmd.DroneLong)
				}
				registry.AtualizarStatus(cmd.DroneID, "", broker.DroneStatusLivre)
				fila.AplicarConclusao(cmd.ReqID)
				if cmd.Status == "falha" {
					broker.ImprimirFalha(cmd.DroneID, cmd.ReqID, cmd.Motivo)
					if req != nil {
						req.Status = "na_fila"
						fila.Aplicar(req)
					}
				} else {
					broker.ImprimirConclusao(cmd.DroneID, cmd.ReqID, fila.Tamanho())
					// Registra no histórico do painel
					if req != nil {
						broker.AdicionarMissaoConcluida(broker.MissaoConcluida{
							Timestamp: time.Now(),
							DroneID:   cmd.DroneID,
							ReqID:     cmd.ReqID,
							SetorID:   req.SetorID,
							Prio:      req.Prioridade,
							Dist:      0,
						})
					}
				}
			}
		}
	}()

	// ── Servidor RPC para drones se registrarem ─────────────────────
	// liderRPCAddr retorna o endereço RPC (porta 9010) do líder atual
	liderRPCAddr := func() string {
		leaderID := raftNode.LeaderID()
		if leaderID == "" {
			return ""
		}
		// Extrai host do RaftAddr e usa porta 9010
		for _, p := range cfg.Peers {
			if p.ID == leaderID {
				host := p.RaftAddr
				for i := len(host) - 1; i >= 0; i-- {
					if host[i] == ':' {
						host = host[:i]
						break
					}
				}
				return fmt.Sprintf("%s:%d", host, droneRPCPort)
			}
		}
		return ""
	}

	brokerRPCHandler := broker.NewBrokerRPC(
		registry,
		raftNode.IsLeader,
		liderRPCAddr,
		// onRegistro: replica via Raft
		func(droneID, addr string, lat, long float64) {
			proposeRegistrarDrone(droneID, addr, lat, long)
		},
		// onConclusao: replica via Raft
		func(droneID, reqID string, sucesso bool, motivo string, lat, long float64) {
			proposeConcluir(reqID, droneID, sucesso, motivo, lat, long)
		},
	)
	brokerRPCSrv := broker.NewBrokerRPCServer(droneRPCPort, brokerRPCHandler)
	if err := brokerRPCSrv.Start(); err != nil {
		log.Fatalf("drone-rpc: %v", err)
	}

	// ── Dispatcher com Scheduler ────────────────────────────────────
	// Roda só no líder: seleciona req + drone e chama RPC no drone
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if !raftNode.IsLeader() {
				continue
			}
			req, drone := scheduler.Selecionar(fila, registry)
			if req == nil || drone == nil {
				continue
			}

			// Marca como ocupado imediatamente (antes de chamar RPC)
			// para evitar que o mesmo drone seja selecionado duas vezes
			proposeDroneStatus(drone.ID, req.ID, "ocupado")
			proposeAlocar(req.ID, drone.ID)

			go func(r *broker.Requisicao, d *broker.DroneRegistrado) {
				args := &broker.DespachoArgs{
					ReqID:      r.ID,
					SetorID:    r.SetorID,
					Lat:        r.Lat,
					Long:       r.Long,
					Prioridade: r.Prioridade,
				}
				reply, err := broker.DespacharDrone(d.Addr, args)
				if err != nil || (reply != nil && !reply.Aceito) {
					motivo := "RPC não respondeu"
					if reply != nil {
						motivo = reply.Mensagem
					}
					log.Printf("⚠️  [DISPATCH] drone=%s INATIVO | req=%s | %s",
					d.ID, r.ID[:min(12, len(r.ID))], motivo)
					registry.MarcarInativo(d.ID)
					proposeDroneStatus(d.ID, "", "inativo")
					proposeCmd(raft.Command{
						Tipo: raft.CmdConcluir, ReqID: r.ID, DroneID: d.ID,
						Status: "falha", Motivo: motivo,
					})
				} else {
					broker.ImprimirDespacho(d.ID, r.SetorID, r.ID,
						r.Prioridade, 0.0, reply.TempoEstimado, fila.Tamanho())
				}
			}(req, drone)
		}
	}()

	// ── Health check de drones (só o líder verifica) ────────────────
	// Drones livres: ping a cada 30s
	// Drones ocupados: ping a cada 30s também — se morreu durante missão,
	//   marca inativo E reinsere a requisição na fila
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if !raftNode.IsLeader() {
				continue
			}
			for _, d := range registry.Todos() {
				if d.Status == broker.DroneStatusInativo {
					continue // já marcado
				}
				if !broker.PingDrone(d.Addr, d.ID) {
					if d.Status == broker.DroneStatusOcupado {
						log.Printf("[HEALTH] drone %s morreu durante missão req=%s → reinserindo na fila",
							d.ID, d.ReqAtual)
						proposeConcluir(d.ReqAtual, d.ID, false, "drone morreu durante missão", d.Lat, d.Long)
					} else {
						log.Printf("[HEALTH] drone %s não respondeu → inativo", d.ID)
					}
					registry.MarcarInativo(d.ID)
					proposeDroneStatus(d.ID, "", "inativo")
				}
			}
		}
	}()

	// ── Servidor WebSocket — sensores conectam aqui ─────────────────
	// Porta WS separada da porta TCP legado
	wsPort := envInt("BROKER_WS_PORT", 8090)

	// liderWSAddr: endereço WS externo do líder (usa WSPort do config para porta correta)
	liderWSAddr := func() string {
		return cfg.WSAddrForPeer(raftNode.LeaderID())
	}

	onAlerta := func(alerta broker.AlertaSensor) {
		// Registra no buffer para exibição no painel (todos os nós)
		adicionarAlerta(broker.AlertaLog{
			Timestamp: time.Now(),
			SensorID:  alerta.SensorID,
			SetorID:   alerta.SetorID,
			Tipo:      alerta.Tipo,
			Prio:      alerta.Prioridade,
			Descricao: alerta.Descricao,
		})
		broker.ImprimirAlertaSensor(alerta.SensorID, alerta.SetorID,
			alerta.Tipo, alerta.Descricao, alerta.Prioridade)

		if !raftNode.IsLeader() {
			return
		}
		reqID := fila.GerarID()
		propose(reqID, alerta.SensorID, alerta.SetorID, alerta.Tipo,
			alerta.Prioridade, alerta.Coordenadas.Lat, alerta.Coordenadas.Long)
	}

	wsSrv := broker.NewWSServer(raftNode.IsLeader, liderWSAddr, onAlerta)
	go func() {
		addr := fmt.Sprintf("0.0.0.0:%d", wsPort)
		log.Printf("[WS-SRV] escutando sensores em %s", addr)
		if err := http.ListenAndServe(addr, wsSrv.Handler()); err != nil {
			log.Printf("[WS-SRV] erro: %v", err)
		}
	}()

	// ── Servidor TCP para sensores legado ───────────────────────────
	liderAddr := func() string { return cfg.TCPAddrForPeer(raftNode.LeaderID()) }
	tcpSrv := broker.NewTCPServer(tcpPort, fila, raftNode.IsLeader, liderAddr, propose)
	if err := tcpSrv.Start(); err != nil {
		log.Fatalf("tcp: %v", err)
	}

	// ── Monitor de papel ─────────────────────────────────────────────
	go func() {
		for range raftNode.RoleChanges() {
			r, term := raftNode.State()
			log.Printf("[RAFT][%s] papel=%s mandato=%d", self.ID, r, term)
		}
	}()

	// ── Status periódico ─────────────────────────────────────────────
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			imprimirPainel(self.ID, raftNode, fila, registry)
		}
	}()

	// ── Encerramento ─────────────────────────────────────────────────
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Printf("[MAIN][%s] encerrando...", self.ID)
	raftNode.Stop()
	tcpSrv.Stop()
	brokerRPCSrv.Stop()
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}


var _ = fmt.Sprintf

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// bufferAlertas guarda os últimos alertas recebidos para exibição no painel.
var bufferAlertas []broker.AlertaLog
var bufferMu sync.Mutex

func adicionarAlerta(a broker.AlertaLog) {
	bufferMu.Lock()
	defer bufferMu.Unlock()
	bufferAlertas = append(bufferAlertas, a)
	if len(bufferAlertas) > 20 {
		bufferAlertas = bufferAlertas[len(bufferAlertas)-20:]
	}
}

func alertasRecentes() []broker.AlertaLog {
	bufferMu.Lock()
	defer bufferMu.Unlock()
	cp := make([]broker.AlertaLog, len(bufferAlertas))
	copy(cp, bufferAlertas)
	return cp
}

// imprimirPainel exibe o estado do cluster de forma visual e informativa.
func imprimirPainel(nodeID string, raftNode *raft.Node, fila *broker.FilaPrioridade, registry *broker.Registry) {
	role, term := raftNode.State()
	lider := raftNode.LeaderID()

	if role == raft.Leader {
		// Monta snapshot da fila
		total := fila.Tamanho()
		contadores := fila.ContadoresPorPrioridade()
		var proxSnap *broker.RequisicaoSnapshot
		if prox := fila.ProximoSnapshot(); prox != nil {
			proxSnap = &broker.RequisicaoSnapshot{
				ID:         prox.ID,
				SetorID:    prox.SetorID,
				Prioridade: prox.Prioridade,
			}
		}
		filaSnap := broker.FilaSnapshot{
			Total:      total,
			Contadores: contadores,
			Proxima:    proxSnap,
		}

		// Monta snapshot de drones com prioridade da missão atual
		dronesRaw := registry.SnapshotDrones()
		for i, d := range dronesRaw {
			if d.ReqAtual != "" {
				if req, ok := fila.Consultar(d.ReqAtual); ok {
					dronesRaw[i].Prio = req.Prioridade
					// Calcula distância atual do drone ao setor da missão
					dlat := req.Lat - d.Lat
					dlong := req.Long - d.Long
					dronesRaw[i].DistSetor = math.Sqrt(dlat*dlat+dlong*dlong) * 111.0
				}
			}
		}

		broker.ImprimirPainelLider(nodeID, term, filaSnap, dronesRaw, alertasRecentes())
	} else {
		broker.ImprimirPainelFollower(nodeID, term, lider, fila.Tamanho(),
			[2]int{registry.Livres(), registry.Total()})
	}
}

// portaDe extrai a porta de um endereço "host:porta".
func portaDe(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[i+1:]
		}
	}
	return addr
}
