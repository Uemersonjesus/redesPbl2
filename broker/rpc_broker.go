package broker

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

// -----------------------------------------------------------------------
// Tipos RPC broker ↔ drone
// -----------------------------------------------------------------------

// RegistroArgs é enviado pelo drone ao registrar-se no broker.
type RegistroArgs struct {
	DroneID string
	Addr    string  // endereço RPC do container do drone (host:porta)
	Lat     float64 // posição inicial (próximo ao setor 1 por padrão)
	Long    float64
}

type RegistroReply struct {
	OK      bool
	Mensagem string
}

// DespachoArgs é enviado pelo broker ao drone quando há missão.
type DespachoArgs struct {
	ReqID      string
	SetorID    string
	Lat        float64
	Long       float64
	Prioridade int
	Descricao  string
}

type DespachoReply struct {
	Aceito    bool
	TempoEstimado int // segundos
	Mensagem  string
}

// ConclusaoArgs é enviado pelo drone ao broker após terminar a missão.
type ConclusaoArgs struct {
	DroneID string
	ReqID   string
	Sucesso bool
	Motivo  string  // preenchido em caso de falha
	LatFinal  float64 // posição final do drone
	LongFinal float64
}

type ConclusaoReply struct {
	OK bool
}

// StatusArgs é enviado pelo drone para heartbeat / verificação.
type StatusArgs struct {
	DroneID string
}

type StatusReply struct {
	Status   string
	ReqAtual string
}

// -----------------------------------------------------------------------
// BrokerRPC — servidor RPC que os drones chamam
// -----------------------------------------------------------------------

type BrokerRPC struct {
	registry    *Registry
	isLider     func() bool
	liderAddr   func() string // retorna endereço RPC do líder atual
	onRegistro  func(droneID, addr string, lat, long float64)
	onConclusao func(droneID, reqID string, sucesso bool, motivo string, lat, long float64)
}

func NewBrokerRPC(
	registry *Registry,
	isLider func() bool,
	liderAddr func() string,
	onRegistro func(string, string, float64, float64),
	onConclusao func(string, string, bool, string, float64, float64),
) *BrokerRPC {
	return &BrokerRPC{
		registry:    registry,
		isLider:     isLider,
		liderAddr:   liderAddr,
		onRegistro:  onRegistro,
		onConclusao: onConclusao,
	}
}

// Registrar é chamado pelo drone ao subir.
// Se não somos o líder, indica o endereço do líder para o drone tentar lá.
func (b *BrokerRPC) Registrar(args *RegistroArgs, reply *RegistroReply) error {
	// Ignora probes/pings de descoberta — não replica no Raft
	if args.DroneID == "_probe_" || args.DroneID == "_ping_" {
		reply.OK = b.isLider()
		if !reply.OK {
			reply.Mensagem = b.liderAddr()
		}
		return nil
	}

	if !b.isLider() {
		reply.OK = false
		reply.Mensagem = b.liderAddr() // indica o líder ao drone
		log.Printf("[RPC-BROKER] registro de %s recusado — não sou líder, líder=%s",
			args.DroneID, reply.Mensagem)
		return nil
	}

	jaExistia := b.registry.Existe(args.DroneID)
	if !jaExistia {
		log.Printf("[RPC-BROKER] drone %s registrando-se | addr=%s lat=%.4f long=%.4f",
			args.DroneID, args.Addr, args.Lat, args.Long)
	}
	b.onRegistro(args.DroneID, args.Addr, args.Lat, args.Long)
	reply.OK = true
	reply.Mensagem = fmt.Sprintf("drone %s registrado com sucesso", args.DroneID)
	return nil
}

// Concluir é chamado pelo drone após terminar (ou falhar) uma missão.
func (b *BrokerRPC) Concluir(args *ConclusaoArgs, reply *ConclusaoReply) error {
	if args.Sucesso {
		log.Printf("[RPC-BROKER] drone %s concluiu req=%s | pos=(%.4f,%.4f)",
			args.DroneID, args.ReqID, args.LatFinal, args.LongFinal)
	} else {
		log.Printf("[RPC-BROKER] drone %s FALHOU req=%s | motivo=%s",
			args.DroneID, args.ReqID, args.Motivo)
	}
	b.onConclusao(args.DroneID, args.ReqID, args.Sucesso, args.Motivo, args.LatFinal, args.LongFinal)
	reply.OK = true
	return nil
}

// -----------------------------------------------------------------------
// Servidor RPC do broker (drones conectam aqui)
// -----------------------------------------------------------------------

type BrokerRPCServer struct {
	porta    int
	srv      *rpc.Server
	listener net.Listener
	stopCh   chan struct{}
}

func NewBrokerRPCServer(porta int, handler *BrokerRPC) *BrokerRPCServer {
	srv := rpc.NewServer()
	srv.Register(handler)
	return &BrokerRPCServer{
		porta:  porta,
		srv:    srv,
		stopCh: make(chan struct{}),
	}
}

func (s *BrokerRPCServer) Start() error {
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, s.srv)

	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", s.porta))
	if err != nil {
		return fmt.Errorf("RPC broker porta %d: %w", s.porta, err)
	}
	s.listener = ln
	log.Printf("[RPC-BROKER] escutando drones em 0.0.0.0:%d", s.porta)

	go func() {
		http.Serve(ln, mux)
	}()
	return nil
}

func (s *BrokerRPCServer) Stop() {
	close(s.stopCh)
	if s.listener != nil {
		s.listener.Close()
	}
}

// -----------------------------------------------------------------------
// Cliente RPC — broker chama os drones
// -----------------------------------------------------------------------

// DespacharDrone chama o drone via RPC para aceitar uma missão.
// Retorna false se o drone não responder (considerado inativo).
func DespacharDrone(addr string, args *DespachoArgs) (*DespachoReply, error) {
	conn, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("conectando drone %s: %w", addr, err)
	}
	defer conn.Close()

	var reply DespachoReply
	done := make(chan error, 1)
	go func() { done <- conn.Call("DroneRPC.Despachar", args, &reply) }()

	select {
	case err := <-done:
		if err != nil {
			return nil, err
		}
		return &reply, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout chamando drone %s", addr)
	}
}

// PingDrone verifica se o drone está vivo.
func PingDrone(addr string, droneID string) bool {
	conn, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return false
	}
	defer conn.Close()

	args := StatusArgs{DroneID: droneID}
	var reply StatusReply
	done := make(chan error, 1)
	go func() { done <- conn.Call("DroneRPC.Status", &args, &reply) }()

	select {
	case err := <-done:
		return err == nil
	case <-time.After(3 * time.Second):
		return false
	}
}
