package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// -----------------------------------------------------------------------
// Tipos RPC (espelhados do broker para compilação independente)
// -----------------------------------------------------------------------

type DespachoArgs struct {
	ReqID      string
	SetorID    string
	Lat        float64
	Long       float64
	Prioridade int
	Descricao  string
}

type DespachoReply struct {
	Aceito        bool
	TempoEstimado int
	Mensagem      string
}

type StatusArgs struct {
	DroneID string
}

type StatusReply struct {
	Status   string
	ReqAtual string
}

// -----------------------------------------------------------------------
// DroneRPC — servidor RPC do container do drone
// -----------------------------------------------------------------------

type DroneRPC struct {
	drone *Drone
}

// Despachar é chamado pelo broker líder quando há uma missão para este drone.
func (d *DroneRPC) Despachar(args *DespachoArgs, reply *DespachoReply) error {
	if !d.drone.EstaLivre() {
		reply.Aceito = false
		reply.Mensagem = "drone ocupado"
		return nil
	}

	tempo := d.drone.IniciarMissao(args)
	reply.Aceito = true
	reply.TempoEstimado = tempo
	reply.Mensagem = fmt.Sprintf("missão aceita, tempo estimado: %ds", tempo)

	log.Printf("[DRONE][%s] missão aceita | req=%s setor=%s prio=%d tempo=%ds",
		d.drone.ID, args.ReqID, args.SetorID, args.Prioridade, tempo)
	return nil
}

// Status é chamado pelo broker para verificar se o drone está vivo.
func (d *DroneRPC) Status(args *StatusArgs, reply *StatusReply) error {
	reply.Status = string(d.drone.Status())
	reply.ReqAtual = d.drone.ReqAtual()
	return nil
}

// -----------------------------------------------------------------------
// Servidor RPC do drone
// -----------------------------------------------------------------------

type DroneRPCServer struct {
	porta    int
	listener net.Listener
}

func NewDroneRPCServer(porta int, drone *Drone) (*DroneRPCServer, error) {
	srv := rpc.NewServer()
	srv.Register(&DroneRPC{drone: drone})

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, srv)
	mux.Handle(rpc.DefaultDebugPath, srv)

	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", porta))
	if err != nil {
		return nil, fmt.Errorf("abrindo RPC drone porta %d: %w", porta, err)
	}

	go http.Serve(ln, mux)
	log.Printf("[DRONE][%s] RPC escutando em 0.0.0.0:%d", drone.ID, porta)

	return &DroneRPCServer{porta: porta, listener: ln}, nil
}

func (s *DroneRPCServer) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
}
