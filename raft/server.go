package raft

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// RaftRPC é o receptor dos métodos RPC expostos via net/rpc.
// net/rpc exige que o receptor seja um tipo registrado; usamos este wrapper
// para delegar ao Node (que mantém o estado real).
type RaftRPC struct {
	node *Node
}

// RequestVote delega ao nó a lógica de votação.
func (r *RaftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return r.node.RequestVote(args, reply)
}

// AppendEntries delega ao nó a lógica de heartbeat/replicação.
func (r *RaftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return r.node.AppendEntries(args, reply)
}

// PreVote responde a pré-votações sem alterar o mandato atual.
func (r *RaftRPC) PreVote(args *PreVoteArgs, reply *PreVoteReply) error {
	return r.node.PreVote(args, reply)
}

// StartServer registra o serviço RPC e começa a escutar na addr fornecida.
// Roda em segundo plano; chame Stop() no Node para encerrar.
func StartServer(node *Node, addr string) error {
	srv := rpc.NewServer()
	if err := srv.Register(&RaftRPC{node: node}); err != nil {
		return err
	}

	// Usamos um ServeMux próprio para não poluir o http.DefaultServeMux
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, srv)
	mux.Handle(rpc.DefaultDebugPath, srv)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	log.Printf("[RAFT][%s] RPC escutando em %s", node.id, addr)
	go func() {
		if err := http.Serve(listener, mux); err != nil {
			select {
			case <-node.stopCh:
				// encerramento normal
			default:
				log.Printf("[RAFT] erro no servidor RPC: %v", err)
			}
		}
	}()

	return nil
}
