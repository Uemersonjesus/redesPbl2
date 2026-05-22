package raft

import (
	"log"
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

// Node é o nó Raft. Toda a lógica de eleição e replicação vive aqui.
type Node struct {
	mu sync.Mutex

	// Identidade
	id       string
	peerAddr []string // endereços RPC dos outros nós

	// Estado persistente (em produção seria gravado em disco)
	currentTerm int
	votedFor    string // "" = ninguém
	log         []LogEntry

	// Estado volátil
	commitIndex int
	lastApplied int

	// Estado volátil do líder (reiniciado após eleição)
	nextIndex  map[string]int
	matchIndex map[string]int

	// Papel atual
	role Role

	// Controle de tempo
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
	lastHeartbeat    time.Time // última vez que ouvimos do líder

	// Canal para notificar que o papel mudou (para log externo)
	roleChangeCh chan Role

	// Último líder conhecido (para redirecionar clientes)
	lastKnownLeader string

	// Canal onde entradas commitadas são enviadas para aplicação
	applyCh chan LogEntry

	// Sinalização de parada
	stopCh chan struct{}
}

// NewNode cria e configura um nó Raft ainda não iniciado.
func NewNode(id string, peers []string, electionMs, heartbeatMs int) *Node {
	n := &Node{
		id:               id,
		peerAddr:         peers,
		role:             Follower,
		votedFor:         "",
		electionTimeout:  randomElectionTimeout(electionMs),
		heartbeatTimeout: time.Duration(heartbeatMs) * time.Millisecond,
		// Inicializa no passado para que o election timeout expire imediatamente
		// e a primeira eleição possa ocorrer sem esperar.
		lastHeartbeat:    time.Now().Add(-2 * time.Second),
		roleChangeCh:     make(chan Role, 8),
		applyCh:          make(chan LogEntry, 256),
		stopCh:           make(chan struct{}),
		nextIndex:        make(map[string]int),
		matchIndex:       make(map[string]int),
	}
	// Entrada sentinela no índice 0 (log é 1-based)
	n.log = []LogEntry{{Term: 0, Index: 0}}
	return n
}

// Start inicia as goroutines principais do nó.
func (n *Node) Start() {
	log.Printf("[RAFT][%s] iniciando como %s no mandato %d", n.id, n.role, n.currentTerm)
	go n.ticker()
}

// Stop encerra o nó de forma ordenada.
func (n *Node) Stop() {
	close(n.stopCh)
}

// RoleChanges retorna o canal de notificação de mudança de papel.
func (n *Node) RoleChanges() <-chan Role {
	return n.roleChangeCh
}

// IsLeader reporta se este nó é atualmente o líder.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// State retorna papel e mandato atuais (thread-safe).
func (n *Node) State() (Role, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.currentTerm
}

// -----------------------------------------------------------------------
// ticker — loop principal: verifica timeouts e aciona eleições/heartbeats
// -----------------------------------------------------------------------

func (n *Node) ticker() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		role := n.role
		n.mu.Unlock()

		switch role {
		case Follower, Candidate:
			n.checkElectionTimeout()
		case Leader:
			n.sendHeartbeats()
			time.Sleep(n.heartbeatTimeout)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func (n *Node) checkElectionTimeout() {
	n.mu.Lock()
	elapsed := time.Since(n.lastHeartbeat)
	timeout := n.electionTimeout
	n.mu.Unlock()

	if elapsed >= timeout {
		n.startElection()
	}
}

// -----------------------------------------------------------------------
// Eleição
// -----------------------------------------------------------------------

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.lastHeartbeat = time.Now()
	n.electionTimeout = randomElectionTimeout(int(n.electionTimeout.Milliseconds()))

	term := n.currentTerm
	lastIdx, lastTerm := n.lastLogIndexTerm()

	log.Printf("[RAFT][%s] iniciando eleição | mandato=%d", n.id, term)
	n.mu.Unlock()

	n.notifyRole(Candidate)

	votes := 1
	majority := (len(n.peerAddr)+1)/2 + 1
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range n.peerAddr {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply
			if err := call(addr, "RaftRPC.RequestVote", &args, &reply); err != nil {
				log.Printf("[RAFT][%s] RequestVote -> %s: %v", n.id, addr, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
				return
			}
			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				v := votes
				voteMu.Unlock()
				if v >= majority && n.role == Candidate && n.currentTerm == term {
					n.becomeLeader()
				}
			}
		}(addr)
	}

	wg.Wait()
}

// -----------------------------------------------------------------------
// RPC handlers (chamados pelo servidor RPC)
// -----------------------------------------------------------------------

// RequestVote processa pedido de voto de um candidato.
func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	if args.Term < n.currentTerm {
		return nil // mandato antigo: nega
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}

	// Concede voto se ainda não votou (ou já votou no mesmo candidato)
	// e o log do candidato é ao menos tão atualizado quanto o nosso.
	lastIdx, lastTerm := n.lastLogIndexTerm()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOK {
		n.votedFor = args.CandidateID
		n.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		log.Printf("[RAFT][%s] voto concedido para %s (mandato %d)", n.id, args.CandidateID, args.Term)
	}
	reply.Term = n.currentTerm
	return nil
}

// AppendEntries processa heartbeat e replicação de entradas do líder.
func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.Success = false

	if args.Term < n.currentTerm {
		return nil // líder desatualizado
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}

	// Reconhecemos um líder válido: resetamos o timer
	n.lastHeartbeat = time.Now()
	n.role = Follower
	n.lastKnownLeader = args.LeaderID

	// Verificação de consistência do log
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex >= len(n.log) {
			return nil // não temos a entrada anterior
		}
		if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Conflito: remove entradas a partir do ponto conflitante
			n.log = n.log[:args.PrevLogIndex]
			return nil
		}
	}

	// Anexa novas entradas
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(n.log) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx] // remove conflito
			} else {
				continue // já temos esta entrada
			}
		}
		n.log = append(n.log, entry)
	}

	// Atualiza commitIndex
	if args.LeaderCommit > n.commitIndex {
		last := len(n.log) - 1
		newCommit := args.LeaderCommit
		if newCommit > last {
			newCommit = last
		}
		// Envia entradas novas para aplicação
		for i := n.commitIndex + 1; i <= newCommit; i++ {
			if i < len(n.log) {
				select {
				case n.applyCh <- n.log[i]:
				default:
				}
			}
		}
		n.commitIndex = newCommit
	}

	reply.Success = true
	reply.Term = n.currentTerm

	// Só loga replicação de entradas reais (não heartbeats vazios)
	if len(args.Entries) > 0 {
		log.Printf("[RAFT][%s] replicando %d entradas do líder %s (mandato %d)",
			n.id, len(args.Entries), args.LeaderID, args.Term)
	}
	return nil
}

// -----------------------------------------------------------------------
// Heartbeats (enviados pelo líder)
// -----------------------------------------------------------------------

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	leaderID := n.id
	commitIndex := n.commitIndex
	n.mu.Unlock()

	for _, addr := range n.peerAddr {
		go func(addr string) {
			n.mu.Lock()
			prevIdx, prevTerm := n.prevLogInfo(addr)
			entries := n.entriesFrom(addr)
			n.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			var reply AppendEntriesReply
			if err := call(addr, "RaftRPC.AppendEntries", &args, &reply); err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
				return
			}
			if reply.Success && len(entries) > 0 {
				n.matchIndex[addr] = prevIdx + len(entries)
				n.nextIndex[addr] = n.matchIndex[addr] + 1
				n.advanceCommitIndex()
			} else if !reply.Success {
				if n.nextIndex[addr] > 1 {
					n.nextIndex[addr]--
				}
			}
		}(addr)
	}
}

// -----------------------------------------------------------------------
// Helpers internos (chamados com mu já adquirido)
// -----------------------------------------------------------------------

func (n *Node) becomeLeader() {
	n.role = Leader
	log.Printf("[RAFT][%s] 🏆 ELEITO LÍDER | mandato=%d", n.id, n.currentTerm)
	// Inicializa nextIndex e matchIndex para todos os peers
	lastIdx := len(n.log) - 1
	for _, addr := range n.peerAddr {
		n.nextIndex[addr] = lastIdx + 1
		n.matchIndex[addr] = 0
	}
	n.notifyRoleUnlocked(Leader)
}

func (n *Node) stepDown(term int) {
	n.currentTerm = term
	n.role = Follower
	n.votedFor = ""
	n.notifyRoleUnlocked(Follower)
}

func (n *Node) lastLogIndexTerm() (int, int) {
	last := len(n.log) - 1
	return last, n.log[last].Term
}

func (n *Node) prevLogInfo(addr string) (int, int) {
	ni := n.nextIndex[addr]
	prevIdx := ni - 1
	if prevIdx <= 0 || prevIdx >= len(n.log) {
		return 0, 0
	}
	return prevIdx, n.log[prevIdx].Term
}

func (n *Node) entriesFrom(addr string) []LogEntry {
	ni := n.nextIndex[addr]
	if ni <= 0 || ni >= len(n.log) {
		return nil
	}
	entries := make([]LogEntry, len(n.log)-ni)
	copy(entries, n.log[ni:])
	return entries
}

func (n *Node) advanceCommitIndex() {
	// Avança commitIndex se a maioria dos nós confirmou
	majority := (len(n.peerAddr)+1)/2 + 1
	for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			break
		}
		count := 1 // conta o próprio líder
		for _, addr := range n.peerAddr {
			if n.matchIndex[addr] >= idx {
				count++
			}
		}
		if count >= majority {
			prev := n.commitIndex
			n.commitIndex = idx
			// commitIndex avançado — visível no status periódico
			// Envia entradas recém commitadas para aplicação (não bloqueante)
			for i := prev + 1; i <= idx; i++ {
				if i < len(n.log) {
					select {
					case n.applyCh <- n.log[i]:
					default:
					}
				}
			}
			break
		}
	}
}

func (n *Node) notifyRole(r Role) {
	select {
	case n.roleChangeCh <- r:
	default:
	}
}

func (n *Node) notifyRoleUnlocked(r Role) {
	// Versão sem lock (mu já adquirido pelo chamador)
	select {
	case n.roleChangeCh <- r:
	default:
	}
}

// -----------------------------------------------------------------------
// Utilitários
// -----------------------------------------------------------------------

func randomElectionTimeout(baseMs int) time.Duration {
	// Varia entre 1x e 2x o base para evitar split votes.
	// Com jitter maior a probabilidade de dois nós dispararem ao mesmo tempo
	// é muito menor — especialmente importante com apenas 2 nós restantes.
	jitter := rand.Intn(baseMs) + rand.Intn(baseMs/2)
	return time.Duration(baseMs+jitter) * time.Millisecond
}

// call faz uma chamada RPC com timeout de 200ms.
// Sem timeout, um peer offline pode travar a goroutine indefinidamente
// e impedir que o nó se proclame líder após receber votos suficientes.
func call(addr, method string, args, reply interface{}) error {
	// Tenta até 2 vezes com intervalo curto.
	// O "unexpected EOF" ocorre quando dois nós tentam conexão simultânea —
	// uma segunda tentativa após 20ms resolve na maioria dos casos.
	var lastErr error
	for tentativa := 0; tentativa < 2; tentativa++ {
		if tentativa > 0 {
			time.Sleep(20 * time.Millisecond)
		}

		type result struct{ err error }
		ch := make(chan result, 1)

		go func() {
			conn, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				ch <- result{err}
				return
			}
			defer conn.Close()
			ch <- result{conn.Call(method, args, reply)}
		}()

		select {
		case r := <-ch:
			if r.err == nil {
				return nil
			}
			lastErr = r.err
		case <-time.After(200 * time.Millisecond):
			return fmt.Errorf("timeout RPC -> %s %s", addr, method)
		}
	}
	return lastErr
}

// LeaderID retorna o ID do líder conhecido (vazio se desconhecido).
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Leader {
		return n.id
	}
	return n.lastKnownLeader
}

// ApplyCh retorna o canal de entradas commitadas para aplicação no estado.
// O consumidor (main.go) lê deste canal e aplica na fila distribuída.
func (n *Node) ApplyCh() <-chan LogEntry {
	return n.applyCh
}

// Propose submete um comando para replicação. Só funciona no líder.
// Retorna erro se este nó não for o líder.
func (n *Node) Propose(cmd []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return fmt.Errorf("não sou líder")
	}
	idx := len(n.log)
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   idx,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	return nil
}

// PreVote responde a uma pré-votação.
// Concede o voto se:
//   1. O candidato tem mandato >= nosso mandato atual
//   2. O log do candidato é tão atualizado quanto o nosso
//   3. NÃO ouvimos de um líder válido recentemente (ou não temos líder)
//
// A regra 3 protege clusters saudáveis contra eleições desnecessárias.
// Mas na inicialização (lastHeartbeat = time.Now()) todos têm heartbeat
// "recente" — por isso só aplicamos a regra 3 se já houve mandato > 0,
// indicando que o cluster já teve um líder antes.
func (n *Node) PreVote(args *PreVoteArgs, reply *PreVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Sempre retorna o termo atual para que o candidato detecte líder existente
	reply.Term = n.currentTerm
	reply.VoteGranted = false

	// Recusa se mandato do candidato é menor que o atual
	if args.CandidateTerm < n.currentTerm {
		return nil // reply.Term > args.CandidateTerm sinaliza líder existente
	}

	// Proteção: recusa se ouvimos de um líder recentemente.
	// lastHeartbeat é inicializado no PASSADO (-2s), então esta condição
	// nunca bloqueia a primeira eleição na inicialização.
	if n.role == Follower && time.Since(n.lastHeartbeat) < n.electionTimeout {
		return nil // reply.Term indica o mandato do líder atual
	}

	// Concede se o log do candidato é tão atualizado quanto o nosso
	lastIdx, lastTerm := n.lastLogIndexTerm()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	reply.VoteGranted = logOK
	return nil
}
