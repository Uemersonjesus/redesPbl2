package raft

// -----------------------------------------------------------------------
// RequestVote — enviado por candidatos durante eleições
// -----------------------------------------------------------------------

// RequestVoteArgs são os argumentos enviados pelo candidato.
type RequestVoteArgs struct {
	Term         int    // mandato do candidato
	CandidateID  string // ID do candidato
	LastLogIndex int    // índice da última entrada do log do candidato
	LastLogTerm  int    // mandato da última entrada do log do candidato
}

// RequestVoteReply é a resposta do nó receptor.
type RequestVoteReply struct {
	Term        int  // mandato atual do receptor (para o candidato se atualizar)
	VoteGranted bool // true se o voto foi concedido
}

// -----------------------------------------------------------------------
// AppendEntries — enviado pelo líder (heartbeat e replicação de log)
// -----------------------------------------------------------------------

// AppendEntriesArgs são os argumentos enviados pelo líder.
type AppendEntriesArgs struct {
	Term     int    // mandato do líder
	LeaderID string // ID do líder (para redirecionar clientes)

	// Campos de consistência do log:
	PrevLogIndex int        // índice da entrada imediatamente anterior às novas
	PrevLogTerm  int        // mandato de PrevLogIndex
	Entries      []LogEntry // novas entradas (vazio = heartbeat)

	LeaderCommit int // índice de commit do líder
}

// AppendEntriesReply é a resposta do seguidor.
type AppendEntriesReply struct {
	Term    int  // mandato atual do receptor
	Success bool // true se o seguidor aceitou as entradas
}

// -----------------------------------------------------------------------
// PreVote — fase de pré-votação para evitar disruptive server
//
// Antes de incrementar o mandato e iniciar eleição real,
// o candidato verifica se conseguiria maioria. Nós isolados
// nunca passam desta fase — o mandato não sobe infinitamente.
// -----------------------------------------------------------------------

type PreVoteArgs struct {
	CandidateTerm int    // mandato que o candidato USARIA se eleição real
	CandidateID   string
	LastLogIndex  int
	LastLogTerm   int
}

type PreVoteReply struct {
	Term        int  // mandato atual do receptor
	VoteGranted bool
}
