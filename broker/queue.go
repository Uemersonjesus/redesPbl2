package broker

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// -----------------------------------------------------------------------
// Requisição de drone
// -----------------------------------------------------------------------

type Requisicao struct {
	ID         string    `json:"id"`
	SensorID   string    `json:"sensor_id"`
	SetorID    string    `json:"setor_id"`
	Tipo       string    `json:"tipo"`
	Prioridade int       `json:"prioridade"`
	Lat        float64   `json:"lat"`
	Long       float64   `json:"long"`
	Timestamp  time.Time `json:"timestamp"`
	Status     string    `json:"status"`
	DroneID    string    `json:"drone_id"`
	index      int
}

// Pontuacao combina prioridade + tempo de espera para evitar starvation.
func (r *Requisicao) Pontuacao() float64 {
	espera := time.Since(r.Timestamp).Seconds()
	return float64(r.Prioridade)*10 + espera*0.1
}

// -----------------------------------------------------------------------
// Heap
// -----------------------------------------------------------------------

type filaHeap []*Requisicao

func (h filaHeap) Len() int           { return len(h) }
func (h filaHeap) Less(i, j int) bool { return h[i].Pontuacao() > h[j].Pontuacao() }
func (h filaHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *filaHeap) Push(x interface{}) {
	n := len(*h)
	r := x.(*Requisicao)
	r.index = n
	*h = append(*h, r)
}
func (h *filaHeap) Pop() interface{} {
	old := *h
	n := len(old)
	r := old[n-1]
	old[n-1] = nil
	r.index = -1
	*h = old[:n-1]
	return r
}

// -----------------------------------------------------------------------
// FilaPrioridade — thread-safe, aplicada via log Raft em todos os brokers
// -----------------------------------------------------------------------

// FilaPrioridade é a fila distribuída de requisições de drone.
// O estado é idêntico em todos os brokers porque é aplicado via log Raft.
// Apenas o líder escreve (via Propose); todos lêem do applyCh.
type FilaPrioridade struct {
	mu     sync.Mutex
	h      filaHeap
	lookup map[string]*Requisicao
	seq    int
}

func NewFilaPrioridade() *FilaPrioridade {
	fq := &FilaPrioridade{
		lookup: make(map[string]*Requisicao),
	}
	heap.Init(&fq.h)
	return fq
}

// Aplicar insere uma requisição já commitada pelo Raft.
// Chamado por TODOS os brokers ao ler do applyCh — mantém estado idêntico.
func (fq *FilaPrioridade) Aplicar(r *Requisicao) {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	if _, existe := fq.lookup[r.ID]; existe {
		return // idempotente: ignora duplicata
	}
	r.Status = "na_fila"
	heap.Push(&fq.h, r)
	fq.lookup[r.ID] = r
	// Log de inserção silenciado — visível no status periódico do broker
}

// AplicarAlocacao marca a requisição como alocada (commitado via Raft).
func (fq *FilaPrioridade) AplicarAlocacao(reqID, droneID string) {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if r, ok := fq.lookup[reqID]; ok {
		r.Status = "drone_alocado"
		r.DroneID = droneID
		// Remove do heap (já saiu da fila de espera)
		if r.index >= 0 && r.index < fq.h.Len() {
			heap.Remove(&fq.h, r.index)
		}
		// alocação logada pelo dispatcher
	}
}

// AplicarConclusao marca a requisição como concluída.
func (fq *FilaPrioridade) AplicarConclusao(reqID string) {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if r, ok := fq.lookup[reqID]; ok {
		r.Status = "concluido"
		// conclusão logada pelo dispatcher
	}
}

// Proximo retorna a requisição de maior prioridade na fila (sem remover).
// O dispatcher decide se aloca; a remoção acontece via AplicarAlocacao.
func (fq *FilaPrioridade) Proximo() *Requisicao {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.h.Len() == 0 {
		return nil
	}
	return fq.h[0] // peek no topo do heap
}

// Consultar retorna o estado atual de uma requisição.
func (fq *FilaPrioridade) Consultar(id string) (*Requisicao, bool) {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	r, ok := fq.lookup[id]
	return r, ok
}

// Posicao retorna a posição na fila (1-based). 0 = sendo atendido.
func (fq *FilaPrioridade) Posicao(id string) int {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	r, ok := fq.lookup[id]
	if !ok {
		return -1
	}
	if r.index < 0 {
		return 0
	}
	return r.index + 1
}

// Tamanho retorna quantas requisições aguardam na fila.
func (fq *FilaPrioridade) Tamanho() int {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	return fq.h.Len()
}

// GerarID gera um ID único para nova requisição.
func (fq *FilaPrioridade) GerarID() string {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	fq.seq++
	return fmt.Sprintf("req-%d-%d", time.Now().UnixMilli(), fq.seq)
}

// ContadoresPorPrioridade retorna quantas requisições PENDENTES há por prioridade.
// Conta apenas status "na_fila" — exclui alocadas e concluídas.
func (fq *FilaPrioridade) ContadoresPorPrioridade() map[int]int {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	c := map[int]int{1: 0, 2: 0, 3: 0, 4: 0}
	for _, r := range fq.lookup {
		if r.Status == "na_fila" {
			c[r.Prioridade]++
		}
	}
	return c
}

// ProximoSnapshot retorna snapshot da próxima requisição sem remover.
func (fq *FilaPrioridade) ProximoSnapshot() *Requisicao {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.h.Len() == 0 {
		return nil
	}
	return fq.h[0]
}
