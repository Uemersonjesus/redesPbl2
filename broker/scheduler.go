package broker

import (
	"fmt"
	"log"
	"sync"
)

// -----------------------------------------------------------------------
// Scheduler — seleciona o melhor drone para cada requisição
//
// Critérios em ordem:
//   1. Fair: alterna entre prioridades para evitar starvation
//   2. Posição na fila (quem chegou primeiro tem vantagem)
//   3. Criticidade da requisição (prio 4 > 3 > 2 > 1)
//   4. Distância do drone ao setor da requisição (menor = melhor)
// -----------------------------------------------------------------------

// Contadores de fair scheduling por nível de prioridade.
// A ideia: após X atendimentos de alta prio, força atender uma de prio menor.
const fairLimite = 3 // após 3 críticas consecutivas, intercala uma normal

type Scheduler struct {
	mu sync.Mutex
	// Contagem de atendimentos consecutivos por prioridade (fair scheduling)
	contPrio   map[int]int
	ultimaPrio int
	// Estatísticas totais de execução
	totalPorPrio    map[int]int // total executado por prioridade
	totalExecutadas int
	ultimoDespacho  UltimoDespacho
}

// UltimoDespacho guarda detalhes do último despacho para o painel
type UltimoDespacho struct {
	DroneID   string
	ReqID     string
	SetorID   string
	Prio      int
	Dist      float64
	MotivoDrone string // por que este drone foi escolhido
	FairAtivou  bool   // se o fair scheduling intercalou
}

// EstatGlobal é atualizado pelo scheduler e lido pelo painel
var EstatGlobal struct {
	sync.RWMutex
	TotalPorPrio    map[int]int
	TotalExecutadas int
	ConsecAtual     int
	UltimoPrio      int
	UltimoDroneID   string
	UltimoSetorID   string
	UltimoMotivo    string
	FairAtivou      bool
} = struct {
	sync.RWMutex
	TotalPorPrio    map[int]int
	TotalExecutadas int
	ConsecAtual     int
	UltimoPrio      int
	UltimoDroneID   string
	UltimoSetorID   string
	UltimoMotivo    string
	FairAtivou      bool
}{TotalPorPrio: make(map[int]int)}

func NewScheduler() *Scheduler {
	return &Scheduler{
		contPrio:     make(map[int]int),
		totalPorPrio: make(map[int]int),
	}
}

// Estatisticas retorna cópia das estatísticas para o painel
func (s *Scheduler) Estatisticas() (map[int]int, int, int, UltimoDespacho) {
	s.mu.Lock()
	defer s.mu.Unlock()
	totais := make(map[int]int)
	for k, v := range s.totalPorPrio {
		totais[k] = v
	}
	consec := s.contPrio[s.ultimaPrio]
	return totais, s.totalExecutadas, consec, s.ultimoDespacho
}

// Selecionar escolhe a próxima requisição a atender e o melhor drone.
// Retorna nil, nil se não há nada a fazer.
func (s *Scheduler) Selecionar(fila *FilaPrioridade, registry *Registry) (*Requisicao, *DroneRegistrado) {
	drones := registry.DronesLivres()
	if len(drones) == 0 {
		return nil, nil
	}

	// Coleta candidatas da fila (peek — sem remover)
	candidatas := s.candidatasOrdenadas(fila)
	if len(candidatas) == 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Seleciona a requisição respeitando fair scheduling
	req := s.selecionarRequisicao(candidatas)
	if req == nil {
		return nil, nil
	}

	// Seleciona o melhor drone para esta requisição
	drone := s.selecionarDrone(req, drones)
	if drone == nil {
		return nil, nil
	}

	// Atualiza contadores de fair
	s.contPrio[req.Prioridade]++
	s.ultimaPrio = req.Prioridade

	prioLabels := map[int]string{1: "INFO", 2: "MÉDIA", 3: "ALTA", 4: "CRÍTICA"}
	dist := drone.Distancia(req.Lat, req.Long)

	// Registra estatísticas
	s.totalPorPrio[req.Prioridade]++
	s.totalExecutadas++
	s.ultimoDespacho = UltimoDespacho{
		DroneID:     drone.ID,
		ReqID:       req.ID,
		SetorID:     req.SetorID,
		Prio:        req.Prioridade,
		Dist:        dist,
		MotivoDrone: fmt.Sprintf("menor distância ao setor (%.1fkm)", dist),
	}
	// Atualiza estatísticas globais para o painel
	EstatGlobal.Lock()
	EstatGlobal.TotalPorPrio[req.Prioridade]++
	EstatGlobal.TotalExecutadas++
	EstatGlobal.ConsecAtual = s.contPrio[req.Prioridade]
	EstatGlobal.UltimoPrio = req.Prioridade
	EstatGlobal.UltimoDroneID = drone.ID
	EstatGlobal.UltimoSetorID = req.SetorID
	EstatGlobal.UltimoMotivo = fmt.Sprintf("menor distância (%.1fkm)", dist)
	EstatGlobal.FairAtivou = false
	EstatGlobal.Unlock()

	log.Printf("[SCHED] ▶ selecionado: req=%s | prio=%s | setor=%s | drone=%s | dist=%.1fkm | motivo=menor distância",
		req.ID, prioLabels[req.Prioridade], req.SetorID, drone.ID, dist)

	return req, drone
}

// candidatasOrdenadas retorna até 10 requisições do topo da fila.
func (s *Scheduler) candidatasOrdenadas(fila *FilaPrioridade) []*Requisicao {
	// Peek nas primeiras entradas do heap
	// A fila já está ordenada por pontuação (prio + tempo de espera)
	fila.mu.Lock()
	defer fila.mu.Unlock()

	n := len(fila.h)
	if n > 10 {
		n = 10
	}
	candidatas := make([]*Requisicao, 0, n)
	for i := 0; i < n; i++ {
		r := fila.h[i]
		if r.Status == "na_fila" {
			candidatas = append(candidatas, r)
		}
	}
	return candidatas
}

// selecionarRequisicao aplica a lógica de fair scheduling.
func (s *Scheduler) selecionarRequisicao(candidatas []*Requisicao) *Requisicao {
	if len(candidatas) == 0 {
		return nil
	}

	// Verifica se atingiu o limite de atendimentos consecutivos da mesma prioridade
	if s.ultimaPrio > 0 && s.contPrio[s.ultimaPrio] >= fairLimite {
		// Tenta encontrar uma requisição de prioridade diferente
		for _, r := range candidatas {
			if r.Prioridade != s.ultimaPrio {
				log.Printf("[SCHED] fair: intercalando prio=%d após %d atendimentos de prio=%d",
					r.Prioridade, s.contPrio[s.ultimaPrio], s.ultimaPrio)
				s.contPrio[s.ultimaPrio] = 0 // reseta contador
				return r
			}
		}
		// Só há requisições da mesma prioridade — reseta e continua
		s.contPrio[s.ultimaPrio] = 0
	}

	// Retorna a de maior pontuação (topo do heap)
	return candidatas[0]
}

// selecionarDrone escolhe o drone mais adequado para a requisição.
// Critério: menor distância ao setor da requisição.
func (s *Scheduler) selecionarDrone(req *Requisicao, drones []*DroneRegistrado) *DroneRegistrado {
	if len(drones) == 0 {
		return nil
	}

	melhor := drones[0]
	menorDist := melhor.Distancia(req.Lat, req.Long)

	for _, d := range drones[1:] {
		dist := d.Distancia(req.Lat, req.Long)
		if dist < menorDist {
			menorDist = dist
			melhor = d
		}
	}
	return melhor
}

// Stats retorna estatísticas do scheduler para o painel
func (s *Scheduler) Stats() EstatScheduler {
	totais, total, consec, ultimo := s.Estatisticas()
	return EstatScheduler{
		TotalPorPrio:    totais,
		TotalExecutadas: total,
		ConsecAtual:     consec,
		UltimoPrio:      s.ultimaPrio,
		UltimoDroneID:   ultimo.DroneID,
		UltimoSetorID:   ultimo.SetorID,
		MotivoDrone:     ultimo.MotivoDrone,
	}
}
