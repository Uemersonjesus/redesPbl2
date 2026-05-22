package broker

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// -----------------------------------------------------------------------
// Estado de um drone registrado no broker
// -----------------------------------------------------------------------

type StatusDrone string

const (
	DroneStatusLivre    StatusDrone = "livre"
	DroneStatusOcupado  StatusDrone = "ocupado"
	DroneStatusInativo  StatusDrone = "inativo"
)

// DroneRegistrado é a visão que o broker tem de cada drone.
type DroneRegistrado struct {
	ID          string
	Addr        string      // host:porta RPC do container do drone
	Lat         float64     // posição atual (atualizada após cada missão)
	Long        float64
	Status      StatusDrone
	ReqAtual    string      // ID da requisição que está atendendo
	SetorAtual  string      // setor que está atendendo atualmente
	UltimoVisto time.Time   // último heartbeat/resposta RPC bem-sucedida
}

// Distancia calcula distância euclidiana simples .
// Para precisão real usaríamos Haversine, mas o estreito é pequeno.
func (d *DroneRegistrado) Distancia(lat, long float64) float64 {
	dlat := d.Lat - lat
	dlong := d.Long - long
	return math.Sqrt(dlat*dlat+dlong*dlong) * 111.0 // graus → km aproximado
}

// -----------------------------------------------------------------------
// Registry — registro thread-safe de drones
// -----------------------------------------------------------------------

type Registry struct {
	mu     sync.RWMutex
	drones map[string]*DroneRegistrado // droneID → drone
}

func NewRegistry() *Registry {
	return &Registry{drones: make(map[string]*DroneRegistrado)}
}

// Registrar adiciona ou atualiza um drone (chamado via apply do log Raft).
// Se o drone estava inativo e voltou, reseta para livre.
func (r *Registry) Registrar(id, addr string, lat, long float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, existe := r.drones[id]; existe {
		d.Addr = addr
		d.Lat = lat
		d.Long = long
		d.UltimoVisto = time.Now()
		// Drone voltou após inatividade — reseta para livre
		if d.Status == DroneStatusInativo {
			d.Status = DroneStatusLivre
			d.ReqAtual = ""
			d.SetorAtual = ""
		}
		return
	}
	r.drones[id] = &DroneRegistrado{
		ID:          id,
		Addr:        addr,
		Lat:         lat,
		Long:        long,
		Status:      DroneStatusLivre,
		UltimoVisto: time.Now(),
	}
}

// AtualizarStatus muda o status de um drone (aplicado via log Raft).
func (r *Registry) AtualizarStatus(droneID, reqID string, status StatusDrone) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.drones[droneID]; ok {
		d.Status = status
		d.ReqAtual = reqID
		if status == DroneStatusLivre {
			d.SetorAtual = ""
		}
		d.UltimoVisto = time.Now()
	}
}

// AtualizarStatusComSetor muda status e registra o setor sendo atendido.
func (r *Registry) AtualizarStatusComSetor(droneID, reqID, setorID string, status StatusDrone) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.drones[droneID]; ok {
		d.Status = status
		d.ReqAtual = reqID
		d.SetorAtual = setorID
		d.UltimoVisto = time.Now()
	}
}

// AtualizarPosicao atualiza lat/long após conclusão de missão.
func (r *Registry) AtualizarPosicao(droneID string, lat, long float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.drones[droneID]; ok {
		d.Lat = lat
		d.Long = long
	}
}

// MarcarInativo marca o drone como inativo (RPC não respondeu).
func (r *Registry) MarcarInativo(droneID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.drones[droneID]; ok {
		d.Status = DroneStatusInativo
	}
}

// DronesLivres retorna todos os drones disponíveis para despacho.
func (r *Registry) DronesLivres() []*DroneRegistrado {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var livres []*DroneRegistrado
	for _, d := range r.drones {
		if d.Status == DroneStatusLivre {
			livres = append(livres, d)
		}
	}
	return livres
}

// Get retorna um drone pelo ID.
func (r *Registry) Get(droneID string) (*DroneRegistrado, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.drones[droneID]
	return d, ok
}

// Total retorna quantidade total de drones registrados.
func (r *Registry) Total() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.drones)
}

// Livres retorna quantidade de drones livres.
func (r *Registry) Livres() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, d := range r.drones {
		if d.Status == DroneStatusLivre {
			count++
		}
	}
	return count
}

// Snapshot retorna cópia do estado atual (para log de status).
func (r *Registry) Snapshot() []DroneRegistrado {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snap := make([]DroneRegistrado, 0, len(r.drones))
	for _, d := range r.drones {
		snap = append(snap, *d)
	}
	return snap
}

// Todos retorna todos os drones (para verificação de saúde).
func (r *Registry) Todos() []*DroneRegistrado {
	r.mu.RLock()
	defer r.mu.RUnlock()
	todos := make([]*DroneRegistrado, 0, len(r.drones))
	for _, d := range r.drones {
		copia := *d
		todos = append(todos, &copia)
	}
	return todos
}

// AddrDrone retorna o endereço RPC de um drone.
func (r *Registry) AddrDrone(droneID string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.drones[droneID]
	if !ok {
		return "", fmt.Errorf("drone %s não registrado", droneID)
	}
	return d.Addr, nil
}

// SnapshotDrones retorna cópia de todos os drones para o painel.
func (r *Registry) SnapshotDrones() []DroneSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	snaps := make([]DroneSnapshot, 0, len(r.drones))
	for _, d := range r.drones {
		snaps = append(snaps, DroneSnapshot{
			ID:         d.ID,
			Status:     string(d.Status),
			Lat:        d.Lat,
			Long:       d.Long,
			ReqAtual:   d.ReqAtual,
			SetorAtual: d.SetorAtual,
		})
	}
	return snaps
}

// Existe verifica se um drone já está registrado.
func (r *Registry) Existe(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.drones[id]
	return ok
}
