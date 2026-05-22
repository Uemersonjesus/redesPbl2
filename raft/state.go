package raft

import "fmt"

type Role int

const (
	Follower  Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return fmt.Sprintf("ROLE(%d)", int(r))
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

// CommandType identifica operações replicadas via log Raft.
type CommandType string

const (
	// Fila de requisições
	CmdInserir  CommandType = "INSERIR"
	CmdAlocar   CommandType = "ALOCAR"
	CmdConcluir CommandType = "CONCLUIR"
	CmdFalha    CommandType = "FALHA"

	// Registro de drones
	CmdRegistrarDrone   CommandType = "REG_DRONE"
	CmdDroneStatus      CommandType = "DRONE_STATUS"
)

// Command é o payload de uma entrada do log.
type Command struct {
	Tipo CommandType `json:"tipo"`

	// Campos de requisição
	ReqID      string  `json:"req_id,omitempty"`
	SensorID   string  `json:"sensor_id,omitempty"`
	SetorID    string  `json:"setor_id,omitempty"`
	TipoSensor string  `json:"tipo_sensor,omitempty"`
	Prioridade int     `json:"prioridade,omitempty"`
	Lat        float64 `json:"lat,omitempty"`
	Long       float64 `json:"long,omitempty"`

	// Campos de drone
	DroneID   string  `json:"drone_id,omitempty"`
	DroneAddr string  `json:"drone_addr,omitempty"` // host:porta RPC
	DroneLat  float64 `json:"drone_lat,omitempty"`
	DroneLong float64 `json:"drone_long,omitempty"`
	Status    string  `json:"status,omitempty"` // "livre", "ocupado", "inativo"
	Motivo    string  `json:"motivo,omitempty"` // motivo de falha
}
