package broker

// AlertaSensor é o payload recebido do sensor via WebSocket.
// Declarado aqui para ser compartilhado por ws_server.go e main.go.
type AlertaSensor struct {
	SensorID    string  `json:"sensor_id"`
	SetorID     string  `json:"setor_id"`
	Tipo        string  `json:"tipo"`
	Prioridade  int     `json:"prioridade"`
	Coordenadas struct {
		Lat  float64 `json:"lat"`
		Long float64 `json:"long"`
	} `json:"coordenadas"`
	Timestamp string `json:"timestamp"`
	Alerta    bool   `json:"alerta"`
	Descricao string `json:"descricao"`
}

// SensorInfo descreve um sensor conhecido pelo broker.
// Mantido para compatibilidade — não usado na arquitetura atual
// onde sensores conectam ativamente no broker.
type SensorInfo struct {
	ID      string
	SetorID string
	Addr    string
}
