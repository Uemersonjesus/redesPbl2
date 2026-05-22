package broker

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsPingIntervalo = 20 * time.Second
	wsPongTimeout   = 60 * time.Second
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// WSServer aceita conexões WebSocket de sensores.
// Não-líderes respondem 503 + header X-Lider-Addr para redirect.
type WSServer struct {
	isLider   func() bool
	liderAddr func() string
	onAlerta  func(AlertaSensor)
	mux       *http.ServeMux
}

func NewWSServer(isLider func() bool, liderAddr func() string, onAlerta func(AlertaSensor)) *WSServer {
	s := &WSServer{
		isLider:   isLider,
		liderAddr: liderAddr,
		onAlerta:  onAlerta,
		mux:       http.NewServeMux(),
	}
	s.mux.HandleFunc("/ws/sensor", s.handleSensor)
	s.mux.HandleFunc("/lider", s.handleLider) // endpoint simples para descoberta
	s.mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	return s
}

func (s *WSServer) Handler() http.Handler { return s.mux }

// handleLider responde JSON indicando se é líder e quem é o líder atual.
// Usado pelo sensor para descoberta antes de tentar o WS.
func (s *WSServer) handleLider(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.isLider() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"lider":true}`))
	} else {
		addr := s.liderAddr()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"lider":      false,
			"lider_addr": addr,
		})
	}
}

// handleSensor processa upgrade WebSocket.
// Não-líderes recusam com 503 + header X-Lider-Addr.
func (s *WSServer) handleSensor(w http.ResponseWriter, r *http.Request) {
	if !s.isLider() {
		addr := s.liderAddr()
		// Header precisa ser definido ANTES de WriteHeader
		if addr != "" {
			w.Header().Set("X-Lider-Addr", addr)
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable) // 503
		w.Write([]byte("não sou o líder"))
		// redirect silencioso — frequente e esperado
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS-SRV] upgrade falhou de %s: %v", r.RemoteAddr, err)
		return
	}

	Verbose("sensor conectado: %s", r.RemoteAddr)
	go s.gerenciarSensor(conn, r.RemoteAddr)
}

// gerenciarSensor envia pings e recebe alertas.
func (s *WSServer) gerenciarSensor(conn *websocket.Conn, remoteAddr string) {
	defer conn.Close()

	conn.SetPongHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(wsPongTimeout))
		return nil
	})

	pingStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(wsPingIntervalo)
		defer ticker.Stop()
		for {
			select {
			case <-pingStop:
				return
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					conn.Close()
					return
				}
				// Perde liderança → fecha conexão → sensor reconecta no novo líder
				if !s.isLider() {
					log.Printf("[WS-SRV] perdi liderança → fechando conexão com %s", remoteAddr)
					conn.Close()
					return
				}
			}
		}
	}()
	defer close(pingStop)

	conn.SetReadDeadline(time.Now().Add(wsPongTimeout))

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[WS-SRV] sensor %s fechou inesperadamente: %v", remoteAddr, err)
			}
			return
		}

		var alerta AlertaSensor
		if err := json.Unmarshal(msg, &alerta); err != nil {
			log.Printf("[WS-SRV] JSON inválido de %s: %v", remoteAddr, err)
			continue
		}

		if alerta.Alerta && s.onAlerta != nil {
			Verbose("🚨 sensor=%s setor=%s prio=%d | %s",
				alerta.SensorID, alerta.SetorID, alerta.Prioridade, alerta.Descricao)
			s.onAlerta(alerta)
		}
	}
}
