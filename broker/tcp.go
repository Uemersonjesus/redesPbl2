package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

// MensagemSensor é o payload JSON recebido via TCP do sensor.
type MensagemSensor struct {
	SensorID    string `json:"sensor_id"`
	Tipo        string `json:"tipo"`
	Prioridade  int    `json:"prioridade"`
	Coordenadas struct {
		Lat  float64 `json:"lat"`
		Long float64 `json:"long"`
	} `json:"coordenadas"`
	Timestamp string `json:"timestamp"`
	SetorID   string `json:"setor_id"`
	Cmd       string `json:"cmd"`
	MsgID     string `json:"msg_id"`
}

// RespostaTCP é o ACK enviado de volta ao sensor.
type RespostaTCP struct {
	Status   string `json:"status"`
	MsgID    string `json:"msg_id"`
	Posicao  int    `json:"posicao"`
	DroneID  string `json:"drone_id,omitempty"`
	Mensagem string `json:"mensagem,omitempty"`
}

// TCPServer aceita conexões de sensores.
// propose é injetado pelo main e envia o comando para replicação Raft.
type TCPServer struct {
	porta     int
	fila      *FilaPrioridade
	isLider   func() bool
	liderAddr func() string
	propose   func(reqID, sensorID, setorID, tipo string, prio int, lat, long float64) error
	listener  net.Listener
	stopCh    chan struct{}
}

func NewTCPServer(
	porta int,
	fila *FilaPrioridade,
	isLider func() bool,
	liderAddr func() string,
	propose func(string, string, string, string, int, float64, float64) error,
) *TCPServer {
	return &TCPServer{
		porta:     porta,
		fila:      fila,
		isLider:   isLider,
		liderAddr: liderAddr,
		propose:   propose,
		stopCh:    make(chan struct{}),
	}
}

func (s *TCPServer) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", s.porta))
	if err != nil {
		return fmt.Errorf("abrindo TCP porta %d: %w", s.porta, err)
	}
	s.listener = ln
	log.Printf("[TCP] escutando sensores em 0.0.0.0:%d", s.porta)
	go s.acceptLoop()
	return nil
}

func (s *TCPServer) Stop() {
	close(s.stopCh)
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *TCPServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				log.Printf("[TCP] erro aceitando: %v", err)
				continue
			}
		}
		go s.handleConn(conn)
	}
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg MensagemSensor
		if err := json.Unmarshal(line, &msg); err != nil {
			s.responder(conn, RespostaTCP{Status: "erro", Mensagem: "JSON inválido"})
			return
		}
		if msg.Cmd == "status" {
			s.handleStatus(conn, msg.MsgID)
			return
		}
		s.handleRequisicao(conn, &msg)
	}
}

func (s *TCPServer) handleRequisicao(conn net.Conn, msg *MensagemSensor) {
	// Redireciona para o líder se este nó não for o líder
	if !s.isLider() {
		lider := s.liderAddr()
		if lider == "" {
			s.responder(conn, RespostaTCP{
				Status:   "nao_lider",
				Mensagem: "",
			})
			return
		}
		log.Printf("[TCP] não sou líder, redirecionando para %s", lider)
		s.responder(conn, RespostaTCP{
			Status:   "nao_lider",
			Mensagem: lider,
		})
		return
	}

	// Gera ID e propõe replicação via Raft
	reqID := s.fila.GerarID()
	err := s.propose(
		reqID,
		msg.SensorID,
		msg.SetorID,
		msg.Tipo,
		msg.Prioridade,
		msg.Coordenadas.Lat,
		msg.Coordenadas.Long,
	)
	if err != nil {
		s.responder(conn, RespostaTCP{Status: "erro", Mensagem: err.Error()})
		return
	}

	// Aguarda a requisição aparecer na fila local (aplicada via Raft)
	posicao := s.aguardarInserir(reqID, 2*time.Second)

	log.Printf("[TCP] req=%s enfileirada | setor=%s tipo=%s prio=%d pos=%d",
		reqID, msg.SetorID, msg.Tipo, msg.Prioridade, posicao)

	s.responder(conn, RespostaTCP{
		Status:   "na_fila",
		MsgID:    reqID,
		Posicao:  posicao,
		Mensagem: fmt.Sprintf("requisição %s na posição %d", reqID, posicao),
	})
}

// aguardarInserir faz polling até a req aparecer na fila ou timeout.
func (s *TCPServer) aguardarInserir(reqID string, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := s.fila.Consultar(reqID); ok {
			return s.fila.Posicao(reqID)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1
}

func (s *TCPServer) handleStatus(conn net.Conn, msgID string) {
	req, ok := s.fila.Consultar(msgID)
	if !ok {
		s.responder(conn, RespostaTCP{Status: "erro", Mensagem: "msg_id não encontrado"})
		return
	}
	s.responder(conn, RespostaTCP{
		Status:  req.Status,
		MsgID:   msgID,
		Posicao: s.fila.Posicao(msgID),
		DroneID: req.DroneID,
	})
}

func (s *TCPServer) responder(conn net.Conn, resp RespostaTCP) {
	data, _ := json.Marshal(resp)
	conn.Write(append(data, '\n'))
}
