package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
)

// Message é o envelope de uma mensagem no sistema.
// Sensores e boias enviam JSON com este formato via UDP.
type Message struct {
	Topic   string          `json:"topic"`   // ex: "drone.status", "sensor.gps", "boia.maritima"
	Payload json.RawMessage `json:"payload"` // corpo opaco — cada tópico define o seu
	From    string          `json:"from"`    // ID do produtor (sensor, boia, etc.)
}

// Handler é a assinatura de uma função assinante.
// Recebe a mensagem e pode retornar um resultado (futuro: publicar de volta).
type Handler func(msg Message)

// Broker gerencia tópicos, assinaturas e recepção UDP.
type Broker struct {
	mu       sync.RWMutex
	subs     map[string][]Handler // tópico -> lista de handlers
	udpConn  *net.UDPConn
	stopCh   chan struct{}
	udpPort  int
	bufSize  int
}

// New cria um Broker configurado para escutar na porta UDP indicada.
func New(udpPort int) *Broker {
	return &Broker{
		subs:    make(map[string][]Handler),
		stopCh:  make(chan struct{}),
		udpPort: udpPort,
		bufSize: 65507, // tamanho máximo de datagrama UDP
	}
}

// Subscribe registra um handler para um tópico.
// Suporta wildcard simples: "drone.*" captura "drone.status", "drone.cmd", etc.
func (b *Broker) Subscribe(topic string, h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[topic] = append(b.subs[topic], h)
	log.Printf("[BROKER] assinante registrado para tópico %q", topic)
}

// Publish entrega uma mensagem a todos os handlers cujo tópico casa.
// Pode ser chamado internamente (após receber UDP) ou por outros componentes.
func (b *Broker) Publish(msg Message) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	dispatched := 0
	for topic, handlers := range b.subs {
		if matchTopic(topic, msg.Topic) {
			for _, h := range handlers {
				go h(msg) // cada handler roda em sua própria goroutine
				dispatched++
			}
		}
	}
	if dispatched == 0 {
		log.Printf("[BROKER] nenhum assinante para tópico %q (from=%s)", msg.Topic, msg.From)
	}
}

// Start inicia o loop de recepção UDP.
func (b *Broker) Start() error {
	addr := fmt.Sprintf("0.0.0.0:%d", b.udpPort)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("resolvendo endereço UDP: %w", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("abrindo socket UDP: %w", err)
	}
	b.udpConn = conn

	log.Printf("[BROKER] escutando UDP em %s", addr)
	go b.recvLoop()
	return nil
}

// Stop encerra o broker de forma ordenada.
func (b *Broker) Stop() {
	close(b.stopCh)
	if b.udpConn != nil {
		b.udpConn.Close()
	}
}

// recvLoop lê datagramas UDP e os entrega ao dispatcher.
func (b *Broker) recvLoop() {
	buf := make([]byte, b.bufSize)
	for {
		select {
		case <-b.stopCh:
			return
		default:
		}

		n, remoteAddr, err := b.udpConn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-b.stopCh:
				return // encerramento esperado
			default:
				log.Printf("[BROKER] erro lendo UDP: %v", err)
				continue
			}
		}

		raw := make([]byte, n)
		copy(raw, buf[:n])

		go b.dispatch(raw, remoteAddr.String())
	}
}

func (b *Broker) dispatch(raw []byte, from string) {
	var msg Message
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.Printf("[BROKER] datagrama inválido de %s: %v | raw=%q", from, err, string(raw))
		return
	}
	if msg.From == "" {
		msg.From = from
	}
	log.Printf("[BROKER] msg recebida | topic=%s from=%s", msg.Topic, msg.From)
	b.Publish(msg)
}

// -----------------------------------------------------------------------
// Correspondência de tópicos
// -----------------------------------------------------------------------

// matchTopic verifica se o padrão de assinatura casa com o tópico da mensagem.
// Regras:
//   - Igualdade exata: "drone.status" == "drone.status"
//   - Wildcard de segmento: "drone.*" == "drone.qualquercoisa"
//   - Wildcard global:  "*" == qualquer tópico
func matchTopic(pattern, topic string) bool {
	if pattern == "*" {
		return true
	}
	if pattern == topic {
		return true
	}
	// Verifica sufixo ".*"
	if len(pattern) > 2 && pattern[len(pattern)-2:] == ".*" {
		prefix := pattern[:len(pattern)-2]
		if len(topic) > len(prefix)+1 && topic[:len(prefix)] == prefix && topic[len(prefix)] == '.' {
			return true
		}
	}
	return false
}
