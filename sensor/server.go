package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pingEsperadoTimeout = 75 * time.Second
	reConectarIntervalo = 5 * time.Second
)

// WSClient gerencia a conexão do sensor com o broker líder.
type WSClient struct {
	sensor      *Sensor
	brokerAddrs []string
	alertCh     chan Leitura
	stopCh      chan struct{}
}

func NewWSClient(s *Sensor, brokerAddrs []string) *WSClient {
	return &WSClient{
		sensor:      s,
		brokerAddrs: brokerAddrs,
		alertCh:     make(chan Leitura, 64),
		stopCh:      make(chan struct{}),
	}
}

func (c *WSClient) Start() {
	go c.loopLeituras()
	go c.loopConexao()
}

func (c *WSClient) Stop() {
	close(c.stopCh)
}

// -----------------------------------------------------------------------
// Descoberta do líder via HTTP GET /lider
// -----------------------------------------------------------------------

type respostaLider struct {
	Lider     bool   `json:"lider"`
	LiderAddr string `json:"lider_addr"`
}

// descobrirLider usa o endpoint HTTP /lider para encontrar o broker líder
// sem disparar "bad handshake" no WebSocket.
func (c *WSClient) descobrirLider() (*websocket.Conn, string, error) {
	// Fase 1: descobre quem é o líder via HTTP
	liderAddr := ""
	for _, addr := range c.brokerAddrs {
		resp := c.consultarLider(addr)
		if resp == nil {
			continue
		}
		if resp.Lider {
			liderAddr = addr
			break
		}
		if resp.LiderAddr != "" {
			liderAddr = resp.LiderAddr
			break
		}
	}

	if liderAddr == "" {
		return nil, "", fmt.Errorf("nenhum broker disponível ou líder não eleito")
	}

	// Fase 2: conecta via WebSocket no líder confirmado
	url := "ws://" + liderAddr + "/ws/sensor"
	dialer := websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("WS para líder %s falhou: %v", liderAddr, err)
	}
	return conn, liderAddr, nil
}

// consultarLider faz GET /lider em um broker e retorna a resposta.
func (c *WSClient) consultarLider(addr string) *respostaLider {
	url := "http://" + addr + "/lider"
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	var r respostaLider
	if err := json.Unmarshal(body, &r); err != nil {
		return nil
	}
	return &r
}

// -----------------------------------------------------------------------
// Loop de conexão e manutenção
// -----------------------------------------------------------------------

func (c *WSClient) loopConexao() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		conn, addr, err := c.descobrirLider()
		if err != nil {
			log.Printf("[SENSOR][%s] líder não encontrado: %v — tentando em %v",
				c.sensor.ID, err, reConectarIntervalo)
			time.Sleep(reConectarIntervalo)
			continue
		}

		log.Printf("[SENSOR][%s] ✅ conectado ao broker líder %s", c.sensor.ID, addr)
		c.manterConexao(conn)
		log.Printf("[SENSOR][%s] ❌ conexão com %s perdida — reconectando...",
			c.sensor.ID, addr)
		time.Sleep(reConectarIntervalo)
	}
}

// manterConexao mantém a conexão ativa: responde pings e envia alertas.
func (c *WSClient) manterConexao(conn *websocket.Conn) {
	defer conn.Close()

	conn.SetPingHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(pingEsperadoTimeout))
		return conn.WriteControl(websocket.PongMessage, []byte(appData),
			time.Now().Add(5*time.Second))
	})
	conn.SetReadDeadline(time.Now().Add(pingEsperadoTimeout))

	errEnvio := make(chan error, 1)
	go func() {
		for {
			select {
			case <-c.stopCh:
				errEnvio <- nil
				return
			case leitura, ok := <-c.alertCh:
				if !ok {
					errEnvio <- nil
					return
				}
				data, err := json.Marshal(leitura)
				if err != nil {
					continue
				}
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					// Reencaminha para nova conexão — não loga erro pois será reenviado
					go func(l Leitura) {
						time.Sleep(1 * time.Second)
						select {
						case c.alertCh <- l:
						default:
							log.Printf("[SENSOR][%s] buffer cheio, alerta descartado após erro de envio", c.sensor.ID)
						}
					}(leitura)
					errEnvio <- err
					return
				}
				log.Printf("[SENSOR][%s] ✉️  alerta enviado: %s",
					c.sensor.ID, leitura.Descricao)
			}
		}
	}()

	for {
		select {
		case err := <-errEnvio:
			if err != nil {
				log.Printf("[SENSOR][%s] goroutine de envio encerrou com erro", c.sensor.ID)
			}
			return
		default:
		}

		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
				websocket.CloseNormalClosure,
			) {
				log.Printf("[SENSOR][%s] broker encerrou inesperadamente: %v", c.sensor.ID, err)
			} else {
				log.Printf("[SENSOR][%s] timeout de ping — broker considerado morto", c.sensor.ID)
			}
			return
		}
	}
}

// -----------------------------------------------------------------------
// Loop de leituras
// -----------------------------------------------------------------------

func (c *WSClient) loopLeituras() {
	ticker := time.NewTicker(c.sensor.Perfil.IntervaloLeitura)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			leitura := c.sensor.Ler()
			if leitura.Alerta {
				log.Printf("[SENSOR][%s] ⚠️  ALERTA: %s", c.sensor.ID, leitura.Descricao)
				select {
				case c.alertCh <- leitura:
				default:
					log.Printf("[SENSOR][%s] buffer cheio, alerta descartado", c.sensor.ID)
				}
			}
		}
	}
}
