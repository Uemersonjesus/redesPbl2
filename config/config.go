package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// NodeConfig representa a configuração de um nó do cluster.
type NodeConfig struct {
	ID       string `json:"id"`        // ex: "node-1"
	RaftAddr string `json:"raft_addr"` // ex: "192.168.1.10:7001"
	UDPPort  int    `json:"udp_port"`  // ex: 8001
	TCPPort  int    `json:"tcp_port"`  // ex: 9001 (sensores)
	WSPort   int    `json:"ws_port"`   // ex: 8090 — porta WS externa do broker (para sensores)
}

// Config é a configuração completa do sistema.
type Config struct {
	SelfID            string       `json:"self_id"`
	Peers             []NodeConfig `json:"peers"`
	ElectionTimeoutMs int          `json:"election_timeout_ms"`
	HeartbeatMs       int          `json:"heartbeat_ms"`
}

// Self retorna a configuração deste nó.
func (c *Config) Self() (NodeConfig, error) {
	for _, p := range c.Peers {
		if p.ID == c.SelfID {
			return p, nil
		}
	}
	return NodeConfig{}, fmt.Errorf("self id %q não encontrado nos peers", c.SelfID)
}

// PeerAddrs retorna os endereços RPC de todos os outros nós (exceto este).
func (c *Config) PeerAddrs() []string {
	addrs := make([]string, 0, len(c.Peers)-1)
	for _, p := range c.Peers {
		if p.ID != c.SelfID {
			addrs = append(addrs, p.RaftAddr)
		}
	}
	return addrs
}

// Load carrega a configuração seguindo esta prioridade:
//
//  1. CONFIG_FILE → caminho para um JSON completo
//  2. PEERS       → lista de peers inline (sem JSON, ideal para LAN/Docker)
//  3. Fallback    → 3 nós localhost para desenvolvimento
//
// Formato de PEERS (separa peers por vírgula, campos por |):
//
//	PEERS="node-1|192.168.1.10:7001|8001,node-2|192.168.1.11:7001|8002,node-3|192.168.1.12:7001|8003"
//	NODE_ID=node-1
func Load() (*Config, error) {
	// Prioridade 1: arquivo JSON explícito
	if path := os.Getenv("CONFIG_FILE"); path != "" {
		return loadFromFile(path)
	}
	if _, err := os.Stat("config.json"); err == nil {
		return loadFromFile("config.json")
	}

	// Prioridade 2: peers via variável de ambiente (ideal para LAN)
	if peers := os.Getenv("PEERS"); peers != "" {
		return loadFromEnv(peers)
	}

	// Prioridade 3: dev local com 3 nós em localhost
	cfg := devConfig()
	if id := os.Getenv("NODE_ID"); id != "" {
		cfg.SelfID = id
	}
	applyDefaults(cfg)
	return cfg, nil
}

func loadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("lendo config %q: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parseando config: %w", err)
	}
	applyDefaults(&cfg)
	return &cfg, nil
}

// loadFromEnv constrói a config a partir da variável PEERS.
//
// Formato: "id|raft_addr|udp_port,id|raft_addr|udp_port,..."
// Exemplo: "node-1|192.168.1.10:7001|8001,node-2|192.168.1.11:7001|8002"
func loadFromEnv(peersEnv string) (*Config, error) {
	cfg := &Config{}
	cfg.SelfID = os.Getenv("NODE_ID")
	if cfg.SelfID == "" {
		return nil, fmt.Errorf("NODE_ID não definido (obrigatório quando PEERS é usado)")
	}

	for _, entry := range strings.Split(peersEnv, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, "|")
		if len(parts) < 3 {
			return nil, fmt.Errorf("peer inválido %q — esperado: id|raft_addr|udp_port[|tcp_port]", entry)
		}
		udpPort, err := strconv.Atoi(strings.TrimSpace(parts[2]))
		if err != nil {
			return nil, fmt.Errorf("udp_port inválido em %q: %w", entry, err)
		}
		tcpPort := 9001 // padrão
		if len(parts) >= 4 {
			if n, err := strconv.Atoi(strings.TrimSpace(parts[3])); err == nil {
				tcpPort = n
			}
		}
		wsPort := 8090 // padrão
		if len(parts) >= 5 {
			if n, err := strconv.Atoi(strings.TrimSpace(parts[4])); err == nil {
				wsPort = n
			}
		}
		cfg.Peers = append(cfg.Peers, NodeConfig{
			ID:       strings.TrimSpace(parts[0]),
			RaftAddr: strings.TrimSpace(parts[1]),
			UDPPort:  udpPort,
			TCPPort:  tcpPort,
			WSPort:   wsPort,
		})
	}

	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("nenhum peer válido em PEERS=%q", peersEnv)
	}

	applyDefaults(cfg)
	return cfg, nil
}

func applyDefaults(c *Config) {
	// Variáveis de ambiente sobrescrevem o config (útil para Docker)
	if v := os.Getenv("ELECTION_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.ElectionTimeoutMs = n
		}
	}
	if v := os.Getenv("HEARTBEAT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.HeartbeatMs = n
		}
	}
	if c.ElectionTimeoutMs == 0 {
		c.ElectionTimeoutMs = 500
	}
	if c.HeartbeatMs == 0 {
		c.HeartbeatMs = 50
	}
}

func devConfig() *Config {
	return &Config{
		SelfID: "node-1",
		Peers: []NodeConfig{
			{ID: "node-1", RaftAddr: "127.0.0.1:7001", UDPPort: 8001, TCPPort: 9001, WSPort: 8090},
			{ID: "node-2", RaftAddr: "127.0.0.1:7002", UDPPort: 8002, TCPPort: 9002, WSPort: 8091},
			{ID: "node-3", RaftAddr: "127.0.0.1:7003", UDPPort: 8003, TCPPort: 9003, WSPort: 8092},
		},
	}
}

// ExampleJSON retorna um exemplo de config.json para referência.
func ExampleJSON() string {
	cfg := devConfig()
	cfg.ElectionTimeoutMs = 150
	cfg.HeartbeatMs = 50
	b, _ := json.MarshalIndent(cfg, "", "  ")
	return string(b)
}

// UDPPortForSelf resolve a porta UDP (env UDP_PORT sobrescreve o config).
func (c *Config) UDPPortForSelf() int {
	if v := os.Getenv("UDP_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	self, err := c.Self()
	if err != nil {
		return 8000
	}
	return self.UDPPort
}

// TCPAddrForPeer retorna o endereço TCP (host:tcpport) de um peer pelo ID.
func (c *Config) TCPAddrForPeer(id string) string {
	for _, p := range c.Peers {
		if p.ID == id {
			// Extrai o host do RaftAddr e usa o TCPPort
			host := p.RaftAddr
			if idx := len(host) - 1; idx >= 0 {
				// Remove a porta do RaftAddr (tudo após o último ':')
				for i := len(host) - 1; i >= 0; i-- {
					if host[i] == ':' {
						host = host[:i]
						break
					}
				}
			}
			if p.TCPPort == 0 {
				return fmt.Sprintf("%s:9001", host)
			}
			return fmt.Sprintf("%s:%d", host, p.TCPPort)
		}
	}
	return ""
}

// WSAddrForPeer retorna o endereço WS de um peer pelo ID.
// Usa a porta interna 8090 (BROKER_WS_PORT) pois o redirecionamento
// ocorre dentro da rede Docker onde todos os brokers escutam em 8090.
// O WSPort do config é a porta externa do host — usada apenas fora do Docker.
func (c *Config) WSAddrForPeer(id string) string {
	for _, p := range c.Peers {
		if p.ID == id {
			// Extrai só o host do RaftAddr
			host := p.RaftAddr
			for i := len(host) - 1; i >= 0; i-- {
				if host[i] == ':' {
					host = host[:i]
					break
				}
			}
			// Sempre usa porta interna 8090 para redirecionamento WS
			// (válido tanto para hostname Docker quanto para IP de host)
			return fmt.Sprintf("%s:8090", host)
		}
	}
	return ""
}
