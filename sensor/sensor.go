package main

import (
	"math/rand"
	"os"
	"time"
)

// -----------------------------------------------------------------------
// Prioridades
// -----------------------------------------------------------------------

type Prioridade int

const (
	PrioInformativa Prioridade = 1
	PrioMedia       Prioridade = 2
	PrioAlta        Prioridade = 3
	PrioCritica     Prioridade = 4
)

func (p Prioridade) String() string {
	switch p {
	case PrioInformativa:
		return "Informativa"
	case PrioMedia:
		return "Média"
	case PrioAlta:
		return "Alta"
	case PrioCritica:
		return "Crítica"
	default:
		return "Desconhecida"
	}
}

// -----------------------------------------------------------------------
// Tipos de sensor
// -----------------------------------------------------------------------

type TipoSensor string

const (
	TipoRadar TipoSensor = "radar"
	TipoAIS   TipoSensor = "ais"
	TipoBoia  TipoSensor = "boia_impacto"
	TipoClima TipoSensor = "clima"
)

// Perfil define o comportamento de cada tipo de sensor.
type Perfil struct {
	Tipo       TipoSensor
	Prioridade Prioridade
	// Intervalo de geração de leituras internas (sempre ocorre)
	IntervaloLeitura time.Duration
	// ProbAlerta: probabilidade de uma leitura gerar alerta real (0.0-1.0)
	// Só alertas reais são enviados ao broker via WebSocket
	ProbAlerta float64
}

// Probabilidade de alerta por leitura: 1% em todos os tipos.
// Justificativa: com 16 sensores ativos, a probabilidade agregada de
// surgir pelo menos um alerta em 2 minutos (janela de missão) fica
// em torno de 10-20% — compatível com uma frota pequena de drones.
// Fórmula: P(ao menos 1 em N) = 1 - (1-p)^(N * leituras_por_janela)
// Ex: radar, p=0.01, 4 sensores, 60 leituras/2min → ~33% de algum alerta
// Probabilidades calibradas para demonstração:
// Com DEMO_MODE=true → alta frequência para ver fila e scheduler em ação
// Com DEMO_MODE=false (padrão) → 1% para operação realista
var Perfis = map[TipoSensor]Perfil{
	TipoRadar: {
		Tipo:             TipoRadar,
		Prioridade:       PrioMedia,
		IntervaloLeitura: 2 * time.Second,
		ProbAlerta:       probAlerta(0.08, 0.01), // demo: 8% | real: 1%
	},
	TipoAIS: {
		Tipo:             TipoAIS,
		Prioridade:       PrioAlta,
		IntervaloLeitura: 3 * time.Second,
		ProbAlerta:       probAlerta(0.07, 0.01), // demo: 7% | real: 1%
	},
	TipoBoia: {
		Tipo:             TipoBoia,
		Prioridade:       PrioCritica,
		IntervaloLeitura: 4 * time.Second,
		ProbAlerta:       probAlerta(0.05, 0.01), // demo: 5% | real: 1%
	},
	TipoClima: {
		Tipo:             TipoClima,
		Prioridade:       PrioInformativa,
		IntervaloLeitura: 5 * time.Second,
		ProbAlerta:       probAlerta(0.05, 0.01), // demo: 5% | real: 1%
	},
}

// probAlerta retorna a probabilidade de demo ou real dependendo de SENSOR_DEMO_MODE.
func probAlerta(demo, real float64) float64 {
	if os.Getenv("SENSOR_DEMO_MODE") == "true" {
		return demo
	}
	return real
}

// -----------------------------------------------------------------------
// Leitura de sensor
// -----------------------------------------------------------------------

// Coordenadas geográficas do Estreito de Ormuz
// Lat: 25.8–26.8N   Long: 56.0–57.5E
type Coordenadas struct {
	Lat  float64 `json:"lat"`
	Long float64 `json:"long"`
}

// Leitura representa uma medição gerada pelo sensor.
type Leitura struct {
	SensorID    string     `json:"sensor_id"`
	SetorID     string     `json:"setor_id"`
	Tipo        TipoSensor `json:"tipo"`
	Prioridade  Prioridade `json:"prioridade"`
	Coordenadas Coordenadas `json:"coordenadas"`
	Timestamp   string     `json:"timestamp"`
	Alerta      bool       `json:"alerta"`       // true = problema real, deve ser enviado
	Descricao   string     `json:"descricao"`    // descrição do evento
}

// -----------------------------------------------------------------------
// Sensor
// -----------------------------------------------------------------------

type Sensor struct {
	ID      string
	SetorID string
	Perfil  Perfil
	Coords  Coordenadas
}

func NewSensor(id, setorID string, tipo TipoSensor, lat, long float64) *Sensor {
	return &Sensor{
		ID:      id,
		SetorID: setorID,
		Perfil:  Perfis[tipo],
		Coords:  Coordenadas{Lat: lat, Long: long},
	}
}

// Ler gera uma leitura. Alerta=true significa problema real a enviar.
func (s *Sensor) Ler() Leitura {
	// Pequena variação nas coordenadas (simulando leituras de área)
	lat := s.Coords.Lat + (rand.Float64()-0.5)*0.005
	long := s.Coords.Long + (rand.Float64()-0.5)*0.005

	alerta := rand.Float64() < s.Perfil.ProbAlerta

	l := Leitura{
		SensorID:    s.ID,
		SetorID:     s.SetorID,
		Tipo:        s.Perfil.Tipo,
		Prioridade:  s.Perfil.Prioridade,
		Coordenadas: Coordenadas{Lat: lat, Long: long},
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Alerta:      alerta,
	}

	if alerta {
		l.Descricao = descricaoAlerta(s.Perfil.Tipo)
	}
	return l
}

func descricaoAlerta(tipo TipoSensor) string {
	switch tipo {
	case TipoRadar:
		alertas := []string{
			"embarcação não identificada detectada",
			"objeto suspeito em rota de colisão",
			"movimento irregular em zona restrita",
		}
		return alertas[rand.Intn(len(alertas))]
	case TipoAIS:
		alertas := []string{
			"anomalia AIS: desvio de rota suspeito",
			"sinal AIS ausente em embarcação monitorada",
			"bloqueio parcial de corredor detectado",
		}
		return alertas[rand.Intn(len(alertas))]
	case TipoBoia:
		alertas := []string{
			"impacto físico detectado na boia",
			"colisão com objeto submerso",
			"pressão anormal detectada",
		}
		return alertas[rand.Intn(len(alertas))]
	case TipoClima:
		alertas := []string{
			"velocidade do vento acima do limite operacional",
			"visibilidade reduzida — névoa densa",
			"ondulação crítica detectada",
		}
		return alertas[rand.Intn(len(alertas))]
	}
	return "evento detectado"
}
