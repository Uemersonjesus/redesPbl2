package main

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

type StatusDrone string

const (
	StatusLivre     StatusDrone = "livre"
	StatusOcupado   StatusDrone = "ocupado"
	StatusRetornando StatusDrone = "retornando"
)

type Drone struct {
	ID  string
	mu  sync.Mutex

	status   StatusDrone
	reqAtual string
	lat      float64
	long     float64

	onConclusao func(reqID string, sucesso bool, motivo string, lat, long float64)
}

func NewDrone(id string, lat, long float64, onConclusao func(string, bool, string, float64, float64)) *Drone {
	return &Drone{
		ID:          id,
		status:      StatusLivre,
		lat:         lat,
		long:        long,
		onConclusao: onConclusao,
	}
}

func (d *Drone) EstaLivre() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.status == StatusLivre
}

func (d *Drone) Status() StatusDrone {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.status
}

func (d *Drone) ReqAtual() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reqAtual
}

func (d *Drone) Posicao() (float64, float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lat, d.long
}

// IniciarMissao aceita o despacho e inicia execução em goroutine.
func (d *Drone) IniciarMissao(args *DespachoArgs) int {
	d.mu.Lock()
	d.status = StatusOcupado
	d.reqAtual = args.ReqID
	latOrigem := d.lat
	longOrigem := d.long
	d.mu.Unlock()

	tempo := d.calcularTempo(latOrigem, longOrigem, args.Lat, args.Long, args.Prioridade)

	// Log detalhado da missão
	distKm := distancia(latOrigem, longOrigem, args.Lat, args.Long)
	prioLabel := map[int]string{1: "INFO", 2: "MÉDIA", 3: "ALTA", 4: "CRÍTICA"}
	label, ok := prioLabel[args.Prioridade]
	if !ok {
		label = "?"
	}

	log.Printf("╔══════════════════════════════════════════╗")
	log.Printf("║  🚁 MISSÃO INICIADA                      ║")
	log.Printf("╠══════════════════════════════════════════╣")
	log.Printf("║  drone    : %-29s ║", d.ID)
	log.Printf("║  req      : %-29s ║", truncar(args.ReqID, 29))
	log.Printf("║  setor    : %-29s ║", args.SetorID)
	log.Printf("║  prioridad: %-29s ║", label)
	log.Printf("║  distância: %-25s ║", fmt.Sprintf("%.2f km", distKm))
	log.Printf("║  origem   : %-29s ║", fmt.Sprintf("(%.4f, %.4f)", latOrigem, longOrigem))
	log.Printf("║  destino  : %-29s ║", fmt.Sprintf("(%.4f, %.4f)", args.Lat, args.Long))
	log.Printf("║  tempo est: %-29s ║", fmt.Sprintf("%ds (~%.1f min)", tempo, float64(tempo)/60.0))
	log.Printf("╚══════════════════════════════════════════╝")

	go d.executarMissao(args, tempo, latOrigem, longOrigem)
	return tempo
}

// calcularTempo estima o tempo de missão.
// Máximo: 120s (2 minutos). Mínimo por criticidade.
// Com 16 sensores e prob=1% por leitura, a demanda agregada
// em 2 minutos fica compatível com a capacidade dos drones.
func (d *Drone) calcularTempo(latAtual, longAtual, latDest, longDest float64, prio int) int {
	distKm := distancia(latAtual, longAtual, latDest, longDest)

	// Velocidade do drone: 150 km/h (o estreito tem ~60km de extensão)
	tempoDeslocamento := (distKm / 150.0) * 3600

	// Tempo de operação no setor por criticidade
	var tempoOperacao float64
	switch prio {
	case 4: // Crítica — boia de impacto: inspeção rápida
		tempoOperacao = 15
	case 3: // Alta — AIS anomalia: varredura do corredor
		tempoOperacao = 25
	case 2: // Média — radar: identificação de objeto
		tempoOperacao = 35
	default: // Informativa — clima: leitura atmosférica
		tempoOperacao = 45
	}

	total := int(tempoDeslocamento + tempoOperacao)

	// Mínimo por criticidade
	minimo := map[int]int{4: 20, 3: 30, 2: 45, 1: 60}
	if min, ok := minimo[prio]; ok && total < min {
		total = min
	}

	// Máximo absoluto: 2 minutos
	if total > 120 {
		total = 120
	}
	return total
}

// executarMissao simula a missão e notifica o broker ao concluir.
func (d *Drone) executarMissao(args *DespachoArgs, tempoSeg int, latOrigem, longOrigem float64) {
	// Simula progresso da missão a cada 25%
	intervalo := time.Duration(tempoSeg/4) * time.Second
	if intervalo < 2*time.Second {
		intervalo = 2 * time.Second
	}

	for pct := 25; pct <= 75; pct += 25 {
		time.Sleep(intervalo)
		log.Printf("🚁 [%s] missão em andamento: %d%% | setor=%s | req=%s",
			d.ID, pct, args.SetorID, truncar(args.ReqID, 20))
	}

	// Aguarda tempo restante
	time.Sleep(intervalo)

	// Atualiza posição para o destino
	d.mu.Lock()
	d.lat = args.Lat
	d.long = args.Long
	d.status = StatusLivre
	d.reqAtual = ""
	latFinal := d.lat
	longFinal := d.long
	d.mu.Unlock()

	log.Printf("╔══════════════════════════════════════════╗")
	log.Printf("║  ✅ MISSÃO CONCLUÍDA                     ║")
	log.Printf("╠══════════════════════════════════════════╣")
	log.Printf("║  drone    : %-29s ║", d.ID)
	log.Printf("║  req      : %-29s ║", truncar(args.ReqID, 29))
	log.Printf("║  setor    : %-29s ║", args.SetorID)
	log.Printf("║  pos.final: %-29s ║", fmt.Sprintf("(%.4f, %.4f)", latFinal, longFinal))
	log.Printf("║  status   : %-29s ║", "LIVRE — aguardando próxima missão")
	log.Printf("╚══════════════════════════════════════════╝")

	if d.onConclusao != nil {
		d.onConclusao(args.ReqID, true, "", latFinal, longFinal)
	}
}

// ── Helpers ──────────────────────────────────────────────────────────────

func distancia(lat1, long1, lat2, long2 float64) float64 {
	dlat := lat2 - lat1
	dlong := long2 - long1
	return math.Sqrt(dlat*dlat+dlong*dlong) * 111.0
}

func truncar(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return "…" + s[len(s)-(max-1):]
}
