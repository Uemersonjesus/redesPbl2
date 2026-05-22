package broker

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ═══════════════════════════════════════════════════════════════════════════
// Painel visual do broker no terminal
// Usa caracteres Unicode para criar uma interface clara e informativa
// ═══════════════════════════════════════════════════════════════════════════

const (
	largura       = 72 // largura total do painel
	fairLimiteVis = 3  // limite de atendimentos consecutivos (espelho do scheduler)

	// Cores ANSI
	corReset   = "\033[0m"
	corVerm    = "\033[31m"
	corVerde   = "\033[32m"
	corAmar    = "\033[33m"
	corAzul    = "\033[34m"
	corMagenta = "\033[35m"
	corCiano   = "\033[36m"
	corBranco  = "\033[37m"
	corCinza   = "\033[90m"
	corVermBri = "\033[91m"
	corVerdeBri= "\033[92m"
	corAmarBri = "\033[93m"
	corAzulBri = "\033[94m"
	corBrancoBri = "\033[97m"
	bgVerm     = "\033[41m"
	bgVerde    = "\033[42m"
	bgAzul     = "\033[44m"
	negrito    = "\033[1m"
	dim        = "\033[2m"
)

// PrioConfig mapeia prioridade para label e cor
var prioConfig = map[int]struct {
	Label string
	Cor   string
	Barra string
}{
	4: {Label: "CRÍTICA ", Cor: corVermBri, Barra: "█"},
	3: {Label: "ALTA    ", Cor: corAmarBri, Barra: "▓"},
	2: {Label: "MÉDIA   ", Cor: corAzulBri, Barra: "▒"},
	1: {Label: "INFO    ", Cor: corCinza,   Barra: "░"},
}

// AlertaLog representa um alerta recente recebido de sensor
type AlertaLog struct {
	Timestamp time.Time
	SensorID  string
	SetorID   string
	Tipo      string
	Prio      int
	Descricao string
}

// linha cria uma linha horizontal
func linha(estilo string) string {
	switch estilo {
	case "topo":
		return "╔" + strings.Repeat("═", largura-2) + "╗"
	case "meio":
		return "╠" + strings.Repeat("═", largura-2) + "╣"
	case "sep":
		return "╟" + strings.Repeat("─", largura-2) + "╢"
	case "fim":
		return "╚" + strings.Repeat("═", largura-2) + "╝"
	default:
		return "│" + strings.Repeat(" ", largura-2) + "│"
	}
}

// celula formata conteúdo dentro das bordas do painel
func celula(conteudo string) string {
	// Remove códigos ANSI para calcular largura real
	visivel := removeANSI(conteudo)
	// Emojis ocupam 2 colunas no terminal mas contam como menos em len()
	// Compensamos contando emojis multi-byte como largura 2
	larguraVisivel := comprimentoTerminal(visivel)
	pad := largura - 2 - larguraVisivel - 1
	if pad < 0 {
		pad = 0
	}
	return "│ " + conteudo + strings.Repeat(" ", pad) + "│"
}

// comprimentoTerminal estima a largura visual de uma string no terminal,
// contando emojis e caracteres CJK como 2 colunas.
func comprimentoTerminal(s string) int {
	largura := 0
	for _, r := range s {
		if r >= 0x1F300 && r <= 0x1FAFF { // emojis
			largura += 2
		} else if r >= 0x2600 && r <= 0x26FF { // símbolos miscelâneos
			largura += 2
		} else if r >= 0x2700 && r <= 0x27BF { // dingbats
			largura += 2
		} else if r >= 0x4E00 && r <= 0x9FFF { // CJK
			largura += 2
		} else if r >= 0x1F000 && r <= 0x1F02F { // mahjong
			largura += 2
		} else {
			largura += 1
		}
	}
	return largura
}

func removeANSI(s string) string {
	result := ""
	inEsc := false
	for _, c := range s {
		if c == '\033' {
			inEsc = true
			continue
		}
		if inEsc {
			if c == 'm' {
				inEsc = false
			}
			continue
		}
		result += string(c)
	}
	return result
}

// barraProgresso cria barra visual de progresso
func barraProgresso(atual, total, maxLen int, cor string) string {
	if total == 0 || maxLen <= 0 {
		return strings.Repeat("─", max(maxLen, 0))
	}
	preenchido := atual * maxLen / total
	if preenchido > maxLen {
		preenchido = maxLen
	}
	if preenchido < 0 {
		preenchido = 0
	}
	vazio := maxLen - preenchido
	if vazio < 0 {
		vazio = 0
	}
	return cor + strings.Repeat("█", preenchido) + corCinza +
		strings.Repeat("░", vazio) + corReset
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// BarraPrioridade cria visualização da distribuição de prioridades na fila
func BarraPrioridade(contadores map[int]int, totalFila int) string {
	if totalFila == 0 {
		return dim + "  (fila vazia)" + corReset
	}

	result := ""
	for _, prio := range []int{4, 3, 2, 1} {
		n := contadores[prio]
		if n == 0 {
			continue
		}
		cfg := prioConfig[prio]
		pct := n * 100 / totalFila
		barLen := n * 20 / totalFila
		if barLen < 1 && n > 0 {
			barLen = 1
		}
		vazio := 20 - barLen
		if vazio < 0 {
			vazio = 0
		}
		result += fmt.Sprintf("  %s%s%s %s%s%s %3d%%  %s(%d)%s\n",
			cfg.Cor+negrito, cfg.Label, corReset,
			cfg.Cor, strings.Repeat(cfg.Barra, barLen)+strings.Repeat("·", vazio), corReset,
			pct,
			dim, n, corReset)
	}
	return result
}

// ImprimirPainelLider imprime o painel completo do broker líder
var historicoMissoes []MissaoConcluida
var historicoMu sync.Mutex

func AdicionarMissaoConcluida(m MissaoConcluida) {
	historicoMu.Lock()
	defer historicoMu.Unlock()
	historicoMissoes = append(historicoMissoes, m)
	if len(historicoMissoes) > 20 {
		historicoMissoes = historicoMissoes[len(historicoMissoes)-20:]
	}
}

func ImprimirPainelLider(
	nodeID string,
	mandato int,
	fila FilaSnapshot,
	drones []DroneSnapshot,
	alertasRecentes []AlertaLog,
) {
	// Lê estatísticas do scheduler
	EstatGlobal.RLock()
	estat := EstatScheduler{
		TotalPorPrio:    EstatGlobal.TotalPorPrio,
		TotalExecutadas: EstatGlobal.TotalExecutadas,
		ConsecAtual:     EstatGlobal.ConsecAtual,
		UltimoPrio:      EstatGlobal.UltimoPrio,
		UltimoDroneID:   EstatGlobal.UltimoDroneID,
		UltimoSetorID:   EstatGlobal.UltimoSetorID,
		MotivoDrone:     EstatGlobal.UltimoMotivo,
		FairAtivou:      EstatGlobal.FairAtivou,
	}
	EstatGlobal.RUnlock()
	total := fila.Total
	contadores := fila.Contadores
	proxima := fila.Proxima

	fmt.Println()
	fmt.Println(negrito + corAzulBri + linha("topo") + corReset)

	// Cabeçalho
	titulo := fmt.Sprintf(" ██ BROKER %s  LÍDER  mandato=%d  %s",
		nodeID, mandato, time.Now().Format("15:04:05"))
	fmt.Println(negrito + corAzulBri + "║" + corVerdeBri + titulo +
		strings.Repeat(" ", largura-2-len(titulo)) + corAzulBri + "║" + corReset)

	fmt.Println(corAzulBri + linha("meio") + corReset)

	// ── Seção: Fila de Prioridade ─────────────────────────────────────
	fmt.Println(celula(negrito + corAmarBri + "  📋  FILA DE PRIORIDADE" + corReset))
	fmt.Println(corAzulBri + linha("sep") + corReset)

	if total == 0 {
		fmt.Println(celula(corVerde + "  ✓  Fila vazia — sistema ocioso" + corReset))
	} else {
		// Próxima requisição a ser atendida
		if proxima != nil {
			pcfg := prioConfig[proxima.Prioridade]
			proximo := fmt.Sprintf("  ▶ PRÓXIMA → %s[%s]%s  setor=%-12s  req=%s",
				pcfg.Cor+negrito, pcfg.Label, corReset,
				proxima.SetorID, truncarID(proxima.ID, 22))
			fmt.Println(celula(proximo))
		}

		// Total pendentes
		totalStr := fmt.Sprintf("  PENDENTES: %s%d%s  │  EXECUTADAS: %s%d%s",
			negrito+corAmarBri, total, corReset,
			negrito+corVerde, estat.TotalExecutadas, corReset)
		fmt.Println(celula(totalStr))
		fmt.Println(corAzulBri + linha("sep") + corReset)

		// Barras de pendentes por prioridade
		barras := BarraPrioridade(contadores, total)
		for _, linhab := range strings.Split(strings.TrimRight(barras, "\n"), "\n") {
			if linhab != "" {
				fmt.Println(celula(linhab))
			}
		}

		// Distribuição do que já foi executado
		if estat.TotalExecutadas > 0 {
			fmt.Println(corAzulBri + linha("sep") + corReset)
			fmt.Println(celula(dim + "  Executadas por prioridade:" + corReset))
			for _, prio := range []int{4, 3, 2, 1} {
				n := estat.TotalPorPrio[prio]
				if n == 0 {
					continue
				}
				pcfg := prioConfig[prio]
				pct := n * 100 / estat.TotalExecutadas
				barLen := pct * 15 / 100
				if barLen < 1 { barLen = 1 }
				if barLen > 15 { barLen = 15 }
				vazio := 15 - barLen
				l := fmt.Sprintf("  %s%s%s %s%s%s %3d%% (%d)",
					pcfg.Cor+negrito, pcfg.Label, corReset,
					pcfg.Cor, strings.Repeat("■", barLen)+strings.Repeat("·", vazio), corReset,
					pct, n)
				fmt.Println(celula(l))
			}
		}
	}

	// ── Fair scheduling status ─────────────────────────────────────
	if estat.UltimoPrio > 0 {
		fmt.Println(corAzulBri + linha("sep") + corReset)
		pcfg := prioConfig[estat.UltimoPrio]
		fairBar := strings.Repeat("▰", estat.ConsecAtual) +
			strings.Repeat("▱", fairLimiteVis-estat.ConsecAtual)
		if fairLimiteVis-estat.ConsecAtual < 0 {
			fairBar = strings.Repeat("▰", fairLimiteVis)
		}
		fairStr := fmt.Sprintf("  ⚖ Fair: %s%s%s consecutivos [%s] limite=%d",
			pcfg.Cor+negrito, pcfg.Label, corReset,
			fairBar, fairLimiteVis)
		if estat.FairAtivou {
			fairStr += corAmarBri + "  ← intercalando agora!" + corReset
		}
		fmt.Println(celula(fairStr))
	}

	fmt.Println(corAzulBri + linha("meio") + corReset)

	// ── Seção: Drones ─────────────────────────────────────────────────
	livres := 0
	ocupados := 0
	inativos := 0
	for _, d := range drones {
		switch d.Status {
		case "livre":
			livres++
		case "ocupado":
			ocupados++
		case "inativo":
			inativos++
		}
	}

	droneHeader := fmt.Sprintf("  🚁  DRONES   %s%d livres%s  │  %s%d em missão%s  │  %s%d inativos%s",
		corVerde+negrito, livres, corReset,
		corAmar+negrito, ocupados, corReset,
		corVerm+negrito, inativos, corReset)
	fmt.Println(celula(droneHeader))
	fmt.Println(corAzulBri + linha("sep") + corReset)

	if len(drones) == 0 {
		fmt.Println(celula(dim + "  Nenhum drone registrado" + corReset))
	} else {
		for _, d := range drones {
			var icone, cor string
			switch d.Status {
			case "livre":
				icone = "🟢"
				cor = corVerde
			case "ocupado":
				icone = "🔵"
				cor = corAzulBri
			case "inativo":
				icone = "🔴"
				cor = corVerm
			}

			// Destaque se este drone foi o último selecionado
			destaqueUltimo := ""
			if d.ID == estat.UltimoDroneID && d.Status == "ocupado" {
				destaqueUltimo = corAmarBri + " ◄ ESCOLHIDO" + corReset
			}

			linha3 := fmt.Sprintf("  %s  %s%-8s%s  pos=(%.3f,%.3f)",
				icone, cor+negrito, d.ID, corReset, d.Lat, d.Long)

			if d.Status == "ocupado" && d.ReqAtual != "" {
				pcfg := prioConfig[d.Prio]
				linha3 += fmt.Sprintf("  →  %s%s%s  setor=%-12s  dist=%.1fkm%s",
					pcfg.Cor+negrito, pcfg.Label, corReset,
					d.SetorAtual, d.DistSetor,
					destaqueUltimo)
			} else if d.Status == "livre" {
				linha3 += dim + "  aguardando despacho" + corReset
			} else {
				linha3 += corVerm + "  sem resposta" + corReset
			}
			fmt.Println(celula(linha3))
		}

		// Motivo da última seleção
		if estat.UltimoDroneID != "" {
			fmt.Println(corAzulBri + linha("sep") + corReset)
			motivoStr := fmt.Sprintf("  %sÚltima seleção:%s %s → %s  (%s)",
				dim, corReset,
				estat.UltimoDroneID,
				estat.UltimoSetorID,
				estat.MotivoDrone)
			fmt.Println(celula(motivoStr))
		}
	}

	fmt.Println(corAzulBri + linha("meio") + corReset)

	// ── Seção: Histórico de Missões ─────────────────────────────────
	fmt.Println(celula(negrito + corCiano + "  📊  ÚLTIMAS MISSÕES EXECUTADAS" + corReset))
	fmt.Println(corAzulBri + linha("sep") + corReset)

	if len(historicoMissoes) == 0 {
		fmt.Println(celula(dim + "  Nenhuma missão concluída ainda" + corReset))
	} else {
		inicio := 0
		if len(historicoMissoes) > 4 {
			inicio = len(historicoMissoes) - 4
		}
		for _, m := range historicoMissoes[inicio:] {
			pcfg := prioConfig[m.Prio]
			l := fmt.Sprintf("  %s  %-8s  %s%s%s  setor=%-12s  dist=%.1fkm  req=%s",
				m.Timestamp.Format("15:04:05"),
				m.DroneID,
				pcfg.Cor+negrito, pcfg.Label, corReset,
				m.SetorID, m.Dist,
				dim+truncarID(m.ReqID, 16)+corReset)
			fmt.Println(celula(l))
		}
	}

	fmt.Println(corAzulBri + linha("meio") + corReset)

	// ── Seção: Alertas Recentes ───────────────────────────────────────
	fmt.Println(celula(negrito + corMagenta + "  📡  ALERTAS RECENTES DE SENSORES" + corReset))
	fmt.Println(corAzulBri + linha("sep") + corReset)

	if len(alertasRecentes) == 0 {
		fmt.Println(celula(dim + "  Nenhum alerta recebido ainda" + corReset))
	} else {
		// Mostra os últimos 5 alertas
		inicio := 0
		if len(alertasRecentes) > 5 {
			inicio = len(alertasRecentes) - 5
		}
		for _, a := range alertasRecentes[inicio:] {
			pcfg := prioConfig[a.Prio]
			hora := a.Timestamp.Format("15:04:05")
			l := fmt.Sprintf("  %s  %s%s%s  %-14s  %s%s%s",
				hora,
				pcfg.Cor+negrito, pcfg.Label, corReset,
				a.SensorID,
				dim, a.Descricao, corReset)
			fmt.Println(celula(l))
		}
	}

	fmt.Println(corAzulBri + linha("fim") + corReset)
	fmt.Println()
}

// ImprimirPainelFollower imprime painel compacto para followers
func ImprimirPainelFollower(nodeID string, mandato int, lider string, filaTotal int, drones [2]int) {
	cor := corCiano
	fmt.Printf("%s┤ FOLLOWER %s │%s mandato=%-3d  líder=%-8s  fila=%-4d  drones=%d/%d  %s%s\n",
		cor, nodeID, corReset,
		mandato, lider, filaTotal,
		drones[0], drones[1],
		dim+time.Now().Format("15:04:05")+corReset,
		cor+"├"+corReset)
}

// ImprimirAlertaSensor imprime notificação de alerta recebido
func ImprimirAlertaSensor(sensorID, setorID, tipo, descricao string, prio int) {
	pcfg := prioConfig[prio]
	fmt.Printf("\n%s▶ ALERTA%s  %s%s%s  sensor=%-16s  setor=%-14s  %s\n",
		pcfg.Cor+negrito, corReset,
		pcfg.Cor+negrito, pcfg.Label, corReset,
		sensorID, setorID,
		dim+descricao+corReset)
}

// ImprimirDespacho imprime log de despacho de drone
func ImprimirDespacho(droneID, setorID, reqID string, prio int, dist float64, tempo int, filaRestante int) {
	pcfg := prioConfig[prio]
	fmt.Printf("\n%s▶ DESPACHO%s  drone=%-8s  →  setor=%-12s  %s%s%s  dist=%.1fkm  tempo=%ds  fila=%d restantes\n",
		corAzulBri+negrito, corReset,
		droneID, setorID,
		pcfg.Cor+negrito, pcfg.Label, corReset,
		dist, tempo, filaRestante)
}

// ImprimirConclusao imprime log de conclusão de missão
func ImprimirConclusao(droneID, reqID string, filaRestante int) {
	fmt.Printf("\n%s✔ CONCLUÍDO%s  drone=%-8s  req=%s  fila=%d restantes\n",
		corVerde+negrito, corReset,
		droneID, truncarID(reqID, 22), filaRestante)
}

// ImprimirFalha imprime log de falha de missão
func ImprimirFalha(droneID, reqID, motivo string) {
	fmt.Printf("\n%s✘ FALHOU%s  drone=%-8s  req=%s  motivo=%s  → reinserindo na fila\n",
		corVerm+negrito, corReset,
		droneID, truncarID(reqID, 22), motivo)
}

// ImprimirRegistroDrone imprime log de registro de drone
func ImprimirRegistroDrone(droneID, addr string, total int) {
	fmt.Printf("\n%s+ DRONE%s  %-8s  addr=%-22s  total=%d registrados\n",
		corVerde+negrito, corReset, droneID, addr, total)
}

func truncarID(id string, max int) string {
	if len(id) <= max {
		return id
	}
	return "…" + id[len(id)-(max-1):]
}


// MissaoConcluida representa uma missão concluída para o histórico
type MissaoConcluida struct {
	Timestamp time.Time
	DroneID   string
	ReqID     string
	SetorID   string
	Prio      int
	Dist      float64
}

// ── Tipos de snapshot para o painel ─────────────────────────────────────

// EstatScheduler contém estatísticas do scheduler para o painel
type EstatScheduler struct {
	TotalPorPrio    map[int]int
	TotalExecutadas int
	ConsecAtual     int
	UltimoPrio      int
	UltimoDroneID   string
	UltimoSetorID   string
	MotivoDrone     string
	FairAtivou      bool
}



type RequisicaoSnapshot struct {
	ID         string
	SetorID    string
	Prioridade int
}

type FilaSnapshot struct {
	Total      int
	Contadores map[int]int // prio → quantidade pendentes
	Proxima    *RequisicaoSnapshot
}



type DroneSnapshot struct {
	ID         string
	Status     string
	Lat        float64
	Long       float64
	ReqAtual   string
	SetorAtual string
	Prio       int
	DistSetor  float64 // distância ao setor da missão atual
}
