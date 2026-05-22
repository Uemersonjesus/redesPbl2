package broker

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// LogLevel controla a verbosidade dos logs do broker.
type LogLevel int

const (
	LogNormal  LogLevel = iota // estado do cluster, fila, drones
	LogVerbose                 // + mensagens individuais de drones e sensores
)

var nivelAtual = LogNormal

// InitLogger configura o nível de log a partir da variável BROKER_LOG_LEVEL.
// Valores: "normal" (padrão) | "verbose"
func InitLogger() {
	switch strings.ToLower(os.Getenv("BROKER_LOG_LEVEL")) {
	case "verbose", "v", "debug":
		nivelAtual = LogVerbose
		log.SetFlags(log.Ltime | log.Lmicroseconds)
		log.Printf("[LOG] modo VERBOSE ativado")
	default:
		nivelAtual = LogNormal
		log.SetFlags(log.Ltime)
	}
}

// IsVerbose retorna true se o log verbose está ativo.
func IsVerbose() bool {
	return nivelAtual >= LogVerbose
}

// Verbose loga apenas se BROKER_LOG_LEVEL=verbose.
func Verbose(format string, args ...interface{}) {
	if nivelAtual >= LogVerbose {
		log.Printf("[V] "+format, args...)
	}
}

// Info sempre loga — eventos importantes independente do nível.
func Info(format string, args ...interface{}) {
	log.Printf(format, args...)
}

// Separador visual para o painel de status.
func Separador(titulo string) string {
	if titulo == "" {
		return strings.Repeat("─", 60)
	}
	lado := (56 - len(titulo)) / 2
	if lado < 1 {
		lado = 1
	}
	return fmt.Sprintf("%s %s %s",
		strings.Repeat("─", lado), titulo, strings.Repeat("─", lado))
}
