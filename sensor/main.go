package main

import (
	"log"
	"os"
	"strconv"
	"strings"
)

// Cada container de sensor roda UM único sensor.
// O sensor conecta no broker líder via WebSocket e envia alertas.
//
// Variáveis de ambiente:
//   SENSOR_ID       — ex: "norte-radar-1"
//   SETOR_ID        — ex: "setor-norte"
//   TIPO            — radar | ais | boia_impacto | clima
//   LAT             — latitude fixa do sensor
//   LONG            — longitude fixa do sensor
//   BROKER_WS_ADDRS — endereços WS dos brokers: "broker1:8090,broker2:8090,broker3:8090"
func main() {
	sensorID    := getenv("SENSOR_ID",        "sensor-1")
	setorID     := getenv("SETOR_ID",         "setor-1")
	tipoStr     := getenv("TIPO",             "radar")
	brokerAddrs := parseBrokerAddrs(getenv("BROKER_WS_ADDRS", "broker1:8090,broker2:8090,broker3:8090"))

	lat  := parseFloat(getenv("LAT",  "26.3"))
	long := parseFloat(getenv("LONG", "56.7"))

	tipo := TipoSensor(tipoStr)
	if _, ok := Perfis[tipo]; !ok {
		log.Fatalf("[SENSOR] tipo inválido: %q (válidos: radar, ais, boia_impacto, clima)", tipoStr)
	}

	sensor := NewSensor(sensorID, setorID, tipo, lat, long)
	client := NewWSClient(sensor, brokerAddrs)

	log.Printf("=== SENSOR | id=%s setor=%s tipo=%s lat=%.4f long=%.4f brokers=%v ===",
		sensorID, setorID, tipoStr, lat, long, brokerAddrs)

	client.Start()

	// Bloqueia para sempre — o WSClient roda em goroutines
	select {}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func parseFloat(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

func parseBrokerAddrs(s string) []string {
	var addrs []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			addrs = append(addrs, p)
		}
	}
	return addrs
}
