#!/bin/bash
# Simula um sensor enviando mensagens UDP para o broker
# Uso: ./test_sensor.sh [porta] [topico]

PORT=${1:-8001}
TOPIC=${2:-sensor.gps}

echo "Enviando mensagens para localhost:$PORT tópico=$TOPIC"
echo "Ctrl+C para parar"

i=1
while true; do
  MSG=$(printf '{"topic":"%s","from":"sensor-sim-1","payload":{"lat":-27.%d,"lon":-48.%d,"alt":10,"seq":%d}}' \
    "$TOPIC" $((RANDOM % 100)) $((RANDOM % 100)) $i)
  echo "$MSG" | nc -u -w1 localhost "$PORT"
  echo "[$i] enviado: $MSG"
  i=$((i+1))
  sleep 1
done
