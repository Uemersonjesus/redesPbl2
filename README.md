# REDESPBL22026 — Estreito de Ormuz

Infraestrutura distribuída para coordenação de drones autônomos de monitoramento marítimo.

Implementa consenso Raft, fila de prioridade distribuída, sensores via WebSocket e drones via RPC — tudo contêinerizado com Docker.

---

## Execução local (mesmo PC) — testado e validado

Esta é a forma recomendada e que foi testada. Todos os componentes rodam no mesmo PC em containers Docker na rede `raft-shared`.

### 1. Pré-requisitos

- Docker Desktop instalado e rodando
- Git (para clonar o repositório)


Preencha com o IP real do seu PC no .env.

```dotenv
B1_IP=192.168.100.64
B2_IP=192.168.100.64
B3_IP=192.168.100.64

# ── IPs dos sensores ─────────────────────────────────────────────────
SENSOR_NORTE_IP=192.168.100.64
SENSOR_CENTRO_IP=192.168.100.64
SENSOR_SUL_IP=192.168.100.64

# ── IP da máquina dos drones ──────────────────────────────────────────
# Para o broker chamar o drone de volta via RPC.
# MESMO PC: o drone usará seu hostname Docker automaticamente

# ── Configurações do drone  ────────────────────────────────

DRONE_ID=drone-1
DRONE_RPC_PORT=7101
LAT_INICIAL=26.60
LONG_INICIAL=56.20
```

### 3. Subir os brokers (obrigatoriamente nesta ordem)

O broker1 cria a rede `raft-shared`. Os demais entram nela.

```cmd
docker compose -f docker-compose.broker1.yml up --build 
docker compose -f docker-compose.broker2.yml up --build 
docker compose -f docker-compose.broker3.yml up --build 
```

Aguarde ~5 segundos para a eleição do líder. Verifique:

```cmd
docker logs broker1
docker logs broker2
docker logs broker3
```

Você deve ver um dos brokers com `🏆 ELEITO LÍDER` e os outros com `líder=nodeX`.

### 4. Subir  drone

```cmd
docker compose -f docker-compose.drone.yml up --build 

```

### 5. Subir os sensores

Suba os que quiser — cada um é independente:

```cmd
docker compose -f docker-compose.sensor-norte-radar-1.yml up --build -d
docker compose -f docker-compose.sensor-norte-ais-1.yml up --build -d
docker compose -f docker-compose.sensor-norte-boia-1.yml up --build -d
docker compose -f docker-compose.sensor-norte-clima-1.yml up --build -d
docker compose -f docker-compose.sensor-centro-radar-1.yml up --build -d
docker compose -f docker-compose.sensor-centro-ais-1.yml up --build -d
docker compose -f docker-compose.sensor-centro-boia-1.yml up --build -d
docker compose -f docker-compose.sensor-centro-clima-1.yml up --build -d
docker compose -f docker-compose.sensor-sul-radar-1.yml up --build -d
docker compose -f docker-compose.sensor-sul-ais-1.yml up --build -d
docker compose -f docker-compose.sensor-sul-boia-1.yml up --build -d
docker compose -f docker-compose.sensor-sul-clima-1.yml up --build -d
```

### 6. Acompanhar os logs

```cmd
docker logs -f broker1
docker logs -f drone-1
docker logs -f norte-radar-1
```

---

## Parar containers

**Um container específico:**
```cmd
docker rm -f broker2
docker rm -f drone-1
docker rm -f norte-radar-1
```

**Todos de uma vez (PowerShell):**
```powershell
docker ps -aq | ForEach-Object { docker rm -f $_ }
```

**Todos de uma vez (CMD):**
```cmd
for /f "tokens=*" %i in ('docker ps -aq') do docker rm -f %i
```

---

## Componentes e portas

| Componente | Raft | WS Sensores | RPC Drones |
|---|---|---|---|
| broker1 | 7001 | 8090 | 9010 |
| broker2 | 7002 | 8091 | 9012 |
| broker3 | 7003 | 8092 | 9013 |
| drone-1 | — | — | 7101 |
| drone-2 | — | — | 7102 |
| drone-3 | — | — | 7103 |

Sensores não expõem portas — comunicam apenas via WebSocket de saída para os brokers.

---

## Sensores disponíveis

| Sensor | Setor | Tipo | Prioridade |
|---|---|---|---|
| norte-radar-1 | setor-norte | radar | MÉDIA |
| norte-ais-1 | setor-norte | ais | ALTA |
| norte-boia-1 | setor-norte | boia_impacto | CRÍTICA |
| norte-clima-1 | setor-norte | clima | INFO |
| centro-radar-1 | setor-centro | radar | MÉDIA |
| centro-ais-1 | setor-centro | ais | ALTA |
| centro-boia-1 | setor-centro | boia_impacto | CRÍTICA |
| centro-clima-1 | setor-centro | clima | INFO |
| sul-radar-1 | setor-sul | radar | MÉDIA |
| sul-ais-1 | setor-sul | ais | ALTA |
| sul-boia-1 | setor-sul | boia_impacto | CRÍTICA |
| sul-clima-1 | setor-sul | clima | INFO |

---

## SENSOR_DEMO_MODE

| Valor | Comportamento |
|---|---|
| `true` | Probabilidades altas (5–8%) — fila enche em ~1 minuto |
| `false` | 1% por leitura — operação realista |

---

---

## Rede Docker

Todos os containers rodam na rede bridge `raft-shared`.
O broker1 cria a rede. Os demais entram com `external: true`.
A comunicação interna usa hostnames Docker (`broker1`, `drone-1`, etc).
As portas mapeadas no `ports:` permitem acesso externo (LAN ou outro PC).

---

## Arquitetura resumida

```
Sensores (WebSocket)
    │
    ▼
Broker Líder ──── Raft ──── Broker Follower × 2
    │                           (replicam todo estado)
    ▼
Scheduler
    │  
    ▼
Drone (RPC)
    │
    ▼
Broker (conclusão via RPC → replicada no log Raft)
```
