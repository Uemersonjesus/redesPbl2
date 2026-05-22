FROM golang:1.22-alpine

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
# O drone não usa gorilla/websocket diretamente,
# mas o go.mod do módulo raiz declara a dependência.
# GOFLAGS=-mod=mod permite compilar sem baixar deps não usadas pelo pacote.
ENV GONOSUMDB=*
ENV GOPROXY=direct
ENV GOFLAGS=-mod=mod

COPY . .
RUN go build -v -o /app/drone-bin ./drone/
RUN ls -la /app/drone-bin

ENV DRONE_ID=drone-1
ENV DRONE_HOST=drone-1
ENV DRONE_RPC_PORT=7100
ENV LAT_INICIAL=26.6
ENV LONG_INICIAL=56.2
ENV BROKER_ADDRS_RPC=broker1:9010,broker2:9010,broker3:9010

EXPOSE 7100
CMD ["./drone-bin"]
