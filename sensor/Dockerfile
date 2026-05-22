FROM golang:1.22-alpine

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
ENV GONOSUMDB=*
ENV GOPROXY=direct
RUN go mod download

COPY . .
RUN go build -v -o /app/sensor-bin ./sensor/
RUN ls -la /app/sensor-bin

ENV SENSOR_ID=sensor-1
ENV SETOR_ID=setor-1
ENV TIPO=radar
ENV LAT=26.3
ENV LONG=56.7
ENV BROKER_WS_ADDRS=broker1:8090,broker2:8090,broker3:8090

CMD ["./sensor-bin"]
