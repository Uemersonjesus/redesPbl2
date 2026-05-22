FROM golang:1.22-alpine

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -v -o /app/redespbl22026 .
RUN ls -la /app/redespbl22026

ENV NODE_ID=node-1

EXPOSE 7001/tcp
EXPOSE 8090/tcp
EXPOSE 9001/tcp
EXPOSE 9010/tcp

CMD ["/app/redespbl22026"]
