FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o bin/worker ./cmd/worker

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/bin/worker .
ENTRYPOINT ["./worker"]
