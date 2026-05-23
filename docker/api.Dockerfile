FROM golang:1.23-alpine AS builder
WORKDIR /app
RUN go install github.com/swaggo/swag/cmd/swag@latest
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN swag init -g cmd/api/main.go -o docs
RUN go build -o bin/api ./cmd/api

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/bin/api .
ENTRYPOINT ["./api"]
