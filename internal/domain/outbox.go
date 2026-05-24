package domain

type OutboxEntry struct {
	ID      string
	Topic   string
	Payload []byte
}
