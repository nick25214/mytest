package message

import (
	"time"

	uuid "github.com/satori/go.uuid"
)

type MQMessage struct {
	ID        string
	Message   string
	TimeStamp time.Time
}

func NewMQMessage(message string) *MQMessage {
	return &MQMessage{
		ID:        uuid.NewV4().String(),
		TimeStamp: time.Now().UTC(),
		Message:   message,
	}
}
