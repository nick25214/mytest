package mq

import (
	"fmt"
	"mq_test/message"
	"sync"
)

type AbstractMQ struct {
	server MQServer
	locker sync.RWMutex
}

func (this *AbstractMQ) connect(server MQServer) (MQ, error) {
	this.server = server
	return nil, fmt.Errorf("does not implement")
}

func (this *AbstractMQ) Start() error {
	return fmt.Errorf("does not implement")
}

func (this *AbstractMQ) Send(msg *message.MQMessage) error {

	return fmt.Errorf("does not implement")
}

func (this *AbstractMQ) Receive() (*message.MQMessage, error) {
	return &message.MQMessage{Message: "does not implement"}, fmt.Errorf("does not implement")
}

func (this *AbstractMQ) Close() error {
	return fmt.Errorf("does not implement")
}

func (this *AbstractMQ) Exists() bool {
	return false
}

func (this *AbstractMQ) DestLen() int64 {
	return -1
}

func (this *AbstractMQ) SrcLen() int64 {
	return -1
}

func (this *AbstractMQ) GetMQSettings() (MQServer, error) {
	return this.server, nil
}

func (this *AbstractMQ) IsStart() bool {
	return false
}

func (this *AbstractMQ) IsClose() bool {
	return false
}

func (this *AbstractMQ) Ack() error {
	return nil
}

func (this *AbstractMQ) SetSource(name string) error {
	return nil
}
func (this *AbstractMQ) SetDestination(name string) error {
	return nil
}
