package localmq

import (
	"strings"
	"sync"
	"time"

	"github.com/satori/uuid"
)

const mqLimit int = 10000

type MQSettings struct {
	Server string
	Queue  string
	Ack    AckMode
}

type AckMode int

const (
	ClientAck AckMode = iota
	AutoAck
)

type Messages struct {
	lock          sync.RWMutex
	Messages      map[string]*Message
	orderMessages []*Message
}

type Message struct {
	ID        string
	TimeStamp time.Time
	Locked    bool
	Message   interface{}
}

type QueueServer struct {
	lock   sync.RWMutex
	Queues []*Queue
}

type Queue struct {
	lock     sync.RWMutex
	Name     string
	Messages *Messages
}

var server *QueueServer

type LocalMQ struct {
}

func init() {
	server = &QueueServer{}
}

func (this *LocalMQ) Put(desc *MQSettings, msg *Message) error {
	q := this.getQueue(desc)
	q.Messages.lock.Lock()
	defer q.Messages.lock.Unlock()
	if this.Count(desc) >= mqLimit {
		this.popWithAck(desc)
	}
	msg.ID = uuid.NewV4().String()
	msg.TimeStamp = time.Now().UTC()
	q.Messages.Messages[msg.ID] = msg
	q.Messages.orderMessages = append(q.Messages.orderMessages, msg)
	return nil
}

func (this *LocalMQ) Pop(desc *MQSettings) (*Message, error) {
	q := this.getQueue(desc)
	q.Messages.lock.Lock()
	defer q.Messages.lock.Unlock()
	for _, m := range q.Messages.orderMessages {
		if m.Locked == true {
			continue
		}
		m.Locked = true
		return m, nil
	}
	return nil, nil
}

func (this *LocalMQ) Ack(desc *MQSettings, id string) error {
	q := this.getQueue(desc)
	q.Messages.lock.Lock()
	defer q.Messages.lock.Unlock()
	for idx, m := range q.Messages.orderMessages {
		if m.ID == id {
			tmpOrderMsgs := q.Messages.orderMessages[0:idx]
			tmpOrderMsgs = append(q.Messages.orderMessages[idx+1:])
			q.Messages.orderMessages = tmpOrderMsgs
			break
		}
	}
	delete(q.Messages.Messages, id)
	return nil
}

func (this *LocalMQ) PopWithAck(desc *MQSettings) (*Message, error) {
	q := this.getQueue(desc)
	q.Messages.lock.Lock()
	defer q.Messages.lock.Unlock()
	if len(q.Messages.Messages) > 0 {
		var msg *Message
		for idx, m := range q.Messages.orderMessages {
			if m.Locked == true {
				continue
			}
			msg = m
			tmpOrderMsgs := q.Messages.orderMessages[0:idx]
			tmpOrderMsgs = append(q.Messages.orderMessages[idx+1:])
			q.Messages.orderMessages = tmpOrderMsgs
			delete(q.Messages.Messages, m.ID)
			break
		}
		return msg, nil
	}
	return nil, nil
}

func (this *LocalMQ) popWithAck(desc *MQSettings) (*Message, error) {
	q := this.getQueue(desc)
	if len(q.Messages.Messages) > 0 {
		var msg *Message
		for idx, m := range q.Messages.orderMessages {
			if m.Locked == true {
				continue
			}
			msg = m
			tmpOrderMsgs := q.Messages.orderMessages[0:idx]
			tmpOrderMsgs = append(q.Messages.orderMessages[idx+1:])
			q.Messages.orderMessages = tmpOrderMsgs
			delete(q.Messages.Messages, m.ID)
			break
		}
		return msg, nil
	}
	return nil, nil
}

func (this *LocalMQ) getQueue(desc *MQSettings) *Queue {
	server.lock.Lock()
	defer server.lock.Unlock()
	for _, q := range server.Queues {
		if strings.ToLower(q.Name) == strings.ToLower(desc.Queue) {
			return q
		}
	}

	msgs := &Messages{}
	msgs.Messages = make(map[string]*Message, mqLimit)
	newQueue := &Queue{Name: desc.Queue, Messages: msgs}
	server.Queues = append(server.Queues, newQueue)
	return newQueue
}

func (this *LocalMQ) Count(desc *MQSettings) int {
	return len(this.getQueue(desc).Messages.Messages)
}
