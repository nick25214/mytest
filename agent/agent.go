package agent

import (
	"fmt"
	"mq_test/agent/ratecontroller"
	"mq_test/message"
	"mq_test/mq"
	"sync"
	"time"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
)

type Agent struct {
	sendLocker         sync.RWMutex
	rcvLocker          sync.RWMutex
	mq                 mq.MQ
	sendPool           []*MQState
	rcvPool            []*MQState
	config             *AgentConfig
	rcvRateController  *ratecontroller.RateController
	sendRateController *ratecontroller.RateController
}

type MQState struct {
	mq     mq.MQ
	isFree bool
}

func createMQPool(size int) []*MQState {
	var pool []*MQState
	for i := 0; i < size; i++ {
		m, _ := mq.CreateMQ()
		pool = append(pool, &MQState{mq: m, isFree: true})
	}
	return pool
}

func InitAgent() *Agent {
	c := initConfig()
	rcvRateController := ratecontroller.InitRateController(c.RcvTickSecond, c.RcvLimitPerTick)
	sendRateController := ratecontroller.InitRateController(c.SendTickSecond, c.SendLimitPerTick)
	//m, _ := mq.CreateMQ()
	a := &Agent{
		//mq:                 m,
		rcvRateController:  rcvRateController,
		sendRateController: sendRateController,
		config:             c,
	}

	a.sendPool = createMQPool(c.MqPoolSize)
	a.rcvPool = createMQPool(c.MqPoolSize)

	return a
}

func (this *Agent) Start() {
	for _, m := range this.sendPool {
		if !m.mq.IsStart() {
			m.mq.Start()
		}
	}

	for _, m := range this.rcvPool {
		if !m.mq.IsStart() {
			m.mq.Start()
		}
	}

	this.rcvRateController.Run()
	this.sendRateController.Run()
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			// r := this.GetFreeRcv()
			// if r != nil {
			// 	tracer.Infof("RCV", "limit is %d, dest len is %d, avg is %d", this.rcvRateController.Limit(), r.mq.SrcLen(), this.rcvRateController.Avg())
			// }
			// this.FreeMq(r)

			// s := this.GetFreeSender()
			// if s != nil {
			// 	tracer.Infof("SEND", "limit is %d, dest len is %d, avg is %d", this.sendRateController.Limit(), s.mq.DestLen(), this.sendRateController.Avg())
			// }
			// this.FreeMq(s)
			tracer.Infof("RCV", "limit is %d, avg is %d", this.rcvRateController.Limit(), this.rcvRateController.Avg())
			tracer.Infof("SEND", "limit is %d, avg is %d", this.sendRateController.Limit(), this.sendRateController.Avg())
		}
	}()
}

func (this *Agent) Close() {
	for _, m := range this.sendPool {
		if !m.mq.IsClose() {
			m.mq.Close()
		}
	}

	for _, m := range this.rcvPool {
		if !m.mq.IsClose() {
			m.mq.Close()
		}
	}
	this.rcvRateController.Stop()
	this.sendRateController.Stop()
}

func (this *Agent) GetFreeSender() *MQState {
	//time.Sleep(10 * time.Microsecond)
	this.sendLocker.Lock()
	defer this.sendLocker.Unlock()
	for _, m := range this.sendPool {
		if m.isFree {
			m.isFree = false
			return m
		}
	}
	return nil
}

func (this *Agent) SetSource(name string) {
	this.rcvLocker.Lock()
	defer this.rcvLocker.Unlock()
	for _, m := range this.rcvPool {
		m.mq.SetSource(name)
	}
}
func (this *Agent) SetDestination(name string) {
	this.sendLocker.Lock()
	defer this.sendLocker.Unlock()
	for _, m := range this.sendPool {
		m.mq.SetDestination(name)
	}
}

func (this *Agent) GetFreeRcv() *MQState {
	//time.Sleep(10 * time.Microsecond)
	this.rcvLocker.Lock()
	defer this.rcvLocker.Unlock()
	for _, m := range this.rcvPool {
		if m.isFree {
			m.isFree = false
			return m
		}
	}
	return nil
}

func (this *Agent) FreeMq(m *MQState) {
	if m != nil {
		m.isFree = true
	}
}

func (this *Agent) Receive() (*message.MQMessage, error) {
	//ticker := time.NewTicker(time.Duration(this.config.WaitTickMicroSecond) * time.Microsecond)
	//for range ticker.C {
	m := this.GetFreeRcv()
	defer this.FreeMq(m)
	if m == nil {
		return nil, fmt.Errorf("[Agent] %s", "no free mq")
	}

	if this.rcvRateController.Valid() {
		msg, err := this.mq.Receive()
		if err != nil {
			return nil, err
		}
		return msg, nil
	}

	return nil, fmt.Errorf("over rcv rate: now is %d, limit is %d, dest len is %d, avg is %d",
		this.rcvRateController.Count(), this.rcvRateController.Limit(), this.mq.SrcLen(), this.rcvRateController.Avg())
}

func (this *Agent) ReceiveWithCallback(callback func(msg *message.MQMessage, err error)) error {

	for i := 0; i < len(this.rcvPool); i++ {
		go func() error {
			if err := this.rcv(callback); err != nil {
				panic(err)
			}
			return nil
		}()
	}

	return nil
}

func (this *Agent) rcv(callback func(msg *message.MQMessage, err error)) error {
	m := this.GetFreeRcv()
	defer this.FreeMq(m)
	if m != nil {
		ticker := time.NewTicker(time.Duration(this.config.WaitTickMicroSecond) * time.Microsecond)
		for range ticker.C {
			if this.rcvRateController.Valid() {
				msg, err := m.mq.Receive()
				this.rcvRateController.Incr()
				if err != nil {
					return err
				}
				callback(msg, nil)
			} else {
				callback(nil, fmt.Errorf("over rcv rate: now is %d, limit is %d, dest len is %d, avg is %d",
					this.rcvRateController.Count(), this.rcvRateController.Limit(), m.mq.SrcLen(), this.rcvRateController.Avg()))
			}
		}
	}

	return nil
}

func (this *Agent) Send(msg *message.MQMessage) error {
	m := this.GetFreeSender()
	defer this.FreeMq(m)
	if m == nil {
		return fmt.Errorf("[Agent] %s", "no free mq")
	}

	if this.sendRateController.Valid() || m.mq.DestLen() < 1 {
		if m.mq.DestLen() <= this.config.QueueUpperBound {
			if err := m.mq.Send(msg); err == nil {
				this.sendRateController.Incr()
				return nil
			} else {
				return err
			}
		}
	}

	return fmt.Errorf("over rate: now is %d, limit is %d, dest len is %d, upperbound is %d, avg is %d",
		this.sendRateController.Count(), this.sendRateController.Limit(), m.mq.DestLen(), this.config.QueueUpperBound, this.sendRateController.Avg())
}

// func (this *Agent) Restart() {
// 	this.locker.Lock()
// 	defer this.locker.Unlock()
// 	this.Close()
// 	this.Start()
// }

// func (this *Agent) Start() {
// 	if !this.mq.IsStart() {
// 		this.mq.Start()
// 		this.rcvRateController.Run()
// 		this.sendRateController.Run()
// 	}
// }

// func (this *Agent) Close() {
// 	if !this.mq.IsClose() {
// 		this.mq.Close()
// 		this.rcvRateController.Stop()
// 		this.sendRateController.Stop()
// 	}
// }

// func (this *Agent) Receive() (*message.MQMessage, error) {
// 	//ticker := time.NewTicker(time.Duration(this.config.WaitTickMicroSecond) * time.Microsecond)
// 	//for range ticker.C {

// 	if this.rcvRateController.Valid() {
// 		msg, err := this.mq.Receive()
// 		if err != nil {
// 			return nil, err
// 		}
// 		return msg, nil
// 	}

// 	return nil, fmt.Errorf("over rcv rate: now is %d, limit is %d, dest len is %d, avg is %d",
// 		this.rcvRateController.Count(), this.rcvRateController.Limit(), this.mq.SrcLen(), this.rcvRateController.Avg())
// }

// func (this *Agent) ReceiveWithCallback(callback func(msg *message.MQMessage, err error)) error {
// 	ticker := time.NewTicker(100 * time.Microsecond)
// 	for range ticker.C {
// 		//for {
// 		if this.rcvRateController.Valid() {
// 			msg, err := this.mq.Receive()
// 			this.rcvRateController.Incr()
// 			if err != nil {
// 				return err
// 			}
// 			callback(msg, nil)
// 		} else {
// 			callback(nil, fmt.Errorf("over rcv rate: now is %d, limit is %d, dest len is %d, avg is %d",
// 				this.rcvRateController.Count(), this.rcvRateController.Limit(), this.mq.SrcLen(), this.rcvRateController.Avg()))
// 		}
// 	}

// 	return nil
// }

// func (this *Agent) Send(msg *message.MQMessage) error {
// 	if this.sendRateController.Valid() || this.mq.DestLen() < 1 {
// 		if this.mq.DestLen() <= this.config.QueueUpperBound {
// 			if err := this.mq.Send(msg); err == nil {
// 				this.sendRateController.Incr()
// 				return nil
// 			} else {
// 				return err
// 			}
// 		}
// 	}

// 	return fmt.Errorf("over rate: now is %d, limit is %d, dest len is %d, upperbound is %d, avg is %d",
// 		this.sendRateController.Count(), this.sendRateController.Limit(), this.mq.DestLen(), this.config.QueueUpperBound, this.sendRateController.Avg())
// }
