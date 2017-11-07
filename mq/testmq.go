package mq

import (
	"encoding/json"
	"errors"
	"mq_test/message"
	"time"

	"github.com/garyburd/redigo/redis"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
)

type LocalMQ struct {
	AbstractMQ
	queue    *redis.Pool
	src      *string
	dest     *string
	srcConn  redis.Conn
	destConn redis.Conn
	server   RedisServer
}

type LocalServer struct {
	Server         string
	Pool           int
	MaxIdle        int
	MaxActive      int
	ConnectTimeout int
	IdleTimeout    int
	ReadTimeout    int
	WriteTimeout   int
	Password       string
}

func (this *LocalMQ) connect(server MQServer) (MQ, error) {
	this.locker.Lock()
	defer this.locker.Unlock()
	this.server = server.ServerCfg.(RedisServer)
	s := this.server
	this.src = server.MQSrc
	this.dest = server.MQDest
	maxIdle := s.MaxIdle
	maxActive := s.MaxActive // max number of connections
	host := s.Server
	idleTimeout := time.Duration(s.IdleTimeout) * time.Second
	connectTimeout := s.ConnectTimeout
	readTimeout := s.ReadTimeout
	writeTimeout := s.WriteTimeout
	password := s.Password

	this.queue = &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: idleTimeout,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host,
				redis.DialPassword(password),
				redis.DialConnectTimeout(time.Duration(connectTimeout)*time.Second),
				redis.DialReadTimeout(time.Duration(readTimeout)*time.Second),
				redis.DialWriteTimeout(time.Duration(writeTimeout)*time.Second),
				redis.DialDatabase(s.Pool))
			if err != nil {
				tracer.Error("redis", err)
				if c != nil {
					c.Close()
				}

				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < idleTimeout {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	return this, nil
}

func (this *LocalMQ) Send(msg *message.MQMessage) error {
	// this.locker.Lock()
	// defer this.locker.Unlock()
	if this.destConn == nil {
		return errors.New("cannot get redis pool")
	}

	if bytes, err := json.Marshal(msg); err != nil {
		return err
	} else {
		if err := this.destConn.Send("RPUSH", *this.dest, string(bytes)); err != nil {
			return err
		}

		if err := this.destConn.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (this *LocalMQ) Receive() (*message.MQMessage, error) {
	// this.locker.Lock()
	// defer this.locker.Unlock()
	if this.srcConn == nil {
		return nil, errors.New("cannot get redis pool")
	}

	obj, err := this.srcConn.Do("BLPOP", *this.src, this.server.ConnectTimeout)

	if err != nil {
		return nil, err
	}

	if obj == nil { // 队列为空
		return nil, nil
	}

	msg := message.NewMQMessage("")
	switch obj.(type) {
	case []interface{}:
		vals := obj.([]interface{})
		if err := json.Unmarshal(vals[1].([]byte), &msg); err != nil {
			return nil, err
		}
	}

	return msg, nil
}

func (this *LocalMQ) Start() error {
	this.locker.Lock()
	defer this.locker.Unlock()
	this.destConn = this.queue.Get()
	this.srcConn = this.queue.Get()
	return nil
}

func (this *LocalMQ) Close() error {
	this.locker.Lock()
	defer this.locker.Unlock()
	this.srcConn.Close()
	this.srcConn = nil
	this.destConn.Close()
	this.destConn = nil
	return nil
}

func (this *LocalMQ) SrcLen() int64 {
	// this.locker.RLock()
	// defer this.locker.RUnlock()
	if val, err := redis.Int64(this.srcConn.Do("LLEN", *this.src)); err != nil {
		return -1
	} else {
		return val
	}
}

func (this *LocalMQ) DestLen() int64 {
	// this.locker.RLock()
	// defer this.locker.RUnlock()
	if val, err := redis.Int64(this.destConn.Do("LLEN", *this.dest)); err != nil {
		return -1
	} else {
		return val
	}
}

func (this *LocalMQ) IsStart() bool {
	// this.locker.RLock()
	// defer this.locker.RUnlock()
	return this.srcConn != nil && this.destConn != nil
}

func (this *LocalMQ) IsClose() bool {
	return !this.IsStart()
}
