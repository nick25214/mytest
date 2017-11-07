package mq

import (
	"mq_test/glob"
	"mq_test/message"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/core/helper"
)

type MQType string

const (
	MQTypeRedis MQType = "ps.micro.service.redismq"
	MQTypeTest  MQType = "ps.micro.service.testmq"
)

var mqPool map[int]MQ

type MQServer struct {
	ServerCfg interface{}
	MQEnginge MQType
	MQSrc     *string
	MQDest    *string
}

type MQ interface {
	connect(server MQServer) (MQ, error)
	Start() error
	Close() error
	IsStart() bool
	IsClose() bool
	Send(msg *message.MQMessage) error
	Receive() (*message.MQMessage, error)
	GetMQSettings() (MQServer, error)
	SrcLen() int64
	DestLen() int64
}

func CreateMQ() (MQ, error) {
	var q MQ
	engine := loadQueueEngine()
	if engine == MQTypeRedis {
		q = &RedisMQ{}
	}

	if engine == MQTypeTest {
		q = &TestMQ{}
	}

	queue, err := q.connect(loadServerConfig(engine))
	return queue, err
}

func loadQueueEngine() MQType {
	c := glob.LoadConfig()
	eng := c.GetValue("agent", "engine", "")
	if eng == "redis" {
		return MQTypeRedis
	}

	return MQTypeRedis
}

func loadServerConfig(t MQType) MQServer {
	server := &MQServer{}
	if t == MQTypeRedis {
		loadRedisConfig(server)
	}

	return *server
}

func loadRedisConfig(server *MQServer) {
	c := glob.LoadConfig()
	server.ServerCfg = RedisServer{
		Password:       c.GetValue("redis", "password", ""),
		Server:         c.GetValue("redis", "server", ""),
		Pool:           helper.Atoi(c.GetValue("redis", "pool", ""), 0),
		ConnectTimeout: helper.Atoi(c.GetValue("redis", "timeout_second", "1"), 1),
	}

	src := c.GetValue("redis", "source_queue", "")
	dest := c.GetValue("redis", "destination_queue", "")

	if helper.HasValue(src) {
		server.MQSrc = &src
	}

	if helper.HasValue(dest) {
		server.MQDest = &dest
	}
	server.MQEnginge = MQTypeRedis
}
