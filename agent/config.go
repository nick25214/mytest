package agent

import (
	"mq_test/glob"

	"github.com/Sirupsen/logrus"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/core/helper"
)

type AgentConfig struct {
	RcvLimitPerTick     int
	RcvTickSecond       int
	SendLimitPerTick    int
	SendTickSecond      int
	QueueUpperBound     int64
	WaitTickMicroSecond int
	MqPoolSize          int
}

func initConfig() *AgentConfig {
	c := glob.LoadConfig()
	agentCfg := &AgentConfig{}
	agentCfg.RcvLimitPerTick = helper.Atoi(c.GetValue("agent", "rcv_limit_per_tick", "100"), 100)
	agentCfg.RcvTickSecond = helper.Atoi(c.GetValue("agent", "rcv_tick_second", "1"), 1)
	agentCfg.SendLimitPerTick = helper.Atoi(c.GetValue("agent", "send_limit_per_tick", "100"), 100)
	agentCfg.SendTickSecond = helper.Atoi(c.GetValue("agent", "send_tick_second", "1"), 1)
	agentCfg.QueueUpperBound = int64(helper.Atoi(c.GetValue("agent", "queue_upper_bound", "10000"), 10000))
	agentCfg.WaitTickMicroSecond = helper.Atoi(c.GetValue("agent", "wait_tick_microsecond", "100"), 100)
	agentCfg.MqPoolSize = helper.Atoi(c.GetValue("agent", "mq_pool", "10"), 10)
	level := c.GetValue("log", "level", "error")

	if l, err := logrus.ParseLevel(level); err != nil {
		logrus.SetLevel(logrus.ErrorLevel)
	} else {
		logrus.SetLevel(l)
	}

	return agentCfg
}
