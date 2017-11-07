package main

import (
	"fmt"
	"math/rand"
	"mq_test/agent"
	"mq_test/message"
	"strconv"
	"strings"
	"time"
)

func main() {
	a := agent.InitAgent()
	a.Start()
	defer a.Close()

	go func() {
		e := a.ReceiveWithCallback(func(m *message.MQMessage, err error) {
			if m != nil && err == nil {
				//fmt.Println(m.ID + "-->" + m.Message + " -->" + m.TimeStamp.String() + " " + time.Now().String())
			}

			if err != nil {
				//fmt.Println(err.Error())
			}
		})

		if e != nil {
			//fmt.Println(e.Error())
			if strings.Contains(e.Error(), "use of closed network connection") {
				fmt.Println(e.Error())
			}
		}
	}()

	rand.Seed(time.Now().UnixNano())
	for {
		time.Sleep(1000000 * time.Microsecond)
		go func() {
			r := rand.Intn(5) + 1
			for i := 1; i <= 500*r; i++ {
				m := "test: " + strconv.Itoa(i)
				e := a.Send(message.NewMQMessage(m))
				if e != nil {
					if !strings.Contains(e.Error(), "[Agent]") {
						//fmt.Println(e.Error())
					}
					if strings.Contains(e.Error(), "use of closed network connection") {
						fmt.Println(e.Error())
					}
				}
			}
		}()
	}
}
