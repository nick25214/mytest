package ratecontroller

import (
	"sync"
	"time"
)

type RateController struct {
	time     time.Time
	interval time.Duration
	count    int
	stop     bool
	limit    int
	total    int
	begin    time.Time
	locker   *sync.RWMutex
}

func InitRateController(intervalSecond, limit int) *RateController {
	return &RateController{
		time:     time.Now(),
		begin:    time.Now().Add(-1 * time.Second),
		total:    1,
		interval: time.Duration(intervalSecond) * time.Second,
		limit:    limit,
		locker:   new(sync.RWMutex),
	}
}

func (this *RateController) Run() {
	go func() {
		ticker := time.NewTicker(this.interval)
		for range ticker.C {
			if !this.reset() {
				ticker.Stop()
				return
			}
		}
	}()
}

func (this *RateController) reset() bool {
	this.locker.Lock()
	defer this.locker.Unlock()
	if this.stop {
		return false
	}

	this.count = 0
	this.time = time.Now()
	return true
}

func (this *RateController) Stop() {
	this.locker.Lock()
	defer this.locker.Unlock()
	this.stop = true
}

func (this *RateController) Valid() bool {
	this.locker.Lock()
	defer this.locker.Unlock()
	this.count++
	if this.count > this.limit && time.Now().Before(this.time.Add(this.interval)) {
		return false
	}

	return true
}

func (this *RateController) Count() int {
	this.locker.RLock()
	defer this.locker.RUnlock()
	return this.count
}

func (this *RateController) Limit() int {
	return this.limit
}

func (this *RateController) Avg() int {
	this.locker.RLock()
	defer this.locker.RUnlock()

	s := int(time.Now().Sub(this.begin).Seconds())
	avg := this.total / s
	return avg
}

func (this *RateController) Incr() {
	this.locker.Lock()
	defer this.locker.Unlock()

	this.total = this.total + 1
}
