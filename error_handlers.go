package go_streams

import (
	"fmt"
	"time"
)

type HardLimitErrorHandler struct {
	errorLimit int
	timeLimit  time.Duration

	lastReset    time.Time
	errorCounter int
	ticker       *time.Ticker
}

func NewHardLimitErrorHandler(errorLimit int, resetEvery time.Duration) *HardLimitErrorHandler {
	ticker := time.NewTicker(resetEvery)
	out := &HardLimitErrorHandler{errorLimit: errorLimit, timeLimit: resetEvery, ticker: ticker, errorCounter: 0, lastReset: time.Now()}
	go out.start()
	return out
}

func (this *HardLimitErrorHandler) start() {
	defer this.ticker.Stop()
	for {
		<-this.ticker.C
		this.errorCounter = 0
		this.lastReset = time.Now()
	}
}

func (this *HardLimitErrorHandler) Handle(err error) {
	if err != nil {
		this.errorCounter += 1
	}

	if this.errorCounter == this.errorLimit {
		period := time.Now().Sub(this.lastReset).Seconds()
		panic(fmt.Errorf("error limit reached, %d errors handled in a time period of: %f seconds", this.errorCounter, period))
	}
}
