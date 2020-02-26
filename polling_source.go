package go_streams

import (
	"fmt"
	"time"
)

const pollingSourceName = "pollingSource"

type OnPoll = func(latestCommit string) ([]Entry, error)

type PollingSource struct {
	name       string
	lastCommit string
	cb         OnPoll

	timer *time.Ticker

	closeCh chan bool
}

func NewPollingSource(interval time.Duration, cb OnPoll) *PollingSource {
	name := fmt.Sprintf("%s-%d", pollingSourceName, time.Now().UnixNano())
	return &PollingSource{
		name:    name,
		cb:      cb,
		timer:   time.NewTicker(interval),
		closeCh: make(chan bool),
	}
}

func (this *PollingSource) Start(channel EntryChannel, errorChannel ErrorChannel) {
Loop:
	for {
		select {
		case <-this.closeCh:
			close(channel)
			break Loop
		case <-this.timer.C:
			if arr, err := this.cb(this.lastCommit); err != nil {
				errorChannel <- err
			} else {
				for idx := range arr {
					channel <- arr[idx]
				}
			}
		}
	}
	errorChannel <- NewEofError(this)
}

func (this *PollingSource) Stop() error {
	this.timer.Stop()
	this.closeCh <- true
	return nil
}

func (this *PollingSource) CommitEntry(key string) error {
	this.lastCommit = key
	return nil
}

func (this *PollingSource) Name() string {
	return this.name
}
