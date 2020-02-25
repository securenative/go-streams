package go_streams

import (
	"fmt"
	"time"
)

const sequentialIntegerSourceName = "sequentialIntegerSource"

type sequentialIntegerSource struct {
	latestCommit string
	closeCh      chan bool
	limit        int
	delay        time.Duration
	name         string
}

func NewSequentialIntegerSource(limit int, delay time.Duration) Source {
	name := fmt.Sprintf("%s-%d", sequentialIntegerSourceName, time.Now().UnixNano())
	return &sequentialIntegerSource{closeCh: make(chan bool, 1), limit: limit, delay: delay, name: name}
}

func (this *sequentialIntegerSource) Start(channel EntryChannel, errorChannel ErrorChannel) {
	num := 0
Loop:
	for {
		time.Sleep(this.delay)
		select {
		case <-this.closeCh:
			close(channel)
			break Loop
		default:
			if this.limit <= num && this.limit > 0 {
				_ = this.Stop()
			}
			channel <- Entry{
				Key:   fmt.Sprintf("%d", num),
				Value: num,
			}
			num++
		}
	}
	errorChannel <- NewEofError(this)
}

func (this *sequentialIntegerSource) Stop() error {
	this.closeCh <- true
	return nil
}

func (this *sequentialIntegerSource) CommitEntry(key string) error {
	this.latestCommit = key
	return nil
}

func (this *sequentialIntegerSource) Name() string {
	return this.name
}
