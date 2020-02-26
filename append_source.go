package go_streams

import (
	"fmt"
	"time"
)

const appendSourceName = "appendSource"

type AppendSource struct {
	name         string
	latestCommit string

	appendCh EntryChannel
	closeCh  chan bool
}

func NewAppendSource(bufferSize int) *AppendSource {
	name := fmt.Sprintf("%s-%d", appendSourceName, time.Now().UnixNano())
	return &AppendSource{
		name:     name,
		appendCh: make(chan Entry, bufferSize),
		closeCh:  make(chan bool, 1),
	}
}

func (this *AppendSource) Start(channel EntryChannel, errorChannel ErrorChannel) {
Loop:
	for {
		select {
		case <-this.closeCh:
			close(channel)
			break Loop
		case elem, ok := <-this.appendCh:
			if !ok {
				break Loop
			}
			channel <- elem
		}
	}
	errorChannel <- NewEofError(this)
}

func (this *AppendSource) Stop() error {
	this.closeCh <- true
	return nil
}

func (this *AppendSource) CommitEntry(key string) error {
	this.latestCommit = key
	return nil
}

func (this *AppendSource) Name() string {
	return this.name
}

func (this *AppendSource) Append(key string, entry interface{}) {
	this.appendCh <- Entry{
		Key:   key,
		Value: entry,
	}
}

func (this *AppendSource) LatestCommit() string {
	return this.latestCommit
}
