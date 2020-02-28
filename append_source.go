package go_streams

import (
	"fmt"
	"time"
)

const appendSourceName = "appendSource"

type AppendSource struct {
	name         string
	latestCommit string

	appendCh   EntryChannel
	closeCh    chan bool
	bufferSize int
}

func NewAppendSource(bufferSize int) *AppendSource {
	name := fmt.Sprintf("%s-%d", appendSourceName, time.Now().UnixNano())
	return &AppendSource{
		name:       name,
		appendCh:   make(chan Entry, bufferSize),
		closeCh:    make(chan bool, 1),
		bufferSize: bufferSize,
	}
}

func (this *AppendSource) Start(channel EntryChannel, errorChannel ErrorChannel) {
	logger.Info("Starting append source with buffer size of: %d", this.bufferSize)
Loop:
	for {
		select {
		case <-this.closeCh:
			logger.Debug("close message received by append source event loop")
			close(channel)
			break Loop
		case elem, ok := <-this.appendCh:
			if !ok {
				logger.Warn("failed to pull new message from the append channel, breaking the event loop")
				break Loop
			}
			channel <- elem
		}
	}
	errorChannel <- NewEofError(this)
	logger.Info("Append source stopped")
}

func (this *AppendSource) Stop() error {
	logger.Info("Stopping append source...")
	this.closeCh <- true
	return nil
}

func (this *AppendSource) CommitEntry(keys ...string) error {
	logger.Debug("Committing entry: %s", this.latestCommit)
	this.latestCommit = keys[len(keys)-1]
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

func (this *AppendSource) Ping() error {
	return nil
}

func (this *AppendSource) LatestCommit() string {
	return this.latestCommit
}
