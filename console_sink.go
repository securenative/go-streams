package go_streams

import (
	"log"
)

type consoleSink struct{}

func NewConsoleSink() *consoleSink {
	return &consoleSink{}
}

func (this *consoleSink) Single(entry Entry) error {
	log.Printf("[ConsoleSink] Key: %s, Value: %+v", entry.Key, entry.Value)
	return nil
}

func (this *consoleSink) Batch(entry ...Entry) error {
	for idx := range entry {
		_ = this.Single(entry[idx])
	}
	return nil
}
