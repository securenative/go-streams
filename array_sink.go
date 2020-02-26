package go_streams

import "sync"

type ArraySink struct {
	array []interface{}
	mutex *sync.RWMutex
}

func NewArraySink() *ArraySink {
	return &ArraySink{mutex: &sync.RWMutex{}}
}

func (this *ArraySink) Single(entry Entry) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.array = append(this.array, entry.Value)
	return nil
}

func (this *ArraySink) Batch(entry ...Entry) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	for idx := range entry {
		this.array = append(this.array, entry[idx].Value)
	}
	return nil
}

func (this *ArraySink) Ping() error {
	return nil
}

func (this *ArraySink) Array() []interface{} {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.array
}
