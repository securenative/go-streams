package go_streams

import (
	"log"
	"time"
)

type bufferedProcessor struct {
	entryCh    EntryChannel
	size       int
	timeout    time.Duration
	buffer     []Entry
	bufferKeys []string
}

func NewBufferedProcessor(size int, timeout time.Duration) *bufferedProcessor {
	return &bufferedProcessor{
		timeout:    timeout,
		size:       size,
		entryCh:    make(EntryChannel, size),
		buffer:     make([]Entry, size),
		bufferKeys: make([]string, size),
	}
}

func NewBufferedProcessorFactory(size int, timeout time.Duration) ProcessorFactory {
	return func() Processor {
		return NewBufferedProcessor(size, timeout)
	}
}

func (this *bufferedProcessor) Process(stream Stream, errs ErrorChannel) {
	handlers := stream.GetHandlers()
	bufferIdx := 0
	go stream.GetSource().Start(this.entryCh, errs)
	timeoutCh := time.Tick(this.timeout)

Loop:
	for {
		if bufferIdx == this.size {
			this.processBuffer(stream.GetSource(), this.buffer, this.bufferKeys, handlers, errs)
			bufferIdx = 0
		}
		select {
		case <-timeoutCh:
			this.processBuffer(stream.GetSource(), this.buffer[0:bufferIdx], this.bufferKeys[0:bufferIdx], handlers, errs)
			bufferIdx = 0

		case entry, ok := <-this.entryCh:
			if !ok {
				break Loop
			}
			this.buffer[bufferIdx] = entry
			this.bufferKeys[bufferIdx] = entry.Key
			bufferIdx++
		}
	}
	this.processBuffer(stream.GetSource(), this.buffer[0:bufferIdx], this.bufferKeys[0:bufferIdx], handlers, errs)
	bufferIdx = 0
}

func (this *bufferedProcessor) processBuffer(source Source, entries []Entry, keys []string, handlers []interface{}, errs ErrorChannel) {
	if len(entries) == 0 {
		return
	}

	filteredCount := 0
	for idx := range handlers {
		switch handler := handlers[idx].(type) {
		case FilterFunc:
			for idx := range entries {
				if entries[idx].Filtered {
					continue
				}
				if !RecoverFilter(handler, entries[idx], errs) {
					entries[idx].Filtered = true
					filteredCount++
				}
			}

		case MapFunc:
			for idx := range entries {
				if entries[idx].Filtered {
					continue
				}
				entries[idx].Value = RecoverMap(handler, entries[idx], errs)
			}

		case Sink:
			arr := make([]Entry, len(entries)-filteredCount)
			arrIdx := 0
			for idx := range entries {
				if !entries[idx].Filtered {
					arr[arrIdx] = entries[idx]
					arrIdx++
				}
			}
			if len(arr) > 0 {
				if err := RecoverSinkBatch(handler, arr, errs); err != nil {
					errs <- err
				} else {

					if err := source.CommitEntry(keys...); err != nil {
						errs <- err
					}
				}
			}

		default:
			_ = source.Stop()
			log.Fatalf("unknown handler type: %+v", handler)
		}
	}
}
