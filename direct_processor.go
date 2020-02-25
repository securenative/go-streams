package go_streams

import "log"

type directProcessor struct {
	entryCh EntryChannel
}

func NewDirectProcessor() *directProcessor {
	return &directProcessor{entryCh: make(EntryChannel)}
}

func NewDirectProcessorFactory() ProcessorFactory {
	return func() Processor {
		return NewDirectProcessor()
	}
}

func (this *directProcessor) Process(stream Stream, errs ErrorChannel) {
	handlers := stream.GetHandlers()

	// Notify the source to start sending entries to the channel:
	go stream.GetSource().Start(this.entryCh, errs)

EntryLoop:
	for {
		entry, ok := <-this.entryCh
		if !ok {
			break
		}

		// Process the entry:
		for idx := range handlers {
			switch handler := handlers[idx].(type) {
			case FilterFunc:
				if !RecoverFilter(handler, entry, errs) {
					entry.Filtered = true
					continue EntryLoop
				}

			case MapFunc:
				entry.Value = RecoverMap(handler, entry, errs)

			case Sink:
				if err := RecoverSinkSingle(handler, entry, errs); err != nil {
					errs <- err
				} else {
					if err := stream.GetSource().CommitEntry(entry.Key); err != nil {
						errs <- err
					}
				}

			default:
				_ = stream.GetSource().Stop()
				log.Fatalf("unknown handler type: %+v", handler)
			}
		}
	}
}
