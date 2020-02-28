package go_streams

import (
	"fmt"
	"reflect"
	"time"
)

type streamAndProcessor struct {
	stream    Stream
	processor Processor
}

type engine struct {
	streams          map[string]streamAndProcessor
	processorFactory ProcessorFactory
	errorHandler     ErrorHandler
	errorChannel     ErrorChannel
	stopChannel      chan bool
	stoppedStreams   int
	monitorTicker    *time.Ticker
}

func NewEngine(processor ProcessorFactory, monitorInterval time.Duration) *engine {
	return &engine{
		processorFactory: processor,
		errorChannel:     make(ErrorChannel),
		stopChannel:      make(chan bool),
		streams:          make(map[string]streamAndProcessor),
		monitorTicker:    time.NewTicker(monitorInterval),
	}
}

func (this *engine) Add(streams ...Stream) error {
	for _, stream := range streams {
		_, found := this.streams[stream.GetSource().Name()]
		if found {
			return NewSameSourceError(stream.GetSource())
		}
		this.streams[stream.GetSource().Name()] = streamAndProcessor{
			stream:    stream,
			processor: this.processorFactory(),
		}
	}

	return nil
}

func (this *engine) SetErrorHandler(handler ErrorHandler) {
	this.errorHandler = handler
}

func (this *engine) Start() {
	logger.Info("Starting engine...")
	go this.monitor()

	if len(this.streams) == 0 {
		return
	}

	go this.consumeErrors()

	for _, s := range this.streams {
		go s.stream.Process(s.processor, this.errorChannel)
	}

	<-this.stopChannel
	logger.Info("Engine stopped")
}

func (this *engine) Stop() {
	logger.Info("Stopping engine...")
	this.monitorTicker.Stop()
	for _, s := range this.streams {
		err := s.stream.GetSource().Stop()
		if err != nil {
			this.errorChannel <- err
		}
	}

	this.stopChannel <- true
}

func (this *engine) consumeErrors() {
	for {
		err := <-this.errorChannel
		switch e := err.(type) {
		case *EofError:
			this.handleSourceEof(e.source)
		default:
			logger.Error(e.Error())
			if this.errorHandler != nil && e != nil {
				go this.errorHandler(e)
			}
		}
	}
}

func (this *engine) handleSourceEof(source Source) {
	_, found := this.streams[source.Name()]
	if found {
		this.stoppedStreams += 1
	}

	if this.stoppedStreams == len(this.streams) {
		this.stopChannel <- true
		this.monitorTicker.Stop()
	}
}

func (this *engine) monitor() {
	for {
		_, ok := <-this.monitorTicker.C
		if !ok {
			break
		}

		for _, stream := range this.streams {
			if err := checkSource(stream.stream.GetSource(), 3, 1*time.Second); err != nil {
				panic(err)
			}

			if err := checkSinks(stream.stream.GetHandlers(), 3, 1*time.Second); err != nil {
				panic(err)
			}
		}
	}
}

func checkSinks(handlers []interface{}, retries int, backoff time.Duration) error {
	for _, handler := range handlers {
		switch sink := handler.(type) {
		case Sink:
			if err := checkSource(sink, retries, backoff); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkSource(source Pingable, retries int, backoff time.Duration) error {
	for i := 0; i < retries; i++ {
		if err := source.Ping(); err != nil {
			time.Sleep(time.Duration(i+1) * backoff)
		} else {
			return nil
		}
	}
	return fmt.Errorf("source with name: %s isn't availiable after %d retries", reflect.ValueOf(source).String(), retries)
}
