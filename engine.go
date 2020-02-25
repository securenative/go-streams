package go_streams

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
}

func NewEngine(processor ProcessorFactory) *engine {
	return &engine{
		processorFactory: processor,
		errorChannel:     make(ErrorChannel),
		stopChannel:      make(chan bool),
		streams:          make(map[string]streamAndProcessor),
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
	if len(this.streams) == 0 {
		return
	}

	go this.consumeErrors()

	for _, s := range this.streams {
		go s.stream.Process(s.processor, this.errorChannel)
	}

	<-this.stopChannel
}

func (this *engine) Stop() {
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
	}
}
