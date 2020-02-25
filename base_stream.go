package go_streams

type baseStream struct {
	source Source
	ops    []interface{}
}

func NewStream(source Source) *baseStream {
	return &baseStream{source: source}
}

func (this *baseStream) Filter(fn FilterFunc) Stream {
	this.ops = append(this.ops, fn)
	return this
}

func (this *baseStream) Map(fn MapFunc) Stream {
	this.ops = append(this.ops, fn)
	return this
}

func (this *baseStream) Sink(sink Sink) Stream {
	this.ops = append(this.ops, sink)
	return this
}

func (this *baseStream) GetHandlers() []interface{} {
	return this.ops
}

func (this *baseStream) Process(processor Processor, errs ErrorChannel) {
	processor.Process(this, errs)
}

func (this *baseStream) GetSource() Source {
	return this.source
}
