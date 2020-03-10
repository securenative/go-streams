package go_streams

type CallbackFunc func(entries ...Entry) error

type callbackSink struct {
	cb CallbackFunc
}

func NewCallbackSink(cb CallbackFunc) *callbackSink {
	return &callbackSink{cb: cb}
}

func (this *callbackSink) Ping() error {
	return nil
}

func (this *callbackSink) Single(entry Entry) error {
	return this.cb(entry)
}

func (this *callbackSink) Batch(entry ...Entry) error {
	return this.cb(entry...)
}
