package go_streams

type ArraySink struct {
	array []interface{}
}

func NewArraySink() *ArraySink {
	return &ArraySink{}
}

func (this *ArraySink) Single(entry Entry) error {
	this.array = append(this.array, entry.Value)
	return nil
}

func (this *ArraySink) Batch(entry ...Entry) error {
	for idx := range entry {
		this.array = append(this.array, entry[idx].Value)
	}
	return nil
}

func (this *ArraySink) Array() []interface{} {
	return this.array
}
