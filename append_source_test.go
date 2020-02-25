package go_streams

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAppendSource_Append(t *testing.T) {
	// We only write 4 items before the stream starts,
	// higher number of appends will block
	source := NewAppendSource(4)
	processor := NewDirectProcessor()
	sink := NewArraySink()
	errs := make(ErrorChannel, 1)

	stream := NewStream(source).
		Filter(onlyIntegers).
		Sink(sink)

	source.Append("string", "hello world")
	source.Append("integer", 123)
	source.Append("float", 123.2)
	source.Append("struct", Entry{})

	// Append more items after 100ms:
	go func() {
		time.Sleep(100 * time.Millisecond)
		source.Append("integer", 321)
		source.Append("string", "hello cruel world")
		source.Append("integer-last", 213)
	}()

	// Stop the source after 600ms
	go func() {
		time.Sleep(600 * time.Millisecond)
		err := source.Stop()
		assert.Nil(t, err)
	}()

	stream.Process(processor, errs)

	assert.EqualValues(t, []interface{}{123, 321, 213}, sink.Array())
	assert.EqualValues(t, "integer-last", source.LatestCommit())
}

func onlyIntegers(entry interface{}) bool {
	_, ok := entry.(int)
	return ok
}
