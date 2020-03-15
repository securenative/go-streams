package go_streams

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMapWithContext_Buffered(t *testing.T) {
	source, array, errs := createStreamResources()
	processor := NewBufferedProcessor(5, 256*time.Millisecond)

	stream := NewStream(source).
		MapWithContext(getMapFunc()).
		Sink(array)
	go stream.Process(processor, errs)

	appendNumbers(source)
	assertResults(t, source, array)
}

func TestMapWithContext_Direct(t *testing.T) {
	source, array, errs := createStreamResources()
	processor := NewDirectProcessor()

	stream := NewStream(source).
		MapWithContext(getMapFunc()).
		Sink(array)
	go stream.Process(processor, errs)

	appendNumbers(source)
	assertResults(t, source, array)
}

func createStreamResources() (*AppendSource, *ArraySink, ErrorChannel) {
	source := NewAppendSource(5)
	array := NewArraySink()
	errs := make(ErrorChannel, 5)
	return source, array, errs
}

func assertResults(t *testing.T, source *AppendSource, array *ArraySink) {
	time.Sleep(time.Second)
	assert.NoError(t, source.Stop())
	expected := []interface{}{11, 12, 13, 14, 15}
	assert.Equal(t, expected, array.Array())
}

func appendNumbers(source *AppendSource) {
	source.Append("1", 1)
	source.Append("2", 2)
	source.Append("3", 3)
	source.Append("4", 4)
	source.Append("5", 5)
}

func getMapFunc() MapWithContext {
	ctx := make(map[string]interface{})
	ctx["addNum"] = 10
	return MapWithContext{
		GetContextFunc: func() map[string]interface{} {
			return ctx
		},
		MapWithContextFunc: func(context map[string]interface{}, entry interface{}) interface{} {
			addNum := context["addNum"].(int)
			num := entry.(int)
			return num + addNum
		},
	}
}