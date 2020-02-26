package go_streams

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEngine_HappyCase_SingleStream(t *testing.T) {
	engine := NewEngine(NewDirectProcessorFactory(), 10*time.Second)
	source := NewSequentialIntegerSource(10, time.Millisecond)
	sink := NewArraySink()
	stream := addOneFilterOddsStream(source, sink)
	e := engine.Add(stream)
	assert.Nil(t, e)

	engine.Start()

	assert.NotEmpty(t, sink.Array())
	assert.EqualValues(t, []interface{}{2, 4, 6, 8, 10}, sink.Array())
}

func TestEngine_HappyCase_MultipleStreamsDifferentSource(t *testing.T) {
	engine := NewEngine(NewDirectProcessorFactory(), 10*time.Second)
	source1 := NewSequentialIntegerSource(10, time.Millisecond)
	source2 := NewSequentialIntegerSource(20, time.Millisecond)

	sink1 := NewArraySink()
	sink2 := NewArraySink()

	stream1 := addOneFilterOddsStream(source1, sink1)
	stream2 := addOneFilterOddsStream(source2, sink2)

	e := engine.Add(stream1, stream2)
	assert.Nil(t, e)

	engine.Start()

	assert.NotEmpty(t, sink1.Array())
	assert.EqualValues(t, []interface{}{2, 4, 6, 8, 10}, sink1.Array())

	assert.NotEmpty(t, sink2.Array())
	assert.EqualValues(t, []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18, 20}, sink2.Array())
}

func TestEngine_MultipleStreamsSameSource_ShouldReturnError(t *testing.T) {
	engine := NewEngine(NewDirectProcessorFactory(), 10*time.Second)
	source := NewSequentialIntegerSource(10, time.Millisecond)

	sink1 := NewArraySink()
	sink2 := NewArraySink()

	stream1 := addOneFilterOddsStream(source, sink1)
	stream2 := addOneFilterOddsStream(source, sink2)

	e := engine.Add(stream1, stream2)
	assert.NotNil(t, e)
	sse, ok := e.(*SameSourceError)
	assert.True(t, ok)
	assert.EqualValues(t, source.Name(), sse.source.Name())

	engine.Start()
}

func TestEngine_ErrorHandler_ShouldCatchErrors(t *testing.T) {
	engine := NewEngine(NewDirectProcessorFactory(), 10*time.Second)
	source := NewSequentialIntegerSource(10, time.Millisecond)
	blocker := make(chan error)

	stream := filterPanicsStream(source)

	e := engine.Add(stream)
	assert.Nil(t, e)

	engine.SetErrorHandler(func(err error) {
		blocker <- err
	})

	engine.Start()

	handledErr := <-blocker
	assert.NotNil(t, handledErr)
	_, ok := handledErr.(*FilterError)
	assert.True(t, ok)
}

func TestEngine_Stop_ShouldStopAllStreams(t *testing.T) {
	engine := NewEngine(NewDirectProcessorFactory(), 10*time.Second)
	source1 := NewSequentialIntegerSource(10, 500*time.Millisecond)
	source2 := NewSequentialIntegerSource(20, 250*time.Millisecond)

	sink1 := NewArraySink()
	sink2 := NewArraySink()

	stream1 := addOneFilterOddsStream(source1, sink1)
	stream2 := addOneFilterOddsStream(source2, sink2)

	e := engine.Add(stream1, stream2)
	assert.Nil(t, e)

	engine.SetErrorHandler(func(err error) {
		assert.Fail(t, "No errors should be thrown")
	})

	// Stop the engine after 2 seconds
	go func() {
		time.Sleep(2 * time.Second)
		engine.Stop()
	}()

	engine.Start()

	assert.NotEmpty(t, sink1.Array())
	assert.True(t, len(sink1.Array()) < 5)

	assert.NotEmpty(t, sink2.Array())
	assert.True(t, len(sink1.Array()) < 10)
}
