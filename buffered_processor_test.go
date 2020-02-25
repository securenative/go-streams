package go_streams

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBufferedProcessor_Process_HappyCase(t *testing.T) {
	errs := make(ErrorChannel, 100)
	source := NewSequentialIntegerSource(10, 1*time.Millisecond)
	sink := NewArraySink()
	stream := addOneFilterOddsStream(source, sink)
	processor := NewBufferedProcessor(10, 1*time.Second)
	stream.Process(processor, errs)

	assert.NotEmpty(t, sink.Array())
	assert.EqualValues(t, []interface{}{2, 4, 6, 8, 10}, sink.Array())
}

func TestBufferedProcessor_Process_VerifyTimeout(t *testing.T) {
	errs := make(ErrorChannel, 100)
	source := NewSequentialIntegerSource(10, 100*time.Millisecond)
	sink := NewArraySink()
	stream := addOneFilterOddsStream(source, sink)
	processor := NewBufferedProcessor(2, 50*time.Second)
	stream.Process(processor, errs)

	assert.NotEmpty(t, sink.Array())
	assert.EqualValues(t, []interface{}{2, 4, 6, 8, 10}, sink.Array())
}

func TestBufferedProcessor_Process_FilterPanics(t *testing.T) {
	errs := make(ErrorChannel, 100)
	source := NewSequentialIntegerSource(10, 1*time.Millisecond)
	stream := filterPanicsStream(source)
	processor := NewBufferedProcessor(10, 1*time.Second)
	stream.Process(processor, errs)

	err := <-errs
	assert.NotNil(t, err)
	_, ok := err.(*FilterError)
	assert.True(t, ok)
}

func TestBufferedProcessor_Process_MapPanics(t *testing.T) {
	errs := make(ErrorChannel, 100)
	source := NewSequentialIntegerSource(10, 1*time.Millisecond)
	stream := mapPanicsStream(source)
	processor := NewBufferedProcessor(10, 1*time.Second)
	stream.Process(processor, errs)

	err := <-errs
	assert.NotNil(t, err)
	_, ok := err.(*MapError)
	assert.True(t, ok)
}
