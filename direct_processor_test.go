package go_streams

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDirectProcessor_Process_HappyCase(t *testing.T) {
	errs := make(ErrorChannel, 100)
	source := NewSequentialIntegerSource(10, 1*time.Millisecond)
	sink := NewArraySink()
	stream := addOneFilterOddsStream(source, sink)
	processor := NewDirectProcessor()
	stream.Process(processor, errs)

	assert.NotEmpty(t, sink.Array())
	assert.EqualValues(t, []interface{}{2, 4, 6, 8, 10}, sink.Array())
}

func TestDirectProcessor_Process_FilterPanics(t *testing.T) {
	errs := make(ErrorChannel, 100)
	source := NewSequentialIntegerSource(10, 1*time.Millisecond)
	stream := filterPanicsStream(source)
	processor := NewDirectProcessor()
	stream.Process(processor, errs)

	err := <-errs
	assert.NotNil(t, err)
	_, ok := err.(*FilterError)
	assert.True(t, ok)
}

func TestDirectProcessor_Process_MapPanics(t *testing.T) {
	errs := make(ErrorChannel, 100)
	source := NewSequentialIntegerSource(10, 1*time.Millisecond)
	stream := mapPanicsStream(source)
	processor := NewDirectProcessor()
	stream.Process(processor, errs)

	err := <-errs
	assert.NotNil(t, err)
	_, ok := err.(*MapError)
	assert.True(t, ok)
}

func addOneFilterOddsStream(source Source, sink Sink) Stream {
	return NewStream(source).Map(func(entry interface{}) interface{} {
		return entry.(int) + 1

	}).Filter(func(entry interface{}) bool {
		return entry.(int)%2 == 0

	}).Sink(sink)
}

func mapPanicsStream(source Source) Stream {
	return NewStream(source).Map(func(entry interface{}) interface{} {
		panic("demo")
	}).Filter(func(entry interface{}) bool {
		return entry.(int)%2 == 0
	}).Sink(NewConsoleSink())
}

func filterPanicsStream(source Source) Stream {
	return NewStream(source).Map(func(entry interface{}) interface{} {
		return entry.(int) + 1
	}).Filter(func(entry interface{}) bool {
		panic("demo")
	}).Sink(NewConsoleSink())
}
