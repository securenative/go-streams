package go_streams

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAllMapSinks_Single(t *testing.T) {
	first := Entry{
		Key:      "key1",
		Value:    "value1",
		Filtered: false,
	}

	second := Entry{
		Key:      "key1",
		Value:    "value2",
		Filtered: false,
	}

	replacingSink := NewReplacingMapSink()
	ignoreSink := NewIgnoringMapSink()
	multiSink := NewMultiMapSink()

	// Replacing Map Sink:
	assert.Nil(t, replacingSink.Single(first))
	assert.Nil(t, replacingSink.Single(second))
	assert.EqualValues(t, 1, len(replacingSink.Map()))
	assert.EqualValues(t, second.Value, replacingSink.Map()[first.Key])

	// Ignoring Map Sink:
	assert.Nil(t, ignoreSink.Single(first))
	assert.Nil(t, ignoreSink.Single(second))
	assert.EqualValues(t, 1, len(ignoreSink.Map()))
	assert.EqualValues(t, first.Value, ignoreSink.Map()[first.Key])

	// Multi Map Sink:
	assert.Nil(t, multiSink.Single(first))
	assert.Nil(t, multiSink.Single(second))
	assert.EqualValues(t, 1, len(multiSink.Map()))
	assert.EqualValues(t, 2, len(multiSink.Map()[first.Key]))
	assert.EqualValues(t, []interface{}{first.Value, second.Value}, multiSink.Map()[first.Key])
}

func TestAllMapSinks_Batch(t *testing.T) {
	first := Entry{
		Key:      "key1",
		Value:    "value1",
		Filtered: false,
	}

	second := Entry{
		Key:      "key1",
		Value:    "value2",
		Filtered: false,
	}

	replacingSink := NewReplacingMapSink()
	ignoreSink := NewIgnoringMapSink()
	multiSink := NewMultiMapSink()

	// Replacing Map Sink:
	assert.Nil(t, replacingSink.Batch(first, second))
	assert.EqualValues(t, 1, len(replacingSink.Map()))
	assert.EqualValues(t, second.Value, replacingSink.Map()[first.Key])

	// Ignoring Map Sink:
	assert.Nil(t, ignoreSink.Batch(first, second))
	assert.EqualValues(t, 1, len(ignoreSink.Map()))
	assert.EqualValues(t, first.Value, ignoreSink.Map()[first.Key])

	// Multi Map Sink:
	assert.Nil(t, multiSink.Batch(first, second))
	assert.EqualValues(t, 1, len(multiSink.Map()))
	assert.EqualValues(t, 2, len(multiSink.Map()[first.Key]))
	assert.EqualValues(t, []interface{}{first.Value, second.Value}, multiSink.Map()[first.Key])
}
