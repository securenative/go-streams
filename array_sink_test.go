package go_streams

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArraySink_Single(t *testing.T) {
	expected := Entry{
		Key:      "demo key",
		Value:    "demo value",
		Filtered: false,
	}

	sink := NewArraySink()
	err := sink.Single(expected)
	assert.Nil(t, err)

	assert.NotEmpty(t, sink.Array())
	assert.EqualValues(t, 1, len(sink.Array()))
	assert.EqualValues(t, expected.Value, sink.array[0])
}

func TestArraySink_Batch(t *testing.T) {
	expected := []Entry{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
		{Key: "key4", Value: "value4"},
	}

	sink := NewArraySink()
	err := sink.Batch(expected...)
	assert.Nil(t, err)

	assert.NotEmpty(t, sink.Array())
	assert.EqualValues(t, len(expected), len(sink.Array()))
	for idx := range sink.Array() {
		assert.EqualValues(t, expected[idx].Value, sink.array[idx])
	}
}
