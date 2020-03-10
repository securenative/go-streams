package go_streams

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCallbackSink_Single(t *testing.T) {
	expected := Entry{
		Key:      "demo key",
		Value:    "demo value",
		Filtered: false,
	}

	var array []Entry

	sink := NewCallbackSink(func(entries ...Entry) error {
		array = append(array, entries...)
		return nil
	})

	err := sink.Single(expected)
	assert.Nil(t, err)

	assert.NotEmpty(t, array)
	assert.EqualValues(t, 1, len(array))
	assert.EqualValues(t, expected.Key, array[0].Key)
	assert.EqualValues(t, expected.Value, array[0].Value)
}

func TestCallbackSink_Batch(t *testing.T) {
	expected := []Entry{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
		{Key: "key4", Value: "value4"},
	}

	var array []Entry

	sink := NewCallbackSink(func(entries ...Entry) error {
		array = append(array, entries...)
		return nil
	})

	err := sink.Batch(expected...)
	assert.Nil(t, err)

	assert.NotEmpty(t, array)
	assert.EqualValues(t, len(expected), len(array))
	for idx := range array {
		assert.EqualValues(t, expected[idx].Key, array[idx].Key)
		assert.EqualValues(t, expected[idx].Value, array[idx].Value)
	}
}
