package go_streams

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestPollingSource_Start(t *testing.T) {
	source := NewPollingSource(200*time.Millisecond, func(latestCommit string) (entries []Entry, e error) {
		temp, _ := strconv.Atoi(latestCommit)
		var out []Entry
		for i := temp + 1; i < temp+10; i++ {
			out = append(out, Entry{
				Key:   fmt.Sprintf("%d", i),
				Value: i,
			})
		}
		return out, nil
	})

	processor := NewDirectProcessor()
	sink := NewArraySink()
	errs := make(ErrorChannel, 1)

	stream := NewStream(source).
		Filter(onlyPrimes).
		Sink(sink)

	go func() {
		time.Sleep(1 * time.Second)
		err := source.Stop()
		assert.Nil(t, err)
	}()

	stream.Process(processor, errs)

	assert.NotEmpty(t, sink.Array())
	assert.EqualValues(t, 11, len(sink.Array()))
	assert.EqualValues(t, []interface{}{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31}, sink.Array())
}
