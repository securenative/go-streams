package go_streams

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecoverFilter(t *testing.T) {
	errs := make(ErrorChannel, 1)

	RecoverFilter(func(entry interface{}) bool {
		panic("filter error")
	}, Entry{}, errs)
	err := <-errs

	assert.NotNil(t, err)
	assert.EqualValues(t, "filter error", err.Error())
}

func TestRecoverMap(t *testing.T) {
	errs := make(ErrorChannel, 1)

	RecoverMap(func(entry interface{}) interface{} {
		panic("map error")
	}, Entry{}, errs)
	err := <-errs

	assert.NotNil(t, err)
	assert.EqualValues(t, "map error", err.Error())
}

func TestRecoverSinkSingle(t *testing.T) {
	errs := make(ErrorChannel, 1)

	_ = RecoverSinkSingle(&panicSink{}, Entry{}, errs)
	err := <-errs

	assert.NotNil(t, err)
	assert.EqualValues(t, "single error", err.Error())
}

func TestRecoverSinkBatch(t *testing.T) {
	errs := make(ErrorChannel, 1)

	_ = RecoverSinkBatch(&panicSink{}, []Entry{}, errs)
	err := <-errs

	assert.NotNil(t, err)
	assert.EqualValues(t, "batch error", err.Error())
}

type panicSink struct {
}

func (s *panicSink) Single(entry Entry) error {
	panic("single error")
}

func (s *panicSink) Batch(entry ...Entry) error {
	panic("batch error")
}

func (this *panicSink) Ping() error {
	return nil
}
