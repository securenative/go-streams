package go_streams

import "fmt"

func RecoverFilter(filterFunc FilterFunc, entry Entry, errs ErrorChannel) bool {
	defer func() {
		if p := recover(); p != nil {
			err, ok := p.(error)
			if ok {
				errs <- NewFilterError(err)
			} else {
				errs <- NewFilterError(fmt.Errorf("%v", p))
			}
		}
	}()

	return filterFunc(entry.Value)
}

func RecoverMap(mapFunc MapFunc, entry Entry, errs ErrorChannel) interface{} {
	defer func() {
		if p := recover(); p != nil {
			err, ok := p.(error)
			if ok {
				errs <- NewMapError(err)
			} else {
				errs <- NewMapError(fmt.Errorf("%v", p))
			}
		}
	}()

	return mapFunc(entry.Value)
}

func RecoverSinkSingle(sink Sink, entry Entry, errs ErrorChannel) error {
	defer func() {
		if p := recover(); p != nil {
			err, ok := p.(error)
			if ok {
				errs <- NewSinkError(err)
			} else {
				errs <- NewSinkError(fmt.Errorf("%v", p))
			}
		}
	}()

	return sink.Single(entry)
}

func RecoverSinkBatch(sink Sink, entry []Entry, errs ErrorChannel) error {
	defer func() {
		if p := recover(); p != nil {
			err, ok := p.(error)
			if ok {
				errs <- NewSinkError(err)
			} else {
				errs <- NewSinkError(fmt.Errorf("%v", p))
			}
		}
	}()

	return sink.Batch(entry...)
}
