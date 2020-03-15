package go_streams

import (
	"fmt"
)

func RecoverFilter(filterFunc FilterFunc, entry Entry, errs ErrorChannel) bool {
	defer func() {
		if p := recover(); p != nil {
			logger.Debug("Recovering from panic in filter step for entry: %+v", entry)
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
			logger.Debug("Recovering from panic in map step for entry: %+v", entry)
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

func RecoverMapWithContext(context map[string]interface{}, mapFunc MapWithContextFunc, entry Entry, errs ErrorChannel) interface{} {
	defer func() {
		if p := recover(); p != nil {
			logger.Debug("Recovering from panic in mapWithContext step for entry: %+v", entry)
			err, ok := p.(error)
			if ok {
				errs <- NewMapError(err)
			} else {
				errs <- NewMapError(fmt.Errorf("%v", p))
			}
		}
	}()

	return mapFunc(context, entry.Value)
}

func RecoverSinkSingle(sink Sink, entry Entry, errs ErrorChannel) error {
	defer func() {
		if p := recover(); p != nil {
			logger.Debug("Recovering from panic in sink (single) step for entry: %+v", entry)
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
			logger.Debug("Recovering from panic in sink (batch) step for entry: %+v", entry)
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
