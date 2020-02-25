package go_streams

import "fmt"

type EofError struct {
	source Source
}

func NewEofError(source Source) *EofError {
	return &EofError{source: source}
}

func (e *EofError) Error() string {
	return fmt.Sprintf("Source '%s' has stopped (EOF)", e.source.Name())
}

type SinkError struct {
	err error
}

func NewSinkError(err error) *SinkError {
	if err != nil {
		return &SinkError{err: err}
	}
	return nil
}

func (s *SinkError) Error() string {
	return s.err.Error()
}

type FilterError struct {
	err error
}

func NewFilterError(err error) *FilterError {
	if err != nil {
		return &FilterError{err: err}
	}
	return nil
}

func (f *FilterError) Error() string {
	return f.err.Error()
}

type MapError struct {
	err error
}

func NewMapError(err error) *MapError {
	if err != nil {
		return &MapError{err: err}
	}
	return nil
}

func (m *MapError) Error() string {
	return m.err.Error()
}

type SinkBatchError struct {
	Errors map[string]error
}

func NewSinkBatchError() *SinkBatchError {
	return &SinkBatchError{Errors: make(map[string]error)}
}

func (s *SinkBatchError) Error() string {
	return fmt.Sprintf("there are sink errors:\n%+v", s.Errors)
}

func (s *SinkBatchError) Add(key string, err error) {
	if err != nil {
		s.Errors[key] = err
	}
}

func (s *SinkBatchError) AsError() error {
	if len(s.Errors) == 0 {
		return nil
	}
	return s
}

type SameSourceError struct {
	source Source
}

func NewSameSourceError(source Source) *SameSourceError {
	return &SameSourceError{source: source}
}

func (sse *SameSourceError) Error() string {
	return fmt.Sprintf("Multiple streams with the same source ('%s') found. Sharing sources between different streams aren't allowed at the moment", sse.source.Name())
}
