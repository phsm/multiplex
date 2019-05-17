package multiplex

import (
	"fmt"
)

// ErrorContextDone is returned when the context is Done or Cancelled
type ErrorContextDone struct {
	err string
}

// NewErrorContextDone returns a new error object
func NewErrorContextDone(err string) ErrorContextDone {
	return ErrorContextDone{err}
}

func (e ErrorContextDone) Error() string {
	return e.err
}

// ErrorRead is returned when reading from io.Reader occured
type ErrorRead struct {
	err string
}

// NewErrorRead returns a new error object
func NewErrorRead(err string) ErrorRead {
	return ErrorRead{err}
}

func (e ErrorRead) Error() string {
	return e.err
}

// ErrorWrite is returned when reading from io.Reader occured
type ErrorWrite struct {
	err      string
	WriterID string
}

// NewErrorWrite returns a new error object
func NewErrorWrite(id string, err string) ErrorWrite {
	return ErrorWrite{WriterID: id, err: err}
}

func (e ErrorWrite) Error() string {
	return fmt.Sprintf("writer %s error: %s", e.WriterID, e.err)
}

// ErrorWriterSlow is returned when writer queue is full (writer isnt fas enough to process data stream)
type ErrorWriterSlow struct {
	err      string
	writerID string
}

// NewErrorWriterSlow returns a new error object
func NewErrorWriterSlow(writerID string) ErrorWriterSlow {
	return ErrorWriterSlow{writerID: writerID, err: fmt.Sprintf("Writer %s became slow", writerID)}
}

func (e ErrorWriterSlow) Error() string {
	return e.err
}
