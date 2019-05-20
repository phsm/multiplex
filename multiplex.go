// Package multiplex provides a thread-safe solution to copy data stream from one io.Reader to multiple io.Writers.
// io.Writers can be added and removed on the fly by using Write() and RemoveWriter functions.
// It tracks and reports slow writers (e.g. those not being able to handle all the stream data).
package multiplex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// count of "buckets" (records) in our improvised ring
	// the count was chosen without any reason, you can wiggle it around
	// also this value is used as queue channel capacity
	bufBucketsCount = 4096

	// the size of one "bucket" payload. The value has been chosen to fit
	// the largest network packet (jumbo frame)
	bufBucketSize = 9 * 1024

	// if free slots in the queue is less that this value, it is considered slow
	slowLessThan = 250

	// Errors channel buffer size
	errorsChanSize = 1000

	ringBufferSize = 8 * 1024 * 1024 // 8mbytes
)

// Structure of each bucket in the ring. We store our data chunks here
// with the length of data we read
type bucket struct {
	payload  []byte // our payload slice
	numBytes int    // amount of bytes in the payload
}

// this struct holds the queue for the client and the cancel function, which
// stops the routine. Cancel function is needed when you run RemoveWriter
// to ensure that routine stops
type writer struct {
	queue  chan int
	stats  writerStats
	cancel context.CancelFunc
}

func (w *writer) MarshalJSON() ([]byte, error) {
	m := make(map[string]*writerStats)
	m["stats"] = &w.stats
	return json.Marshal(m)
}

type writerStats struct {
	bytesTransmitted uint64
	queueLength      uint32
	isSlow           bool
	startedAt        time.Time
	statsMu          sync.RWMutex
}

func newWriterStats() writerStats {
	return writerStats{
		statsMu:   sync.RWMutex{},
		startedAt: time.Now(),
	}
}

func (w *writerStats) reportTransmittedBytes(bytes int) {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()
	w.bytesTransmitted += uint64(bytes)
}

func (w *writerStats) reportQueueLength(q uint32) {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()

	w.queueLength = q

	if (bufBucketsCount - q) < slowLessThan {
		w.isSlow = true
	} else {
		w.isSlow = false
	}
}

func (w *writerStats) Stats() (startedAt time.Time, bytesTransmitted uint64, queueLength uint32, isSlow bool) {
	w.statsMu.RLock()
	defer w.statsMu.RUnlock()

	startedAt = w.startedAt
	bytesTransmitted = w.bytesTransmitted
	queueLength = w.queueLength
	isSlow = w.isSlow
	return
}

func (w *writerStats) MarshalJSON() ([]byte, error) {
	startedAt, bytes, q, slow := w.Stats()

	w.statsMu.RLock()
	mapstat := make(map[string]interface{})
	mapstat["bytes_transferred"] = bytes
	mapstat["queue_length"] = q
	mapstat["is_slow"] = slow
	mapstat["started_at"] = startedAt
	w.statsMu.RUnlock()

	return json.Marshal(mapstat)
}

// Multiplex initializes with an io.ReadCloser, copies the stream from the reader to many io.Writers
// How to use it:
// Create an instance with NewMultiplex
// As an arguments pass a Context, and io.Reader instance
// Add or remove some io.Writer`s with Multiplex.AddWriter or Multiplex.RemoveWriter
type Multiplex struct {
	ctx    context.Context      // everything stops if this context is done/cancelled
	cancel context.CancelFunc   // run it to cancel everything
	reader io.Reader            // the reader we read data from
	buf    [ringBufferSize]byte // this array (its not a slice!) hold buckets with data

	cond           *sync.Cond
	readerPosition uint32

	// This waitgroup is used when the reader is stopping by some reason (usually due to EOF).
	// It is used to wait all writers to finish writing.
	writersWg sync.WaitGroup

	writersCount   uint32
	writersCountMu sync.RWMutex

	// if true, stop everything when no writers left.
	stopWhenNoWritersLeft bool

	// Errors channel. All errors are copied here, so you can read errors from outside.
	// It is handy when you have non-fatal errors such as "writer became slow".
	Errors chan error

	// Callback functions
	OnWriterStopCallback    func()
	OnServeFinishedCallback func()

	startedAt      time.Time
	marshallJSONMu sync.Mutex
}

// MarshalJSON is used to jsonify Multiplex object information
func (m *Multiplex) MarshalJSON() ([]byte, error) {
	m.marshallJSONMu.Lock()
	defer m.marshallJSONMu.Unlock()
	retval := make(map[string]interface{})
	retval["started_at"] = m.startedAt
	return json.Marshal(retval)
}

// NewMultiplex initializes new Multiplex structure with io.Reader instance
// The first argument is a context. If you dont need it, just use context.Background() as
// a first argument.
func NewMultiplex(ctx context.Context, r io.Reader, stopWhenNoWritersLeft bool) *Multiplex {
	m := new(Multiplex)
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.reader = r
	m.stopWhenNoWritersLeft = stopWhenNoWritersLeft

	m.cond = sync.NewCond(new(sync.Mutex))

	m.writersCountMu = sync.RWMutex{}

	m.marshallJSONMu = sync.Mutex{}

	m.Errors = make(chan error, errorsChanSize)

	return m
}

// Serve launches the reading loop. It reads data from io.Reader to buckets and puts
// payload buckets numbers into writers channels (queues)
func (m *Multiplex) Serve() error {
	var err error
	var numBytesRead int
	var readerPosition uint32

	// The whole thing stops if this funcion exits
	// as there is no point in running writers if this function isn't running.
	// defer m.cancel()

	// Now start time
	m.marshallJSONMu.Lock()
	m.startedAt = time.Now()
	m.marshallJSONMu.Unlock()

	// Our main loop. Here we continuously read data from the reader and
	// notify the writers
	for {
		select {
		case <-m.ctx.Done():
			err := NewErrorContextDone("reader context is done")
			m.reportError(err)
			return err
		default:
			// continue
		}

		readerPosition = atomic.LoadUint32(&m.readerPosition)

		// read data to the payload of the current bucket
		// also setting the number of bytes read.
		numBytesRead, err = m.reader.Read(m.buf[readerPosition:])
		if err != nil && err != io.EOF {
			// Exit if reader error occured and it is not EOF
			return err
		}

		if readerPosition+uint32(numBytesRead) == uint32(ringBufferSize) {
			atomic.StoreUint32(&m.readerPosition, 0)
		} else if m.readerPosition < ringBufferSize {
			atomic.AddUint32(&m.readerPosition, uint32(numBytesRead))
		} else {
			fmt.Println("Seems that we read more than the buffer size. Strange.")
		}

		m.cond.L.Lock()
		m.cond.Broadcast()
		m.cond.L.Unlock()
	}
}

// AddWriter thread-safely subscribes io.Writer to receive a copy of the stream.
// string id is needed and is used in:
// - slow writer error
// - to distinguish which writer do you want to remove using RemoveWriter function
func (m *Multiplex) Write(id string, w io.Writer) error {
	defer m.OnWriterStopCallback()
	// the writer struct with the queue and the cancel function
	ws := new(writer)
	ws.stats = newWriterStats()

	var writerPosition uint32
	var readerPosition uint32
	var numBytes int
	var err error

	var ctx context.Context
	ctx, ws.cancel = context.WithCancel(m.ctx)

	m.writersWg.Add(1)
	defer m.writersWg.Done()

	m.writersCountMu.Lock()
	m.writersCount++
	m.writersCountMu.Unlock()

	// If some error happens, we need to remove this writer from the queuemap in any case
	defer m.RemoveWriter(id)

	for {
		// Here we selecting either the parent or child context is
		// or bucket number is received. Else we just reading the channel
		select {
		case <-ctx.Done():
			err := NewErrorContextDone(fmt.Sprintf("writer %s: context is done", id))
			m.reportError(err)
			return err
		default:
			// continue
		}

		readerPosition = atomic.LoadUint32(&m.readerPosition)

		for writerPosition == readerPosition {
			ws.stats.reportQueueLength(0)
			m.cond.L.Lock()
			m.cond.Wait()
			m.cond.L.Unlock()
			readerPosition = atomic.LoadUint32(&m.readerPosition)
		}

		if writerPosition < readerPosition {
			ws.stats.reportQueueLength(readerPosition - writerPosition)
			numBytes, err = w.Write(m.buf[writerPosition:readerPosition])
			if err != nil {
				errWrite := NewErrorWrite(id, err.Error())
				m.reportError(errWrite)
				return errWrite
			}
			writerPosition = writerPosition + uint32(numBytes)
		} else {
			numBytes, err = w.Write(m.buf[writerPosition:])
			if err != nil {
				errWrite := NewErrorWrite(id, err.Error())
				m.reportError(errWrite)
				return errWrite
			}
			if writerPosition+uint32(numBytes) == uint32(ringBufferSize) {
				writerPosition = 0
			} else {
				writerPosition = writerPosition + uint32(numBytes)
			}
		}
		ws.stats.reportTransmittedBytes(numBytes)
	}
}

// RemoveWriter thread-safely removes an io.Writer from receiving data.
func (m *Multiplex) RemoveWriter(id string) {

	if m.stopWhenNoWritersLeft && m.writersCount == 0 {
		m.cancel()
	}
}

// WritersCount returns current writers count
func (m *Multiplex) WritersCount() uint32 {
	m.writersCountMu.RLock()
	defer m.writersCountMu.RUnlock()
	return m.writersCount
}

func (m *Multiplex) reportError(err error) {
	free := cap(m.Errors) - len(m.Errors)

	if free > 0 {
		m.Errors <- err
	}
}
