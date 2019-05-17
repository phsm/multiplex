package multiplex_test

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/phsm/multiplex"
	merrors "github.com/phsm/multiplex/errors"
)

const (
	tmpfileOrigin = "/tmp/golang_test_origin"
)

var (
	m           *multiplex.Multiplex
	tmpfileHash string
	ctx         context.Context
)

func setupTestCase(t *testing.T) (*os.File, func(t *testing.T)) {
	t.Log("Setting up test case")
	f, err := os.Open("/dev/urandom")
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	w, err := os.OpenFile(tmpfileOrigin, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	// Read 10Mb of random
	var amount int64
	amount = 10 * 1024 * 1024
	_, err = io.CopyN(w, f, amount)
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	f.Close()
	w.Close()

	rMd5, err := os.Open(tmpfileOrigin)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	h := md5.New()
	_, err = io.Copy(h, rMd5)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	// Now open it as our reader
	r, err := os.Open(tmpfileOrigin)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	tmpfileHash = fmt.Sprintf("%x", h.Sum(nil))
	t.Logf("Origin file hash is %s", tmpfileHash)

	return r, func(t *testing.T) {
		t.Log("Tearing down test case")
		r.Close()
		if err := os.Remove(tmpfileOrigin); err != nil {
			t.Fatalf("%v\n", err)
		}
	}
}

type fakeErrorReader int

func (fakeErrorReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("mocked read error")
}

type fakeErrorWriter int

func (fakeErrorWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("mocked write error")
}

type fakeSlowWriter int

func (fakeSlowWriter) Write(p []byte) (n int, err error) {
	return 1, nil
}

func TestServe(t *testing.T) {
	r, teardown := setupTestCase(t)
	defer teardown(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hash1 := md5.New()
	hash2 := md5.New()

	m = multiplex.NewMultiplex(ctx, r, false)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		m.Write("w1", hash1)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		m.Write("w2", hash2)
		wg.Done()
	}()

	go func() {
		select {
		case <-ctx.Done():
			return
		case err := <-m.Errors:
			log.Printf("errors channel: %v", err)
		}
	}()

	time.Sleep(10 * time.Millisecond)

	err := m.Serve()
	if err == io.EOF {
		t.Log("EOF received as intended")
		wg.Wait()
		hash1Sum := fmt.Sprintf("%x", hash1.Sum(nil))
		hash2Sum := fmt.Sprintf("%x", hash2.Sum(nil))
		t.Logf("First hash is %s, second hash is %s", hash1Sum, hash2Sum)
		if (hash1Sum != tmpfileHash) || (hash2Sum != tmpfileHash) {
			t.Errorf("Hash of outgoing streams mismatch with the origin. The replicated data is not identical to the origin.")
		}

		if _, err := m.MarshalJSON(); err != nil {
			t.Errorf("Error marshalling to JSON: %v", err)
		}
		return
	}

	t.Errorf("Expected to receive EOF at the end but got: %v. Context cancel function fired?", err)
}

func TestContextExit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	f, err := os.Open("/dev/zero")
	if err != nil {
		log.Fatal("Can't open /dev/zero for reading")
	}

	m := multiplex.NewMultiplex(ctx, f, false)
	err = m.Serve()

	_, ok := err.(merrors.ErrorContextDone)
	if !ok {
		log.Fatal("Couldn't assert error to merrors.ErrorContextDone")
	}
	f.Close()

	cancel()
}

func TestReaderError(t *testing.T) {
	var r fakeErrorReader
	m := multiplex.NewMultiplex(context.Background(), r, false)
	err := m.Serve()
	if err == nil || err.Error() != "mocked read error" {
		log.Fatal("didn't receive test error")
	}
}

func TestWriterError(t *testing.T) {
	var w fakeErrorWriter

	f, err := os.Open("/dev/zero")
	if err != nil {
		log.Fatal("Can't open /dev/zero for reading")
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	m := multiplex.NewMultiplex(ctx, f, false)

	go m.Serve()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.Write("w1", w)
		if err == nil {
			log.Fatal("didn't receive an error during write")
		}
		_, ok := err.(merrors.ErrorWrite)
		if !ok {
			log.Fatal("didn't receive error of ErrorWrite type")
		}
	}()

	wg.Wait()
	cancel()
}

func TestRemoveWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	f, err := os.Open("/dev/zero")
	if err != nil {
		log.Fatal("Can't open /dev/zero for reading")
	}
	defer f.Close()

	m := multiplex.NewMultiplex(ctx, f, false)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.Write("w1", ioutil.Discard)

		if err == nil {
			t.Fatal("Unexpected nil value for error")
		}

		_, ok := err.(merrors.ErrorContextDone)
		if !ok {
			t.Fatalf("Received wrong error type: %v", err)
		}
	}()

	go m.Serve()
	time.Sleep(500 * time.Millisecond)
	m.RemoveWriter("w1")
	wg.Wait()
	cancel()
}

func TestSlowWriter(t *testing.T) {
	var w fakeSlowWriter

	f, err := os.Open("/dev/zero")
	if err != nil {
		log.Fatal("Can't open /dev/zero for reading")
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	m := multiplex.NewMultiplex(ctx, f, false)

	go m.Serve()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := m.Write("w1", w)
		defer wg.Done()
		if err == nil {
			t.Fatal("Unexpected nil value for error")
		}

		_, ok := err.(merrors.ErrorContextDone)
		if !ok {
			t.Fatalf("Received wrong error type: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			return
		case err := <-m.Errors:
			if err == nil {
				t.Fatal("Unexpected nil value for error")
			}

			_, ok := err.(merrors.ErrorWriterSlow)
			if !ok {
				t.Fatalf("Received wrong error type: %v", err)
			}
		}
	}()

	wg.Wait()
	cancel()
}

// ExampleWrite shows the Write function in action. We open "/etc/hosts" file for reading
// (i.e. giving us an io.Reader). Then we open "/dev/stdout" pseudo-device as io.Writer.
// Single writer w1 writes the stream from reader to /dev/stdout
func ExampleMultiplex_Write() {
	f, err := os.Open("/etc/hosts")
	if err != nil {
		log.Fatalf("Error while loading a file: %v", err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)

	m := multiplex.NewMultiplex(ctx, f, false)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		o, err := os.OpenFile("/dev/stdout", os.O_WRONLY, 0755)
		if err != nil {
			log.Fatalf("/dev/stdout open error: %v", err)
		}
		defer o.Close()

		wg.Done()
		err = m.Write("w1", o)
		if err != nil && err != io.EOF {
			log.Fatalf("Error while writing: %v", err)
		}
	}()

	wg.Wait()
	m.Serve()

	cancel()
}
