package main

import (
	"context"
	"log"
	"net"
	"net/http"

	"github.com/phsm/multiplex"
)

var mplx *multiplex.Multiplex

// This function handles every HTTP client requests
func handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Pragma", "no-cache")

	// Use Write function to copy the stream to the response body.
	// Since each writer should have unique identifier, we use remote IP + port as an id
	err := mplx.Write(r.RemoteAddr, w)
	if err != nil {
		// Since multiplex provides different types of errors, it is easily to
		// determine which error type you receive (instead of comparing strings)
		// Here we check if the error we receive is of ErrorWrite, which usually
		// indicates remote client disconnect.
		if e, ok := err.(multiplex.ErrorWrite); ok {
			log.Printf("client %s disconnected", e.WriterID)
		} else {
			log.Fatalf("Some strange write error! %v", e)
		}
	}
}

func main() {
	// We assume that our streamer receives the videostream from origin via multicast
	// to the group 224.10.20.30:1234
	addr, err := net.ResolveUDPAddr("udp", "224.10.20.30:1234")
	if err != nil {
		log.Fatal("can't resolve udp address")
	}

	// opening listening multicast socket
	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		log.Fatal("can't listen")
	}

	// use our connection as a reader
	mplx = multiplex.NewMultiplex(context.Background(), conn, false)
	go mplx.Serve()

	// register incoming HTTP requests handler
	http.HandleFunc("/", handler)

	// now multiple clients can receive a copy of the stream
	// from http://<server_ip>:8080/
	log.Fatal(http.ListenAndServe(":8080", nil))
}
