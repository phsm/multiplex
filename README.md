# Multiplex [![GoDoc](https://godoc.org/github.com/phsm/multiplex?status.svg)](https://godoc.org/github.com/phsm/multiplex)

## What is Multiplex?
Multiplex is a thread-safe Golang library which allows reading data from single io.Reader to multiple io.Writers. The writers can be added or removed on the fly.

## Features
- thread-safe
- add/remove writers on the fly
- slow writer awareness (and report): single slow writer won't stuck them all
- Json statistics export
- source and destination agnostic: the library works with everything implementing io.Reader or io.Writer interfaces

## Possible use-cases
Restreaming live video from single stream to multiple clients (e.g. via Golang's built-in HTTP server)