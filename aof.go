package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	aof := &Aof{file: file, rd: bufio.NewReader(file)}

	go func() {
		for {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()

	return aof, nil
}

func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.Marshal())
	return err
}

func (aof *Aof) Read(fn func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	aof.file.Seek(0, io.SeekStart)

	reader := NewResp(aof.file)
	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fn(value)
	}
	return nil
}
