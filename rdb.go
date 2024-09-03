package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type Rdb struct {
	file *os.File
	mu   sync.RWMutex
}

func NewRdb(path string) (*Rdb, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	rdb := &Rdb{file: file}

	return rdb, err
}

func (r *Rdb) Save() error {
	// save string map[string]string
	sets, err := r.saveSETS()
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.file.Write(sets)
	r.mu.Unlock()

	// save hash map[string]map[string]string

	return nil
}

func (r *Rdb) Load() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	data, err := io.ReadAll(r.file)
	if err != nil {
		return err
	}

	n, err := r.loadSETS(data)
	if err != nil {
		return err
	}
	data = data[n:]

	return nil
}

//
//func (r *Rdb) Close() error {
//
//}

func (r *Rdb) saveSETS() ([]byte, error) {
	var buffer bytes.Buffer
	SETsMu.RLock()
	defer SETsMu.RUnlock()

	setSize := int32(len(SETs))
	if err := binary.Write(&buffer, binary.LittleEndian, setSize); err != nil {
		return nil, err
	}

	for key, val := range SETs {
		keyLength := int32(len(key))
		if err := binary.Write(&buffer, binary.LittleEndian, keyLength); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, []byte(key)); err != nil {
			return nil, err
		}

		valLength := int32(len(val))
		if err := binary.Write(&buffer, binary.LittleEndian, valLength); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, []byte(val)); err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

func (r *Rdb) loadSETS(data []byte) (int32, error) {
	buffer := bytes.NewBuffer(data)
	n := int32(0)

	var setSize int32
	if err := binary.Read(buffer, binary.LittleEndian, &setSize); err != nil {
		return 0, err
	}
	n += 4

	for i := int32(0); i < setSize; i++ {
		var keyLength int32
		var valLength int32

		if err := binary.Read(buffer, binary.LittleEndian, &keyLength); err != nil {
			return 0, err
		}
		n += 4
		key := make([]byte, keyLength)
		if err := binary.Read(buffer, binary.LittleEndian, &key); err != nil {
			return 0, err
		}
		n += keyLength

		if err := binary.Read(buffer, binary.LittleEndian, &valLength); err != nil {
			return 0, err
		}
		n += 4
		val := make([]byte, valLength)
		if err := binary.Read(buffer, binary.LittleEndian, &val); err != nil {
			return 0, err
		}
		n += valLength

		SETsMu.Lock()
		SETs[string(key)] = string(val)
		SETsMu.Unlock()
	}
	return n, nil
}

//func (r *Rdb) saveHSETS() ([]byte, error) {}
//
//func (r *Rdb) LoadSETS() error {}
