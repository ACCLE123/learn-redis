package main

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Config struct {
	file       *os.File
	mu         sync.RWMutex
	AppendOnly bool
	Save       []SaveConfig
}
type SaveConfig struct {
	Seconds int
	Changes int
}

var instance *Config
var once sync.Once
var initErr error

func NewConfig(path string) (*Config, error) {
	once.Do(func() {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			initErr = err
			return
		}
		instance = &Config{file: file}
	})
	if initErr != nil {
		return nil, initErr
	}
	return instance, nil
}

func (r *Config) ReadConfig() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	scanner := bufio.NewScanner(r.file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		switch parts[0] {
		case "appendonly":
			r.AppendOnly = parts[1] == "yes"
		case "save":
			if len(parts) == 3 {
				seconds, err1 := strconv.Atoi(parts[1])
				changes, err2 := strconv.Atoi(parts[2])
				if err1 == nil && err2 == nil {
					r.Save = append(r.Save, SaveConfig{Seconds: seconds, Changes: changes})
				}
			}
		}

		if err := scanner.Err(); err != nil {
			return err
		}
	}
	_, err := r.file.Seek(0, 0)
	return err
}
