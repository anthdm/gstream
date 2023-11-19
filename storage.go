package main

import (
	"fmt"
	"sync"
)

type StoreProducerFunc func() Storer

type Storer interface {
	Push([]byte) (int, error)
	Get(int) ([]byte, error)
}

type MemoryStore struct {
	mu   sync.RWMutex
	data [][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make([][]byte, 0),
	}
}

func (s *MemoryStore) Push(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = append(s.data, b)
	return len(s.data) - 1, nil
}

func (s *MemoryStore) Get(offset int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset < 0 {
		return nil, fmt.Errorf("offset cannot be smaller than 0")
	}
	if len(s.data)-1 < offset {
		return nil, fmt.Errorf("offset (%d) too high", offset)
	}
	return s.data[offset], nil
}
