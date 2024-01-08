package main

import (
	"log"
)

func main() {
	cfg := &Config{
		HTTPListenAddr: ":3000",
		WSListenAddr:   ":4000",
		StorageProducerFunc: func() Storer {
			return NewMemoryStore()
		},
	}

	s, err := NewServer(cfg)

	if err != nil {
		log.Fatal(err)
	}

	s.Start()
}
