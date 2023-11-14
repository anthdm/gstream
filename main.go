package main

import (
	"log"
)

func main() {
	cfg := &Config{
		ListenAddr: ":3000",
		StoreProducerFunc: func() Storer {
			return NewMemoryStore()
		},
	}
	s, err := NewServer(cfg)
	if err != nil {
		log.Fatal(err)
	}
	s.Start()
}
