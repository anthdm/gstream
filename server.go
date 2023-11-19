package main

import (
	"fmt"
	"log/slog"
)

type Message struct {
	Topic string
	Data  []byte
}

type Config struct {
	ListenAddr        string
	StoreProducerFunc StoreProducerFunc
}

type Server struct {
	*Config

	topics map[string]Storer

	consumers []Consumer
	producers []Producer
	producech chan Message
	quitch    chan struct{}
}

func NewServer(cfg *Config) (*Server, error) {
	producech := make(chan Message)
	return &Server{
		Config:    cfg,
		topics:    make(map[string]Storer),
		quitch:    make(chan struct{}),
		producech: producech,
		producers: []Producer{
			NewHTTPProducer(cfg.ListenAddr, producech),
		},
	}, nil
}

func (s *Server) Start() {
	// for _, consumer := range s.consumers {
	// 	if err := consumer.Start(); err != nil {
	// 		fmt.Println(err)
	// 	}
	// }
	for _, producer := range s.producers {
		go func(p Producer) {
			if err := p.Start(); err != nil {
				fmt.Println(err)
			}
		}(producer)
	}
	s.loop()
}

func (s *Server) loop() {
	for {
		select {
		case <-s.quitch:
			return
		case msg := <-s.producech:
			offset, err := s.publish(msg)
			if err != nil {
				slog.Error("failed to publish", err)
			} else {
				slog.Info("produced message", "offset", offset)
			}
		}
	}
}

func (s *Server) publish(msg Message) (int, error) {
	store := s.getStoreForTopic(msg.Topic)
	return store.Push(msg.Data)
}

func (s *Server) getStoreForTopic(topic string) Storer {
	if _, ok := s.topics[topic]; !ok {
		s.topics[topic] = s.StoreProducerFunc()
		slog.Info("created new topic", "topic", topic)
	}
	return s.topics[topic]
}
