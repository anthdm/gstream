package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
)

type Producer interface {
	Start() error
}

type HTTPProducer struct {
	listenAddr string
	producech  chan<- MessageToTopic
}

func NewHTTPProducer(listenAddr string, producech chan MessageToTopic) *HTTPProducer {
	return &HTTPProducer{
		listenAddr: listenAddr,
		producech:  producech,
	}
}

func (p *HTTPProducer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		path  = strings.TrimPrefix(r.URL.Path, "/")
		parts = strings.Split(path, "/")
	)

	// // commit
	// if r.Method == "GET" {
	// }

	if r.Method == "POST" {
		if len(parts) != 2 {
			fmt.Println("invalid action")
			return
		}
		topic := parts[1]
		p.producech <- MessageToTopic{
			Payload: []byte("we don't know yet"),
			Topic:   topic,
		}
		fmt.Println(topic)
	}
}

func (p *HTTPProducer) Start() error {
	log.Printf("HTTP transport started at port = %s", p.listenAddr)
	return http.ListenAndServe(p.listenAddr, p)
}
