package main

import "github.com/gorilla/websocket"

func foo() {
	websocket.DefaultDialer.Dial("ws:/oo", nil)
}

type Consumer interface {
	Start() error
}
