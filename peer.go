package main

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/gorilla/websocket"
)

type Peer interface {
	Send([]byte) error
	GetPeerSubscription() <-chan string
}

type WSPeer struct {
	mu               sync.RWMutex
	conn             *websocket.Conn
	topics           []string
	peerTopicsAction chan<- PeerTopicsAction
	PeerSubscription chan string
}

func NewWSPeer(conn *websocket.Conn, peerTopicsAction chan PeerTopicsAction) *WSPeer {
	peerSubscription := make(chan string)
	topics := []string{}
	p := &WSPeer{
		conn:             conn,
		peerTopicsAction: peerTopicsAction,
		PeerSubscription: peerSubscription,
		topics:           topics,
	}

	go p.readLoop()

	return p
}

func (p *WSPeer) readLoop() {
	var msg WSMessage
	for {
		if err := p.conn.ReadJSON(&msg); err != nil {
			slog.Error("ws peer read error", "err", err)
			return
		}
		if err := p.handleMessage(msg); err != nil {
			slog.Error("ws peer handle msg error", "err", err)
			return
		}
	}
}

func (p *WSPeer) handleMessage(msg WSMessage) error {
	// validation of message
	if len(msg.Topics) == 0 {
		return fmt.Errorf("no topics specified")
	}
	p.peerTopicsAction <- PeerTopicsAction{
		Peer:   p,
		Action: msg.Action,
		Topics: msg.Topics,
	}
	if len(p.topics) > 0 {
		p.PeerSubscription <- "update"
	}
	p.topics = append(p.topics, msg.Topics...)
	fmt.Printf("handling message %+v \n", msg)
	return nil
}

func (p *WSPeer) Send(b []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.conn.WriteMessage(websocket.BinaryMessage, b)
}

func (p *WSPeer) GetPeerSubscription() <-chan string {
	return p.PeerSubscription
}
