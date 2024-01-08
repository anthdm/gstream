package main

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

type Consumer interface {
	Start() error
}

type WSConsumer struct {
	ListenAddr       string
	peerch           chan<- Peer
	peerTopicsAction chan PeerTopicsAction
}

func NewWSConsumer(listenAddr string, peerch chan Peer, peerTopicsAction chan PeerTopicsAction) *WSConsumer {
	return &WSConsumer{
		ListenAddr:       listenAddr,
		peerch:           peerch,
		peerTopicsAction: peerTopicsAction,
	}
}

func (wc *WSConsumer) Start() error {
	slog.Info("websocket consumer started", "port", wc.ListenAddr)
	return http.ListenAndServe(wc.ListenAddr, wc)
}

func (wc *WSConsumer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	wc.peerch <- NewWSPeer(conn, wc.peerTopicsAction)
}

type WSMessage struct {
	Action string   `json:"action"`
	Topics []string `json:"topics"`
}
