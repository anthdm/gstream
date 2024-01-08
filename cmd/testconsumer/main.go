package main

import (
	"encoding/json"
	"log"
	"log/slog"

	"github.com/gorilla/websocket"
)

type WSMessage struct {
	Action string   `json:"action"`
	Topics []string `json:"topics"`
}

type Message struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	Offset  int    `json:"offset"`
}

func main() {
	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:4000", nil)
	if err != nil {
		log.Fatal(err)
	}

	msg := WSMessage{
		Action: "subscribe",
		Topics: []string{"topic_1", "topic_2"},
	}

	// b, err := json.Marshal(msg)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	if err := conn.WriteJSON(msg); err != nil {
		log.Fatal(err)
	}

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Fatal(err)
		}

		var m Message
		err = json.Unmarshal(msg, &m)
		if err != nil {
			slog.Error("error unmarshaling message", "err", err)
			continue
		}

		slog.Info("received message from ", "topic", m.Topic, "message", string(m.Payload), "offset", m.Offset)
	}
}
