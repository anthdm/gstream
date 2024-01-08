package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"sync"
)

type MessageToTopic struct {
	Topic   string
	Payload []byte
}

type MessageToPeer struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	Offset  int    `json:"offset"`
}

type PeerTopicsAction struct {
	Peer   Peer
	Action string
	Topics []string
}

type Config struct {
	HTTPListenAddr      string
	WSListenAddr        string
	StorageProducerFunc StoreProducerFunc
}

type PeerTopicDetails struct {
	Offset int
	Topic  string
}

type Server struct {
	*Config
	mu               sync.RWMutex
	topics           map[string]Storer
	peers            map[Peer]bool
	consumers        []Consumer
	producers        []Producer
	peerToTopics     map[Peer][]*PeerTopicDetails
	producech        chan MessageToTopic
	peerch           chan Peer
	peerTopicsAction chan PeerTopicsAction
	quitch           chan struct{}
}

func NewServer(cfg *Config) (*Server, error) {
	producech := make(chan MessageToTopic)
	peerch := make(chan Peer)
	peerTopicsAction := make(chan PeerTopicsAction)

	return &Server{
		Config: cfg,
		topics: make(map[string]Storer),
		producers: []Producer{
			NewHTTPProducer(cfg.HTTPListenAddr, producech),
		},
		consumers: []Consumer{
			NewWSConsumer(cfg.WSListenAddr, peerch, peerTopicsAction),
		},
		peers:            make(map[Peer]bool),
		peerToTopics:     make(map[Peer][]*PeerTopicDetails),
		producech:        producech,
		peerch:           peerch,
		peerTopicsAction: peerTopicsAction,
		quitch:           make(chan struct{}),
	}, nil
}

func (s *Server) Start() {
	for _, consumer := range s.consumers {
		go func(c Consumer) {
			if err := c.Start(); err != nil {
				fmt.Println(err)
			}
		}(consumer)
	}

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
		case peer := <-s.peerch:
			slog.Info("added new connection", "peer", peer)
			s.peers[peer] = true
		case msg := <-s.producech:
			fmt.Println("produced -> ", msg)
			offset, err := s.publish(msg)
			if err != nil {
				log.Fatalf("Error occurred: %s", err)
			} else {
				fmt.Printf("Produced message in topic %s with offset %d", msg.Topic, offset)
			}
		case peerTopicsAction := <-s.peerTopicsAction:
			slog.Info("peer topics update", "peer", peerTopicsAction.Peer, "topics", peerTopicsAction.Topics)
			// update peer topics
			s.handlePeerTopicsUpdate(peerTopicsAction.Peer, peerTopicsAction.Action, peerTopicsAction.Topics)
		}

	}
}

func (s *Server) handlePeerTopicsUpdate(peer Peer, action string, topics []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch action {
	case "subscribe":
		fmt.Println("Inside Handle Peer Topics Update", action, topics)
		for _, topic := range topics {
			if _, ok := s.topics[topic]; !ok {
				msg, err := json.Marshal(MessageToPeer{
					Topic:   topic,
					Offset:  0,
					Payload: []byte("topic not found"),
				})
				if err != nil {
					slog.Error(fmt.Sprintf("Error marshalling message to peer %s", err))
				}
				if err := peer.Send(msg); err != nil {
					if err == io.EOF {
						delete(s.peers, peer)
						delete(s.peerToTopics, peer)
					} else {
						log.Println(err)
					}
				}
			} else {
				s.peerToTopics[peer] = append(s.peerToTopics[peer], &PeerTopicDetails{
					Offset: 0,
					Topic:  topic,
				})
				slog.Info("Peer subscribed to topics", "peer", peer, "topics", topic, "peerTopicdetails", s.peerToTopics[peer][len(s.peerToTopics[peer])-1])
				msg, err := json.Marshal(MessageToPeer{
					Topic:   topic,
					Offset:  0,
					Payload: []byte("subscribed to " + topic),
				})
				if err != nil {
					slog.Error(fmt.Sprintf("Error marshalling message to peer %s", err))
				}
				if err := peer.Send(msg); err != nil {
					if err == io.EOF {
						delete(s.peers, peer)
						delete(s.peerToTopics, peer)
					} else {
						log.Println(err)
					}
				}
			}
		}
	case "unsubscribe":
		for _, topic := range topics {
			if _, ok := s.topics[topic]; !ok {
				msg, err := json.Marshal(MessageToPeer{
					Topic:   topic,
					Offset:  0,
					Payload: []byte("topic not found"),
				})
				if err != nil {
					slog.Error(fmt.Sprintf("Error marshalling message to peer %s", err))
				}
				if err := peer.Send(msg); err != nil {
					if err == io.EOF {
						delete(s.peers, peer)
						delete(s.peerToTopics, peer)
					} else {
						log.Println(err)
					}
				}
			} else {
				for idx, topicDetails := range s.peerToTopics[peer] {
					if topicDetails.Topic == topic {
						s.peerToTopics[peer] = append(s.peerToTopics[peer][:idx], s.peerToTopics[peer][idx+1:]...)
						slog.Info("Peer unsubscribed from topics", "peer", peer, "topics", topic, "peersTopics", s.peerToTopics[peer])
						msg, err := json.Marshal(MessageToPeer{
							Topic:   topic,
							Offset:  0,
							Payload: []byte("unsubscribed from " + topic),
						})
						if err != nil {
							slog.Error(fmt.Sprintf("Error marshalling message to peer %s", err))
						}
						if err := peer.Send(msg); err != nil {
							if err == io.EOF {
								delete(s.peers, peer)
								delete(s.peerToTopics, peer)
							} else {
								log.Println(err)
							}
						}
						break
					}
					if idx == len(s.peerToTopics[peer])-1 {
						slog.Info("peer is not subscribed to topic anymore", "peer", peer, "topic", topic)
					}
				}
			}
		}
	}

	if len(s.peerToTopics[peer]) == 0 {
		return
	} else {
		go s.handlePeer(peer, s.peerToTopics[peer])
	}
}

// Need To rewrite this section to asynchronously write messages to peers
func (s *Server) handlePeer(peer Peer, peerTopicDetails []*PeerTopicDetails) {
	for {
		select {
		case <-s.quitch:
			return
		case <-peer.GetPeerSubscription():
			return
		default:
			// Read message from storage
			if err := s.writeToPeer(peer, peerTopicDetails); err != nil {
				if err.Error() == "peer has caught up with latest offset for all topics" {
					continue
				}
				return
			}
		}
	}
}

func (s *Server) writeToPeer(peer Peer, peerTopicDetails []*PeerTopicDetails) error {
	for {
		select {
		case <-peer.GetPeerSubscription():
			return fmt.Errorf("peer subscriptions updated")
		default:
			// Check if peer has caught up with latest offset of all topics
			for idx, peerTopicDetail := range peerTopicDetails {
				if peerTopicDetail.Offset <= s.topics[peerTopicDetail.Topic].Size() {
					break
				}
				if idx == len(peerTopicDetails)-1 {
					if peerTopicDetail.Offset >= s.topics[peerTopicDetail.Topic].Size() {
						return fmt.Errorf("peer has caught up with latest offset for all topics")
					}
				}
			}
			// Read message from storage
			for _, peerTopicDetail := range peerTopicDetails {
				payload, err := s.topics[peerTopicDetail.Topic].Get(peerTopicDetail.Offset)
				if err != nil {
					continue
				}

				msg, err := json.Marshal(MessageToPeer{
					Topic:   peerTopicDetail.Topic,
					Payload: payload,
					Offset:  peerTopicDetail.Offset,
				})
				if err != nil {
					log.Println("Error marshaling message:", err)
					continue
				}

				peerTopicDetail.Offset++

				if err := peer.Send(msg); err != nil {
					if err == io.EOF {
						delete(s.peers, peer)
						delete(s.peerToTopics, peer)
						return fmt.Errorf("peer got disconnected")
					} else {
						log.Println(err)
						return fmt.Errorf("peer probably got disconnected")
					}
				}
			}
		}
	}
}

func (s *Server) publish(msg MessageToTopic) (int, error) {
	s.getStoreForTopic(msg.Topic)
	return s.topics[msg.Topic].Push(msg.Payload)
}

func (s *Server) getStoreForTopic(topic string) {
	if _, ok := s.topics[topic]; !ok {
		s.topics[topic] = s.StorageProducerFunc()
		slog.Info("Topis is created", "topic", topic)
	}
}
