package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

type topology map[string][]string

type gossipService struct {
	ticker     *time.Ticker
	quit       chan struct{}
	neighbours []string
}

func (s *server) propagate() {
	for {
		select {
		case <-s.gossip.ticker.C:
			if len(s.gossip.neighbours) == 0 {
				continue
			}
			s.idMap.idsMu.Lock()
			for _, id := range s.idMap.idsToSend {
				for _, neighbour := range s.gossip.neighbours {
					message := map[string]any{
						"type":    "gossip",
						"message": id,
					}

					if err := s.node.Send(neighbour, message); err != nil {
						continue
					}
				}
			}
			s.idMap.idsToSend = s.idMap.idsToSend[:0]
			s.idMap.idsMu.Unlock()
		case <-s.gossip.quit:
			return
		}
	}
}

type idMap struct {
	ids       map[int]struct{}
	idsMu     sync.RWMutex
	idsToSend []int
}

type server struct {
	node     *maelstrom.Node
	idMap    idMap
	gossip   gossipService
	topology topology
}

type gossipMessage struct {
	MessageType string `json:"type"`
	Message     int    `json:"message"`
}

func (s *server) handleGossip(message maelstrom.Message) error {
	var body gossipMessage
	if err := json.Unmarshal(message.Body, &body); err != nil {
		return err
	}

	s.idMap.idsMu.Lock()
	defer s.idMap.idsMu.Unlock()
	if _, exists := s.idMap.ids[body.Message]; exists {
		return nil
	}

	s.idMap.ids[body.Message] = struct{}{}
	s.idMap.idsToSend = append(s.idMap.idsToSend, body.Message)
	return nil
}

type broadcastMessage struct {
	MessageType string `json:"type"`
	Message     int    `json:"message"`
}

func (s *server) handleBroadcast(message maelstrom.Message) error {
	var body broadcastMessage
	if err := json.Unmarshal(message.Body, &body); err != nil {
		return err
	}

	s.idMap.idsMu.Lock()
	defer s.idMap.idsMu.Unlock()
	if _, exists := s.idMap.ids[body.Message]; exists {
		return nil
	}

	s.idMap.ids[body.Message] = struct{}{}
	s.idMap.idsToSend = append(s.idMap.idsToSend, body.Message)
	return s.node.Reply(message, map[string]any{
		"type": "broadcast_ok",
	})
}

type readMessage struct {
	MessageType string `json:"type"`
}

func (s *server) handleRead(message maelstrom.Message) error {
	var body readMessage

	if err := json.Unmarshal(message.Body, &body); err != nil {
		return err
	}

	var ids []int
	s.idMap.idsMu.Lock()
	for id := range s.idMap.ids {
		ids = append(ids, id)
	}
	s.idMap.idsMu.Unlock()

	return s.node.Reply(message, map[string]any{
		"type":     "read_ok",
		"messages": ids,
	})
}

type topologyMessage struct {
	MessageType string   `json:"type"`
	Topology    topology `json:"topology"`
}

func (s *server) handleTopology(message maelstrom.Message) error {
	var body topologyMessage
	if err := json.Unmarshal(message.Body, &body); err != nil {
		return err
	}

	s.topology = body.Topology

	for _, neighbour := range s.topology[s.node.ID()] {
		s.gossip.neighbours = append(s.gossip.neighbours, neighbour)
	}

	return s.node.Reply(message, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	node := maelstrom.NewNode()
	gossip := gossipService{ticker: time.NewTicker(1000 * time.Millisecond)}
	defer gossip.ticker.Stop()
	quit := make(chan struct{})
	server := server{node: node, idMap: idMap{ids: make(map[int]struct{})}, gossip: gossip}

	go server.propagate()

	node.Handle("broadcast", server.handleBroadcast)
	node.Handle("gossip", server.handleGossip)
	node.Handle("read", server.handleRead)
	node.Handle("topology", server.handleTopology)

	if err := node.Run(); err != nil {
		log.Fatalln(err)
	}

	close(quit)
}
