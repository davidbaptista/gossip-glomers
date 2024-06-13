package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type gossipMessage struct {
	dst string
	msg map[string]any
}

type gossipService struct {
	ch     chan gossipMessage
	cancel context.CancelFunc
}

func newGossipService(n *maelstrom.Node, numWorkers int) *gossipService {
	ch := make(chan gossipMessage)
	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				select {
				case msg := <-ch:
					for {
						if err := n.Send(msg.dst, msg.msg); err != nil {
							continue
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return &gossipService{
		ch:     ch,
		cancel: cancel,
	}
}

func (g *gossipService) gossip(dst string, msg map[string]any) {
	g.ch <- gossipMessage{dst: dst, msg: msg}
}

func (g *gossipService) close() {
	g.cancel()
}

type server struct {
	node          *maelstrom.Node
	ids           map[int]struct{}
	idsMu         sync.RWMutex
	gossipService *gossipService
}

func (s *server) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "echo_ok"
	body["in_reply_to"] = body["msg_id"]

	return s.node.Reply(msg, body)
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	messageId := int(body["message"].(float64))
	s.idsMu.Lock()
	defer s.idsMu.Unlock()
	if _, exists := s.ids[messageId]; exists {
		return nil
	}
	s.ids[messageId] = struct{}{}

	for _, id := range s.node.NodeIDs() {
		if id == msg.Src || id == s.node.ID() {
			continue
		}

		dst := id
		s.gossipService.gossip(dst, body)
	}

	return s.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var idsList []int
	s.idsMu.Lock()
	for id := range s.ids {
		idsList = append(idsList, id)
	}
	s.idsMu.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": idsList,
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	// setup
	n := maelstrom.NewNode()
	g := newGossipService(n, len(n.NodeIDs()))
	s := server{n, make(map[int]struct{}), sync.RWMutex{}, g}
	defer g.close()

	s.node.Handle("echo", s.handleEcho)
	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("topology", s.handleTopology)

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}
