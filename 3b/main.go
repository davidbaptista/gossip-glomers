package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type server struct {
	node  *maelstrom.Node
	ids   map[int]struct{}
	idsMu sync.RWMutex
}

func (s *server) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal([]byte(msg.Body), &body); err != nil {
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
		go func() {
			if err := s.node.Send(dst, body); err != nil {
				panic(err)
			}
		}()
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
	s := server{n, make(map[int]struct{}), sync.RWMutex{}}

	s.node.Handle("echo", s.handleEcho)
	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("topology", s.handleTopology)

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}
