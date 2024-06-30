package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type server struct {
	node    *maelstrom.Node
	counter int
	mu      sync.RWMutex
	kv      *maelstrom.KV
	ctx     context.Context
}

type addMessage struct {
	MessageType string `json:"type"`
	Delta       int    `json:"delta"`
}

func (s *server) handleAdd(message maelstrom.Message) error {
	var body addMessage
	if err := json.Unmarshal(message.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	prevValue, err := s.kv.ReadInt(s.ctx, s.node.ID())

	if err != nil {
		return err
	}

	err = s.kv.CompareAndSwap(context.Background(), s.node.ID(), prevValue, prevValue+body.Delta, true)

	if err != nil {
		return err
	}

	val := body.Delta
	go func() {
		for _, node := range s.node.NodeIDs() {
			if node == s.node.ID() {
				continue
			}

			dst := node
			go func() {
				msg := map[string]any{
					"type":  "gossip",
					"delta": val,
				}

				if err := s.node.Send(dst, msg); err != nil {
					return
				}
			}()
		}
	}()

	return s.node.Reply(message, map[string]any{
		"type": "add_ok",
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

	s.mu.Lock()
	defer s.mu.Unlock()
	value, err := s.kv.ReadInt(s.ctx, s.node.ID())

	if err != nil {
		return err
	}

	return s.node.Reply(message, map[string]any{
		"type":  "read_ok",
		"value": value,
	})
}

type gossipMessage struct {
	MessageType string `json:"type"`
	Delta       int    `json:"delta"`
}

func (s *server) handleGossip(message maelstrom.Message) error {
	var body gossipMessage

	if err := json.Unmarshal(message.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	prevValue, err := s.kv.ReadInt(context.Background(), s.node.ID())
	if err != nil {
		return err
	}

	err = s.kv.CompareAndSwap(context.Background(), s.node.ID(), prevValue, prevValue+body.Delta, true)

	if err != nil {
		return err
	}

	return nil
}

type initMessage struct {
	MessageType string `json:"type"`
}

func (s *server) handleInit(message maelstrom.Message) error {
	var body initMessage
	if err := json.Unmarshal(message.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.kv.Write(s.ctx, s.node.ID(), 0)

	if err != nil {
		return err
	}

	return nil
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := server{node: node, counter: 0, kv: kv, ctx: ctx}

	node.Handle("init", server.handleInit)
	node.Handle("add", server.handleAdd)
	node.Handle("read", server.handleRead)
	node.Handle("gossip", server.handleGossip)

	if err := node.Run(); err != nil {
		log.Fatalln(err)
	}
}
