package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"sync"
)

type entry struct {
	offset  int
	message int
}

type server struct {
	node            *maelstrom.Node
	mu              sync.RWMutex
	logs            map[string][]entry
	commitedOffsets map[string]int
	lastOffsets     map[string]int
}

type sendRequest struct {
	MessageType string `json:"type"`
	Key         string `json:"key"`
	Msg         int    `json:"msg"`
}

func (s *server) handleSend(message maelstrom.Message) error {
	var body sendRequest

	if err := json.Unmarshal(message.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	offset, ok := s.lastOffsets[body.Key]
	if !ok {
		offset = 0
	} else {
		offset = offset + 1
	}

	s.logs[body.Key] = append(s.logs[body.Key], entry{offset: offset, message: body.Msg})
	s.lastOffsets[body.Key] = offset

	return s.node.Reply(message, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

type pollRequest struct {
	MessageType string         `json:"type"`
	Offsets     map[string]int `json:"offsets"`
}

func (s *server) handlePoll(request maelstrom.Message) error {
	var body pollRequest

	if err := json.Unmarshal(request.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := make(map[string][][]int)

	for key, minOffset := range body.Offsets {
		for _, entry := range s.logs[key] {
			if entry.offset < minOffset {
				continue
			}

			msgs[key] = append(msgs[key], []int{entry.offset, entry.message})
		}
	}

	return s.node.Reply(request, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

type commitOffsetsRequest struct {
	MessageType string         `json:"type"`
	Offsets     map[string]int `json:"offsets"`
}

func (s *server) handleCommitOffsets(request maelstrom.Message) error {
	var body commitOffsetsRequest
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, maxOffset := range body.Offsets {
		for _, entry := range s.logs[key] {
			if entry.offset < maxOffset {
				continue
			}
			if entry.offset > maxOffset {
				break
			}

			s.commitedOffsets[key] = entry.offset
		}
	}

	return s.node.Reply(request, map[string]any{
		"type": "commit_offsets_ok",
	})
}

type listCommitedOffsetsRequest struct {
	MessageType string   `json:"type"`
	Keys        []string `json:"keys"`
}

func (s *server) handleListCommitedOffsets(request maelstrom.Message) error {
	var body listCommitedOffsetsRequest
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	commitedOffsets := make(map[string]int)

	for _, key := range body.Keys {
		if offset, ok := s.commitedOffsets[key]; ok {
			commitedOffsets[key] = offset
		}
	}

	return s.node.Reply(request, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": commitedOffsets,
	})
}

func main() {
	node := maelstrom.NewNode()
	server := server{node: node, logs: make(map[string][]entry), commitedOffsets: make(map[string]int), lastOffsets: make(map[string]int)}

	node.Handle("send", server.handleSend)
	node.Handle("poll", server.handlePoll)
	node.Handle("commit_offsets", server.handleCommitOffsets)
	node.Handle("list_committed_offsets", server.handleListCommitedOffsets)

	if err := node.Run(); err != nil {
		panic(err)
	}
}
