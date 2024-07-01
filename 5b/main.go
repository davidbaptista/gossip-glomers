package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type entry struct {
	offset  int
	message int
}

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
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

	offset, err := s.kv.ReadInt(context.Background(), "last-"+body.Key)

	if err != nil {
		var rpcError *maelstrom.RPCError
		errors.As(err, &rpcError)
		if rpcError.Code == maelstrom.KeyDoesNotExist {
			offset = 0
		} else {
			return err
		}
	} else {
		offset++
	}

	for {
		err := s.kv.CompareAndSwap(context.Background(), "last-"+body.Key, offset-1, offset, true)

		if err == nil {
			break
		}

		var rpcErr *maelstrom.RPCError
		errors.As(err, &rpcErr)

		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			offset++
		} else {
			return err
		}
	}

	offsetStr := fmt.Sprintf("%d", offset)
	if err := s.kv.Write(context.Background(), "entry-"+body.Key+"-"+offsetStr, body.Msg); err != nil {
		return err
	}

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

	msgs := make(map[string][][]int)

	for key, minOffset := range body.Offsets {
		for offset := minOffset; ; offset++ {
			offsetStr := fmt.Sprintf("%d", offset)
			val, err := s.kv.ReadInt(context.Background(), "entry-"+key+"-"+offsetStr)

			if err != nil {
				var rpcErr *maelstrom.RPCError
				errors.As(err, &rpcErr)

				if rpcErr.Code == maelstrom.KeyDoesNotExist {
					break
				} else {
					return err
				}
			}

			msgs[key] = append(msgs[key], []int{offset, val})

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
	kv := maelstrom.NewLinKV(node)
	server := server{node: node, kv: kv}

	node.Handle("send", server.handleSend)
	node.Handle("poll", server.handlePoll)
	node.Handle("commit_offsets", server.handleCommitOffsets)
	node.Handle("list_committed_offsets", server.handleListCommitedOffsets)

	if err := node.Run(); err != nil {
		panic(err)
	}
}
