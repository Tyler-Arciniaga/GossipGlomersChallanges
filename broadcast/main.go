package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type messagesStorage struct {
	messages map[float64]bool
	lock     sync.Mutex
}

func (m *messagesStorage) addMessage(msg float64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.messages[msg] = true
}

func (m *messagesStorage) getMessages() []float64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	res := make([]float64, 0, len(m.messages))
	for msg := range m.messages {
		res = append(res, msg)
	}
	return res
}

type neighbourStore struct {
	lock       sync.Mutex
	neighbours map[string]bool
}

func (n *neighbourStore) add(nei string) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.neighbours[nei] = true
}

func (n *neighbourStore) list() []string {
	n.lock.Lock()
	defer n.lock.Unlock()
	res := make([]string, 0, len(n.neighbours))
	for k := range n.neighbours {
		res = append(res, k)
	}
	return res
}

func main() {
	n := maelstrom.NewNode()
	store := &messagesStorage{messages: make(map[float64]bool)}
	neighbours := &neighbourStore{neighbours: make(map[string]bool)}

	// Periodic gossip to all neighbors
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		for range ticker.C {
			msgs := store.getMessages()
			for _, nei := range neighbours.list() {
				body := map[string]any{
					"type":     "propagate",
					"messages": msgs,
					"src":      n.ID(),
				}
				n.Send(nei, body)
			}
		}
	}()

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology := body["topology"].(map[string]interface{})
		for _, nei := range topology[n.ID()].([]interface{}) {
			neighbours.add(nei.(string))
		}

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		m := body["message"].(float64)
		store.addMessage(m)

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("propagate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := body["messages"].([]interface{})
		for _, m := range msgs {
			store.addMessage(m.(float64))
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = store.getMessages()
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
