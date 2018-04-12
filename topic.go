package plex

import (
    "sync"

    "github.com/CzarSimon/plex/schema"
)

// Topic
type Topic struct {
    Name    string
    Channel chan schema.Message
    Depth   uint64
    lock    sync.Mutex
}

// NewTopic creates a new empty topic with a given name.
func NewTopic(name string) *Topic {
    return &Topic{
        Name: name,
        Channel: make(chan schema.Message),
        Depth: 0,
        lock: sync.Mutex{},
    }
}

// Append add a message to the topic to be consumed by consumers
func (t *Topic) Append(msg schema.Message) {
    t.lock.Lock()
    defer t.lock.Unlock()
    t.Depth++
    msg.Offset = t.Depth
    t.Channel <- msg
}

