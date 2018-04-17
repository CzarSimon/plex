package pkg

import (
	"sync"

	"github.com/CzarSimon/plex/pkg/schema"
)

// A Topic is a time ordered log of messages.
type Topic struct {
	Name    string
	Channel chan schema.Message
	Depth   uint64
	buffer  MessageBuffer
	lock    sync.Mutex
}

// NewTopic creates a new empty topic with a given name.
func NewTopic(name string) *Topic {
	return &Topic{
		Name:    name,
		Channel: make(chan schema.Message),
		Depth:   0,
		buffer:  newQueueBuffer(1000),
		lock:    sync.Mutex{},
	}
}

// Append add a message to the topic to be consumed by consumers
func (t *Topic) Append(msg schema.Message) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.Depth++
	msg.Offset = t.Depth
	t.Channel <- msg
	t.buffer.Put(msg)
}

// MessageBuffer is an interface for storing messages appended to a topic.
type MessageBuffer interface {
	Put(msg schema.Message) error
	GetAll() ([]schema.Message, error)
	Len() uint64
}

type queueBuffer struct {
	first     *messageNode
	last      *messageNode
	depth     uint64
	limit     uint64
	unlimited bool
	lock      sync.Mutex
}

// newQueueBuffer creates a new queueBuffer.
func newQueueBuffer(limit uint64) *queueBuffer {
	return &queueBuffer{
		first:     nil,
		last:      nil,
		depth:     0,
		limit:     limit,
		unlimited: (limit == 0),
		lock:      sync.Mutex{},
	}
}

// Len returns the current depth of the queueBuffer.
func (buf *queueBuffer) Len() uint64 {
	return buf.depth
}

// GetAll returns all elements in the buffer as a slice.
func (buf *queueBuffer) GetAll() ([]schema.Message, error) {
	buf.lock.Lock()
	currentNode := buf.first
	depth := buf.Len()
	buf.lock.Unlock()
	messages := make([]schema.Message, depth)
	var i uint64
	for i = 0; i < depth; i++ {
		messages[i] = currentNode.message
		currentNode = currentNode.next
	}
	return messages, nil
}

// Put adds a message in the queueBuffer.
func (buf *queueBuffer) Put(msg schema.Message) error {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	newNode := newMessageNode(msg, nil)
	buf.updateFirst(newNode)
	buf.updateLast(newNode)
	if buf.depth < buf.limit || buf.unlimited {
		buf.depth++
	}
	return nil
}

// updateFirst updates the first node of a queueBuffer in response to a put.
func (buf *queueBuffer) updateFirst(newNode *messageNode) {
	if buf.first == nil {
		buf.first = newNode
	}
	if buf.depth == buf.limit && !buf.unlimited {
		old := buf.first
		buf.first = buf.first.next
		old.next = nil
	}
}

// updateLast updates the last node of a queueBuffer in response to a put.
func (buf *queueBuffer) updateLast(newNode *messageNode) {
	if buf.last != nil {
		buf.last.next = newNode
	}
	buf.last = newNode
}

// messageNode is an element with a messages that can be liked with other nodes.
type messageNode struct {
	message schema.Message
	next    *messageNode
}

// newMessageNode creates a new messageNode.
func newMessageNode(msg schema.Message, next *messageNode) *messageNode {
	return &messageNode{
		message: msg,
		next:    next,
	}
}
