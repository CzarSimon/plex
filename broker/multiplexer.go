package broker

import (
	"errors"
	"log"
	"sync"

	"github.com/CzarSimon/plex/consumer"
	"github.com/CzarSimon/plex/pkg"
)

// Common errors for a multiplexer
var (
	ErrNoSuchHandler = errors.New("No such handler found")
)

// A multiplexer forwards messages published on a topic to mulitple consumers.
type multiplexer struct {
	nextID   int
	handlers map[int]consumer.Handler
	topic    *pkg.Topic
	lock     *sync.RWMutex
}

// newMultiplexer creates a new multiplexer and returns a pointer to it.
func newMultiplexer(topic *pkg.Topic) *multiplexer {
	return &multiplexer{
		nextID:   0,
		handlers: make(map[int]consumer.Handler),
		topic:    topic,
		lock:     &sync.RWMutex{},
	}
}

// listen listens to messages on a topic an sends messages to regisred consumers.
func (mux *multiplexer) listen() {
	var err error
	for msg := range mux.topic.Channel {
		for id, handler := range mux.handlers {
			err = handler.Handle(msg)
			if err == consumer.ErrConsumerClosed {
				mux.removeHandler(id)
			}
			if err != nil {
				log.Println(err)
			}
		}
	}
}

// addHandler adds a handler to the multiplexer with a new unique id.
func (mux *multiplexer) addHandler(handler consumer.Handler) {
	mux.lock.Lock()
	defer mux.lock.Unlock()
	for {
		id := mux.nextID
		_, ok := mux.handlers[id]
		mux.nextID++
		if !ok {
			mux.handlers[id] = handler
			return
		}
	}
}

// removeHandler removes a handler from a multiplexer.
func (mux *multiplexer) removeHandler(id int) error {
	mux.lock.Lock()
	defer mux.lock.Unlock()
	_, ok := mux.handlers[id]
	if !ok {
		return ErrNoSuchHandler
	}
	delete(mux.handlers, id)
	return nil
}
