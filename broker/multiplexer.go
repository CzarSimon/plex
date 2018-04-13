package broker

import (
    "errors"
    "log"
    "sync"

    "github.com/CzarSimon/plex/pkg"
    "github.com/CzarSimon/plex/pkg/schema"
)

var (
    ErrNoSuchHandler = errors.New("No such handler found")
    ErrConsumerClosed = errors.New("Consumer closed")
)

// A multiplexer forwards messages published on a topic to mulitple consumers.
type multiplexer struct {
    nextId   int
    handlers map[int]ConsumerHandler
    topic    *pkg.Topic
    lock     *sync.RWMutex
}

// newMultiplexer creates a new multiplexer and returns a pointer to it.
func newMultiplexer(topic *pkg.Topic) *multiplexer {
    return &multiplexer{
        nextId:   0,
        handlers: make(map[int]ConsumerHandler),
        topic:    topic,
        lock:     &sync.RWMutex{},
    }
}

// listen listens to messages on a topic an sends messages to regisred consumers.
func (mux *multiplexer) listen() {
    var err error
    for msg := range mux.topic.Channel {
        for id, handle := range mux.handlers {
            err = handle(msg)
            if err == ErrConsumerClosed {
                mux.removeHandler(id)
            }
            if err != nil {
                log.Println(err)
            }
        }
    }
}

// addHandler adds a handler to the multiplexer with a new unique id.
func (mux *multiplexer) addHandler(handler ConsumerHandler) {
    mux.lock.Lock()
    defer mux.lock.Unlock()
    for {
        id := mux.nextId
        _, ok := mux.handlers[id]
        mux.nextId++
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

// ConsumerHandler is a function for consumers to handle incomming mesages.
type ConsumerHandler func(msg schema.Message) error

