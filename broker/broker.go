package broker

import (
	"errors"
	"fmt"

    "github.com/CzarSimon/plex/consumer"
    "github.com/CzarSimon/plex/pkg"
	"github.com/CzarSimon/plex/pkg/schema"
)

// Common errors
var (
	ErrNoSuchTopic    = errors.New("No such topic")
)

// Broker hanlder of incomming messages and listening consumers.
type Broker struct {
	Topics     map[string]*multiplexer
	shutdownCh chan int
    muxCh      chan *multiplexer
}

// New creates a new broker.
func New(shutdownCh chan int, topicList ...*pkg.Topic) *Broker {
	topics := make(map[string]*multiplexer)
	for _, topic := range topicList {
		if _, ok := topics[topic.Name]; !ok {
			topics[topic.Name] = newMultiplexer(topic)
		}
	}
	return &Broker{
		Topics:     topics,
		shutdownCh: shutdownCh,
        muxCh:      make(chan *multiplexer),
	}
}

// Start initializes a brokers eventloop.
func (b *Broker) Start() {
	go b.handleNewTopics()
    for _, mux := range b.Topics {
        b.muxCh <- mux
    }
    for shutdownSignal := range b.shutdownCh {
		fmt.Printf("Shutdown signal %d recieved\n", shutdownSignal)
		if shutdownSignal == 9 {
			fmt.Println("Stopping broker")
			break
		}
	}
}

// HandleMessage appends a new message to its topic.
func (b *Broker) HandleMessage(msg schema.Message) error {
	mux, ok := b.Topics[msg.Topic]
	if !ok {
		return ErrNoSuchTopic
	}
	mux.topic.Append(msg)
	return nil
}

// RegisterConsumer registers a new consumer.
func (b *Broker) RegisterConsumer(topicName string, handler consumer.Handler) error {
    mux, ok := b.Topics[topicName]
    if !ok {
        return ErrNoSuchTopic
    }
    mux.addHandler(handler)
    return nil
}

// handleConsumerRegistrations handles new consumer registrations.
func (b *Broker) handleNewTopics() {
    for mux := range b.muxCh {
        go mux.listen()
    }
}

