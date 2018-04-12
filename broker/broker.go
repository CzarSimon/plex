package broker

import (
	"errors"
	"fmt"

	"github.com/CzarSimon/plex/pkg"
	"github.com/CzarSimon/plex/pkg/schema"
)

// Common errors
var (
	ErrNoSuchTopic = errors.New("No such topic")
)

// Broker hanlder of incomming messages and listening consumers.
type Broker struct {
	Topics     map[string]*pkg.Topic
	shutdownCh chan int
}

// New creates a new broker.
func New(shutdownCh chan int, topicList ...*pkg.Topic) *Broker {
	topics := make(map[string]*pkg.Topic)
	for _, topic := range topicList {
		_, ok := topics[topic.Name]
		if !ok {
			topics[topic.Name] = topic
		}
	}
	return &Broker{
		Topics:     topics,
		shutdownCh: shutdownCh,
	}
}

// Start initializes a brokers eventloop.
func (b *Broker) Start() {
	for {
		shutdownSignal := <-b.shutdownCh
		fmt.Printf("Shutdown signal %d recieved\n", shutdownSignal)
		if shutdownSignal == 9 {
			fmt.Println("Stopping broker")
			break
		}
	}
}

// HandleMessage appends a new message to its topic.
func (b *Broker) HandleMessage(msg schema.Message) error {
	topic, ok := b.Topics[msg.Topic]
	if !ok {
		return ErrNoSuchTopic
	}
	topic.Append(msg)
	return nil
}
