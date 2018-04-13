package consumer

import (
	"errors"
	"fmt"

	"github.com/CzarSimon/plex/pkg/schema"
)

// Common consumer errors
var (
	ErrConsumerClosed = errors.New("Consumer closed")
)

// Handler is a function for consumers to handle incomming mesages.
type Handler interface {
	Handle(msg schema.Message) error
}

// A LogConsumer logs all incomming messages to stdout and
// stores all recorded messages in a slice for inspection purposes.
type LogConsumer struct {
	Name     string
	Messages []schema.Message
}

// NewLogConsumer creates a new LogConsumer.
func NewLogConsumer(name string) LogConsumer {
	return LogConsumer{
		Name:     name,
		Messages: make([]schema.Message, 0),
	}
}

// Handle logs an incomming message and appends it to the list.
func (c LogConsumer) Handle(msg schema.Message) error {
	fmt.Printf("Consumer: %s\n%s\n", c.Name, msg)
	c.Messages = append(c.Messages, msg)
	return nil
}
