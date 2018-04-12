package plex

import (
    "errors"

    "github.com/CzarSimon/plex/schema"
)

var (
    NoSuchTopic = errors.New("No such topic")
)

type Broker struct {
    Topics map[string]*Topic
}

// NewBroker creates a new broker.
func NewBroker(topicList ...*Topic) *Broker {
    topics := make(map[string]*Topic)
    for _, topic := range topicList {
        _, ok := topics[topic.Name]
        if !ok {
            topics[topic.Name] = topic
        }
    }
    return &Broker{Topics: topics}
}

// HandleMessage appends a new message to its topic.
func (b *Broker) HandleMessage(msg schema.Message) error {
    topic, ok := b.Topics[msg.Topic]
    if !ok {
        return NoSuchTopic
    }
    topic.Append(msg)
    return nil
}

