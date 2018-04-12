package schema

import (
    "fmt"
)

// Message 
type Message struct {
    Body   []byte `json:"body"`
    Topic  string `json:"topic"`
    Offset uint64 `json:"offset"`
}

// NewMessage creates a new message.
func NewMessage(topicName string, body []byte) Message {
    return Message{
        Topic: topicName,
        Body: body,
    }
}

// String returns string representation of a message.
func (m Message) String() string {
    return fmt.Sprintf("Topic=%s Offset=%d\n%s",
        m.Topic, m.Offset, string(m.Body))
}

