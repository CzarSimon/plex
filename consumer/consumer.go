package consumer

import (
   "errors"

   "github.com/CzarSimon/plex/pkg/schema"
)

var (
    ErrConsumerClosed = errors.New("Consumer closed")
)

// Handler is a function for consumers to handle incomming mesages.
type Handler func(msg schema.Message) error

