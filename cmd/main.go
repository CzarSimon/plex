package main

import (
	"sync"
	"time"

	"github.com/CzarSimon/plex/broker"
	"github.com/CzarSimon/plex/consumer"
	"github.com/CzarSimon/plex/pkg"
	"github.com/CzarSimon/plex/pkg/schema"
)

func newMessage(topicName, body string) schema.Message {
	return schema.NewMessage(topicName, []byte(body))
}

func produce(b *broker.Broker) {
	var msg schema.Message
	topic := "topic1"
	for {
		time.Sleep(1 * time.Second)
		msg = newMessage(topic, "Hello")
		err := b.HandleMessage(msg)
		if err == broker.ErrNoSuchTopic {
			topic = "topic2"
		}
	}
}

func main() {
	topic1 := pkg.NewTopic("topic1")
	topic2 := pkg.NewTopic("topic2")
	shutdownCh := make(chan int)
	b := broker.New(shutdownCh)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Start()
	}()

	b.RegisterTopic(topic1)
	b.RegisterTopic(topic2)

	b.RegisterConsumer("topic1", consumer.NewLogConsumer("consumer1"))
	b.RegisterConsumer("topic1", consumer.NewLogConsumer("consumer2"))
	b.RegisterConsumer("topic2", consumer.NewLogConsumer("consumer3"))

	go produce(b)
	go func() {
		shutdownSignal := 9
		time.Sleep(10 * time.Second)
		shutdownCh <- shutdownSignal
	}()

	wg.Wait()
}
