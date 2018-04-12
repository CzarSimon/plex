package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/CzarSimon/plex/broker"
	"github.com/CzarSimon/plex/pkg"
	"github.com/CzarSimon/plex/pkg/schema"
)

const TopicName = "t1"

func newMessage(body string) schema.Message {
	return schema.NewMessage(TopicName, []byte(body))
}

func consume(topic *pkg.Topic) {
	for {
		msg := <-topic.Channel
		fmt.Println(msg)
	}
}

func produce(broker *broker.Broker) {
	var msg schema.Message
	for {
		time.Sleep(1 * time.Second)
		msg = newMessage("Hello")
		broker.HandleMessage(msg)
	}
}

func main() {
	topic := pkg.NewTopic(TopicName)
	shutdownCh := make(chan int)
	b := broker.New(shutdownCh, topic)
	go consume(topic)
	go produce(b)

	go func() {
		shutdownSignal := 9
		time.Sleep(10 * time.Second)
		shutdownCh <- shutdownSignal
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Start()
	}()
	wg.Wait()
}
