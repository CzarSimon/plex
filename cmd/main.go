package main

import (
    "fmt"
    "time"

    "github.com/CzarSimon/plex"
    "github.com/CzarSimon/plex/schema"
)

const TopicName = "t1"

func newMessage(body string) schema.Message {
    return schema.NewMessage(TopicName, []byte(body))
}

func consume(topic *plex.Topic) {
    for {
        msg := <-topic.Channel
        fmt.Println(msg)
    }
}

func produce(broker *plex.Broker) {
    var msg schema.Message
    for {
        time.Sleep(1 * time.Second)
        msg = newMessage("Hello")
        broker.HandleMessage(msg)
    }
}

func main() {
    topic := plex.NewTopic(TopicName)
    broker := plex.NewBroker(topic)
    go consume(topic)
    go produce(broker)

    fmt.Println("Press enter to exit: ")
    fmt.Scanln()
}

