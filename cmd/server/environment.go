package main

import (
	"github.com/CzarSimon/plex/broker"
)

// Env holds environment resources.
type Env struct {
	broker     *broker.Broker
	shutdownCh chan int
}

func newEnv() *Env {
	shutdownCh := make(chan int)
	return &Env{
		broker:     broker.New(shutdownCh),
		shutdownCh: shutdownCh,
	}
}

// Config holds configuration values.
type Config struct {
	Port string
}

func getConfig(port string) Config {
	return Config{Port: port}
}
