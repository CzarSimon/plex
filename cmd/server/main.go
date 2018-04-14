package main

import (
	"log"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
)

func setupRouter(env *Env) *httprouter.Router {
	router := httprouter.New()

	router.POST("/v1/topic/:topicName", env.createTopic)
	router.PUT("/v1/topic/:topicName", env.appendMessage)
	router.GET("/v1/topic/:topicName/consume", env.addConsumer)

	router.GET("/health", healthCheck)

	return router
}

func main() {
	config := getConfig("8080")
	env := newEnv()

	go env.broker.Start()

	log.Printf("Starting PLEX_SERVER listening on port: %s\n", config.Port)
	err := http.ListenAndServe(":"+config.Port, setupRouter(env))

	env.shutdownCh <- 9
	time.Sleep(1 * time.Second)
	if err != nil {
		log.Fatal(err)
	}
}
