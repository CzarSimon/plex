package main

import (
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func setupRouter(env *Env) *httprouter.Router {
	router := httprouter.New()

	router.POST("/v1/topic/:name", env.createTopic)
	router.GET("/health", healthCheck)

	return router
}

func main() {
	config := getConfig("8080")
	env := newEnv()

	log.Printf("Starting PLEX_SERVER listening on port: %s\n", config.Port)
	err := http.ListenAndServe(":"+config.Port, setupRouter(env))
	if err != nil {
		log.Fatal(err)
	}
}
