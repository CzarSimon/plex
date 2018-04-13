package main

import (
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

const port = "8080"

func setupRouter() *httprouter.Router {
	router := httprouter.New()

	router.POST("/v1/topic/:name", healthCheck)
	router.GET("/health", healthCheck)

	return router
}

func main() {
	log.Printf("Starting PLEX_SERVER listening on port: %s\n", port)
	err := http.ListenAndServe(":"+port, setupRouter())
	if err != nil {
		log.Fatal(err)
	}
}
