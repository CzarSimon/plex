package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/CzarSimon/httputil"
	"github.com/CzarSimon/httputil/query"
	"github.com/CzarSimon/plex/broker"
	"github.com/CzarSimon/plex/consumer"
	"github.com/CzarSimon/plex/pkg"
	"github.com/CzarSimon/plex/pkg/schema"
	"github.com/julienschmidt/httprouter"
)

// createTopic creates a new topic and registers it to the broker.
func (env *Env) createTopic(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	topicName := ps.ByName("topicName")
	topic := pkg.NewTopic(topicName)
	err := env.broker.RegisterTopic(topic)
	if err == broker.ErrTopicAlreadyExists {
		httputil.SendErr(w, httputil.BadRequest)
		return
	}
	if err != nil {
		httputil.SendErr(w, httputil.InternalServerError)
		return
	}
	log.Printf("Created new topic: %s\n", topicName)
	httputil.SendOK(w)
}

// appendMessage sends a new message over a topic.
func (env *Env) appendMessage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	msgBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		httputil.SendErr(w, httputil.BadRequest)
		return
	}
	msg := schema.NewMessage(ps.ByName("topicName"), msgBody)
	err = env.broker.HandleMessage(msg)
	if err == broker.ErrNoSuchTopic {
		httputil.SendErr(w, httputil.BadRequest)
		return
	}
	if err != nil {
		httputil.SendErr(w, httputil.InternalServerError)
		return
	}
	httputil.SendOK(w)
}

// addConsumer sets up a consumer handler on a given topic.
func (env *Env) addConsumer(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	consumerName, err := query.ParseValue(r, "name")
	if err != nil {
		httputil.SendErr(w, httputil.BadRequest)
		return
	}
	consumer := consumer.NewLogConsumer(consumerName)
	err = env.broker.RegisterConsumer(ps.ByName("topicName"), consumer)
	if err == broker.ErrNoSuchTopic {
		httputil.SendErr(w, httputil.BadRequest)
		return
	}
	if err != nil {
		httputil.SendErr(w, httputil.InternalServerError)
		return
	}
	httputil.SendOK(w)
}

// healthCheck returns a 200 OK if all is well.
func healthCheck(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	httputil.SendOK(w)
}
