package main

import (
	"log"
	"net/http"

	"github.com/CzarSimon/httputil"
	"github.com/CzarSimon/httputil/query"
	"github.com/CzarSimon/plex/broker"
	"github.com/CzarSimon/plex/consumer"
	"github.com/julienschmidt/httprouter"
)

// addConsumer sets up a consumer handler on a given topic.
func (env *Env) addConsumer(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var handler consumer.Handler
	var err error
	switch ps.ByName("type") {
	case "Logger":
		handler, err = createLogConsumer(r)
	default:
		handler, err = nil, httputil.BadRequest
	}
	switch e := err.(type) {
	case httputil.Error:
		httputil.SendErr(w, e)
		return
	case nil:
	default:
		log.Println(err)
		httputil.SendErr(w, httputil.InternalServerError)
		return
	}
	err = env.broker.RegisterConsumer(ps.ByName("topicName"), handler)
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

func createLogConsumer(r *http.Request) (consumer.Handler, error) {
	consumerName, err := query.ParseValue(r, "name")
	if err != nil {
		return nil, httputil.BadRequest
	}
	return consumer.NewLogConsumer(consumerName), nil
}
