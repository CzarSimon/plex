package main

import (
	"fmt"
	"net/http"

	"github.com/CzarSimon/httputil"
	"github.com/CzarSimon/httputil/query"
	"github.com/CzarSimon/plex/broker"
	"github.com/CzarSimon/plex/consumer"
	"github.com/julienschmidt/httprouter"
)

// addConsumer sets up a consumer handler on a given topic.
func (env *Env) addConsumer(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	handler, handlerErr := getConsumerHandler(w, r, ps)
	if handlerErr != nil {
		httputil.SendErr(w, *handlerErr)
		return
	}
	err := env.broker.RegisterConsumer(ps.ByName("topicName"), handler)
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

// getConsumerHandler sets up a consumer handler based on a the requested type.
func getConsumerHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (consumer.Handler, *httputil.Error) {
	var handler consumer.Handler
	var err error

	switch ps.ByName("type") {
	case consumer.LogConsumerType:
		handler, err = createLogConsumer(r)
	case consumer.WebsocketHandlerType:
		handler, err = consumer.NewWebsocketHandler(w, r)
	default:
		handler, err = nil, httputil.BadRequest
	}
	if err == nil {
		return handler, nil
	}

	fmt.Println(err)

	if err != httputil.BadRequest {
		err = httputil.Error{Status: http.StatusInternalServerError, Err: err}
	}
	httperr, ok := err.(httputil.Error)
	if !ok {
		return nil, &httputil.InternalServerError
	}
	return handler, &httperr
}

func createLogConsumer(r *http.Request) (consumer.Handler, error) {
	consumerName, err := query.ParseValue(r, "name")
	if err != nil {
		return nil, httputil.BadRequest
	}
	return consumer.NewLogConsumer(consumerName), nil
}
