package main

import (
	"net/http"

	"github.com/CzarSimon/httputil"
	"github.com/CzarSimon/plex/broker"
	"github.com/CzarSimon/plex/pkg"
	"github.com/julienschmidt/httprouter"
)

func (env *Env) createTopic(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	topic := pkg.NewTopic(ps.ByName("name"))
	err := env.broker.RegisterTopic(topic)
	if err == broker.ErrTopicAlreadyExists {
		httputil.SendErr(w, httputil.BadRequest)
		return
	}
	if err != nil {
		httputil.SendErr(w, httputil.InternalServerError)
		return
	}
	httputil.SendOK(w)
}

func healthCheck(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	httputil.SendOK(w)
}
