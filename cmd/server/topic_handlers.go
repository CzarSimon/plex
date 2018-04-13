package main

import (
	"net/http"

	"github.com/CzarSimon/httputil"
	"github.com/julienschmidt/httprouter"
)

func healthCheck(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	httputil.SendOK(w)
}
