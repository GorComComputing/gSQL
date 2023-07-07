package main

import (
	"fmt"
    	"net/http"
	"strings"
)


// /api handler
func http_pars(w http.ResponseWriter, r *http.Request) {
	enableCors(&w)	// enable CORS
	// parameters from POST or GET
        r.ParseForm()
	words := []string{}

	for param, values := range r.Form {       // range over map
  		for _, value := range values {    // range over []string
     			if param == "cmd" {
				words = strings.Fields(value)
			} else {
				words = append(words, string(param) + "=" + string(value))
			}
  		}
	}
	out := interpretator(words)
	if len(out) > 0 {fmt.Fprintf(w, out)}
}


// Enable CORS
func enableCors(w *http.ResponseWriter) {
        (*w).Header().Set("Access-Control-Allow-Origin", "*")
}

