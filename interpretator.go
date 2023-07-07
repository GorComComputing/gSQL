package main

import (
    //"fmt"
)


// Command list for interpretator
var cmd =  map[string]func([]string)string{
	"select": cmd_select,
}



// Interpretator 
func interpretator(words []string) string {
	if _, ok := cmd[words[0]]; ok {
		return cmd[words[0]](words)
	} else{
		return "Unknown command: " + words[0] + "\n"
	}
}


