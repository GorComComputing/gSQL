package main

import (
    //"fmt"
)


// Command list for interpretator
var cmd =  map[string]func([]string)string{
	"select": cmd_select,
	"update": cmd_update,
	"create": create_table,
	"insert": cmd_insert,
	"delete": cmd_delete,
	
	".save": cmd_save,
	".load": cmd_load,
}


// Interpretator 
func interpretator(words []string) string {
	if _, ok := cmd[words[0]]; ok {
		return cmd[words[0]](words)
	} else{
		return "Unknown command: " + words[0] + "\n"
	}
}


