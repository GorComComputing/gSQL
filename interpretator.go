package main

import (
    //"fmt"
    "os"
)


// Command list for interpretator
var cmd = map[string]func([]string)string{
	"select": cmd_select,
	"update": cmd_update,
	"create": create_table,
	"insert": cmd_insert,
	"delete": cmd_delete,
	
	".save": cmd_save,
	".load": cmd_load,
	".quit": cmd_quit,
	".help": cmd_help,
}


// Interpretator 
func interpretator(words []string) string {
	if _, ok := cmd[words[0]]; ok {
		return cmd[words[0]](words)
	} else{
		return "Unknown command: " + words[0] + "\n"
	}
}

// Print command list
var cmd_print = make(map[string]func([]string)string)
func cmd_help(words []string) string {
	var output string
	for key, _ := range cmd_print {
		output += key + "\n"
	}
	return output
}

// Exit from this program
func cmd_quit(words []string) string {
	os.Exit(0)
	//exit_status = false
	return ""
}


