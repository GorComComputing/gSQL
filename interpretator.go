package main

import (
    //"fmt"
    "os"
)


type Cmd struct {
	addr 	func([]string) string
	descr string     
}


// Command list for interpretator
var cmd = map[string]Cmd{
	"select": Cmd{addr: cmd_select, descr: "SQL request: select * from table;"},
	"update": Cmd{addr: cmd_update, descr: "SQL request: update table set col1=val1, col2=val2;"},
	"create": Cmd{addr: cmd_create, descr: "SQL request: create table;"},
	"insert": Cmd{addr: cmd_insert, descr: "SQL request: insert into table values val1, val2;"},
	"delete": Cmd{addr: cmd_delete, descr: "SQL request: delete from table;"},
	
	".save": Cmd{addr: cmd_save, descr: "Save table into file"},
	".load": Cmd{addr: cmd_load, descr: "Load table from file"},
	".quit": Cmd{addr: cmd_quit, descr: "Exit from this program"},
	".help": Cmd{addr: cmd_help, descr: "Print this Help"},
	
	"lstbl": Cmd{addr: cmd_lstbl, descr: "List of tables"},
	"vtbl": Cmd{addr: cmd_vtbl, descr: "View table"},
	
	"drop": Cmd{addr: cmd_drop, descr: "SQL request: drop table name;"},
	"alter": Cmd{addr: cmd_alter, descr: "SQL request: ALTER TABLE name DROP COLUMN col_name;"},
}


// Interpretator 
func interpretator(words []string) string {
	if _, ok := cmd[words[0]]; ok {
		return cmd[words[0]].addr(words)
	} else{
		return "Unknown command: " + words[0] + "\n"
	}
}

// HELP - Print command list
var cmd_print = make(map[string]Cmd)
func cmd_help(words []string) string {
	var output string
	for key, val := range cmd_print {
		output += key 
		for i := len(key); i < 10; i++ {
			output += " "
		} 
		output += " - " + val.descr + "\n"
	}
	return output
}


// Exit from this program
func cmd_quit(words []string) string {
	os.Exit(0)
	return ""
}


