package main

import (
    //"fmt"
    //"strings"
    //"strconv"
    //"os"
    //"bufio"
    //"encoding/binary"
    
    //"unicode"
    //"errors"
    
    //"encoding/gob"
    //"bytes"
    //"reflect"
)



type DB struct {
	Name 	string
	File 	string
	Tables 	[]TableDef
}

type TableDef struct {
	// user defined
	Name string
	Types []int // column types
	Cols []string // column names
	PKeys int // the first `PKeys` columns are the primary key
	// auto-assigned B-tree key prefixes for different tables
	Prefix uint32
	
    	//Rows []Row
    	Rows [][]string
}

type Row struct {
	Id	 int
    	UserName string
    	Login 	 string
    	Pswd 	 string
    	UserRole int
}


var tst_db = DB{
	Name: "tst_db",
	File: "files/base.db",
	//Tables: []TableDef{UserTable},
}

var UserTable = TableDef{
	Name: "users",
	Cols: []string{"Id", "User Name", "Login", "Password", "User Role"},
}


func init_db() {
	//var words []string
	//cmd_load(words)
}



