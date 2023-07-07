package main

import (
    //"fmt"
    //"strings"
    "strconv"
)


type UserFromDB struct {
    Id int
    UserName string
    Login string
    Pswd string
    UserRole int
}


// SELECT * FROM users
func cmd_select(words []string) string {
	var output string
	
	bk := UserFromDB{Id: 0, UserName: "admin", Login: "admin", Pswd: "pass", UserRole: 1}
	
	output = strconv.Itoa(bk.Id) + " " + bk.UserName + " " + bk.Login + " " + bk.Pswd + " " + strconv.Itoa(bk.UserRole) + "\n"
	return output 
}

