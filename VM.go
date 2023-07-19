package main

import (
    "fmt"
    //"strings"
    "strconv"
    //"os"
    //"bufio"
    //"encoding/binary"
    
    //"unicode"
    "errors"
    
    //"encoding/gob"
    //"bytes"
    //"reflect"
)


type Query struct {
	Type 		string
	TableName 	string
	Fields 		[]string
	Values 		[]string
	Set_Fields 	[]string
	Set_Values 	[]string
}


func qlSelect(req *QLSelect) string {
	var output string
	var query Query

	if req.Output[0].Str[0] == '*' {
		query.Fields = append(query.Fields, "Id")
		query.Fields = append(query.Fields, "UserName")
		query.Fields = append(query.Fields, "Login")
		query.Fields = append(query.Fields, "Pswd")
		query.Fields = append(query.Fields, "UserRole")
	} else {
		for _, val := range req.Output {
			query.Fields = append(query.Fields, string(val.Str))
		}
		/*query.Fields[0] = "Id"
		query.Fields = append(query.Fields, "UserName")
		query.Fields = append(query.Fields, "Login")
		query.Fields = append(query.Fields, "Pswd")
		query.Fields = append(query.Fields, "UserRole")*/
	}
	
	// Find Table
	var TableNum int
	for i, val := range tst_db.Tables {
		if val.Name == req.Table {
		 TableNum = i
		 break
		} else {
			output += "Error: Table " + req.Table + " not found\n"
			return output
		}
	}
	
	
	
	
		var num int
		for i := 0; i < len(tst_db.Tables[TableNum].Rows); i++{
		for _, val := range req.Output{
			num = findIndex(tst_db.Tables[TableNum].Cols, string(val.Str))
			if num < 0 {
				output = "Don't find column " + string(val.Str) + "\n"
				return output
			}
			output += tst_db.Tables[TableNum].Rows[i][num] + " "
			
			/*switch val {
			case "Id":
				num = findIndex(tst_db.Tables[TableNum].Cols, "Id")
				output += string(tst_db.Tables[TableNum].Rows[i][num]) + " "
			case "UserName":
				num = findIndex(tst_db.Tables[TableNum].Cols, "UserName")
				output += string(tst_db.Tables[TableNum].Rows[i][num]) + " "
			//output += string(tst_db.Tables[TableNum].Rows[i].UserName) + " "
			case "Login":
				num = findIndex(tst_db.Tables[TableNum].Cols, "Login")
				output += string(tst_db.Tables[TableNum].Rows[i][num]) + " "
    			//output += string(tst_db.Tables[TableNum].Rows[i].Login) + " "
    			case "Pswd":
    				num = findIndex(tst_db.Tables[TableNum].Cols, "Pswd")
				output += string(tst_db.Tables[TableNum].Rows[i][num]) + " "
    			//output += string(tst_db.Tables[TableNum].Rows[i].Pswd) + " " 
    			case "UserRole":
    				num = findIndex(tst_db.Tables[TableNum].Cols, "UserRole")
				output += string(tst_db.Tables[TableNum].Rows[i][num]) + " "
    			//output += strconv.Itoa(tst_db.Tables[TableNum].Rows[i].UserRole) + " " 
    			default:
			fmt.Println("Don't find column ", val)
			}*/
		}
		if len(output) > 0 {output += "\n"}
		}
		return output
}


func findIndex(Cols []string, Name string) int {
	//var Num int
	for i, val := range Cols {
		if val == Name {
			return i
		}
	}
	return -1
}

func findTable(Tables []TableDef, Name string) int {
	for i, val := range tst_db.Tables {
		if val.Name == Name {
			return i
		} 
	}
	return -1
}


func qlUpdate(req *QLUpdate) string {
	var output string
	//var query Query

		/*var Id_num int = -1
    			var UserName_num int = -1
    			var Login_num int = -1
    			var Pswd_num int = -1
    			var UserRole_num int = -1
    		for i, val := range req.Names {
    			switch val {
    			case "Id": Id_num = i
    			case "UserName": UserName_num = i
    			case "Login": Login_num = i
    			case "Pswd": Pswd_num = i
    			case "UserRole": UserRole_num = i
    			} 
    		}*/
    		
    		var Id_num_set int = -1
    			var UserName_num_set int = -1
    			var Login_num_set int = -1
    			var Pswd_num_set int = -1
    			var UserRole_num_set int = -1
    		for i, val := range req.Names {
    			switch val {
    			case "Id": Id_num_set = i
    			case "UserName": UserName_num_set = i
    			case "Login": Login_num_set = i
    			case "Pswd": Pswd_num_set = i
    			case "UserRole": UserRole_num_set = i
    			} 
    		}
    		
    		
    	// Find Table
	var TableNum int
	for i, val := range tst_db.Tables {
		if val.Name == req.Table {
		 TableNum = i
		 break
		} else {
			output += "Error: Table " + req.Table + " not found\n"
			return output
		}
	}
    		
    		
    		
    		
    		
    		//var counters []int
		for i := range tst_db.Tables[TableNum].Rows {		
			//var Id int
			//var UserRole int
			
			/*if Id_num >= 0 {Id, _ = strconv.Atoi(query.Values[Id_num])}
    			if Id_num >= 0 && tst_db.Tables[TableNum].Rows[i].Id != Id {
        			continue
    			}
    			if UserName_num >= 0 && tst_db.Tables[TableNum].Rows[i].UserName != query.Values[UserName_num] {
        			continue
    			}
    			if Login_num >= 0 && tst_db.Tables[TableNum].Rows[i].Login != query.Values[Login_num] {
        			continue
    			}
    			if Pswd_num >= 0 && tst_db.Tables[TableNum].Rows[i].Pswd != query.Values[Pswd_num] {
        			continue
    			}
    			if UserRole_num >= 0 {UserRole, _ = strconv.Atoi(query.Values[UserRole_num])}
    			if UserRole_num >= 0 && tst_db.Tables[TableNum].Rows[i].UserRole != UserRole {
        			continue
    			}*/
i=i    			
  			if Id_num_set >= 0 {
//    			tst_db.Tables[TableNum].Rows[i].Id, _ = strconv.Atoi(string(req.Values[Id_num_set].Str))
    			}
    			if UserName_num_set >= 0 {
//    			tst_db.Tables[TableNum].Rows[i].UserName = string(req.Values[UserName_num_set].Str)
    			}
    			if Login_num_set >= 0 {
//    			tst_db.Tables[TableNum].Rows[i].Login = string(req.Values[Login_num_set].Str)
    			}
    			if Pswd_num_set >= 0 {
//    			tst_db.Tables[TableNum].Rows[i].Pswd = string(req.Values[Pswd_num_set].Str)
    			}
    			if UserRole_num_set >= 0 {
//    			tst_db.Tables[TableNum].Rows[i].UserRole, _ = strconv.Atoi(string(req.Values[UserRole_num_set].Str))
    			}
   			
//    			output += "Updated OK: " + tst_db.Tables[TableNum].Rows[i].UserName + "\n"
    			//counters = append(counters, i)
		}
		return output
}

func qlEval(ctx *QLEvalContex, node QLNode) {
	if ctx.err != nil {
		return
	}
	switch node.Type {
	// refer to a column
	case QL_SYM:
		/*if v := ctx.env.Get(string(node.Str)); v != nil {
			ctx.out = *v
		} else {
			qlErr(ctx, "unknown column: %s", node.Str)
		}*/
		
		//-------QL_STR = TYPE_BYTES
		//-------QL_I64 = TYPE_INT64
		ctx.out.Type = QL_STR 
		ctx.out.Str = node.Str
	case QL_STR:
		ctx.out.Type = QL_STR 
		ctx.out.Str = node.Str
	case QL_I64:
		ctx.out.Type = QL_I64
		ctx.out = node.Value
	// unary ops
	case QL_NEG:
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			ctx.out.I64 = -ctx.out.I64
		} else {
			qlErr(ctx, "QL_NEG type error")
		}
	

	// binary ops
	case QL_CMP_GE: // >=
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_OR type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_OR type error")
		}
		ctx.out.I64 = Btoi(add1 >= add2)
	case QL_CMP_GT:// >
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_OR type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_OR type error")
		}
		ctx.out.I64 = Btoi(add1 > add2)
	case QL_CMP_LT:// < 
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_OR type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_OR type error")
		}
		ctx.out.I64 = Btoi(add1 < add2)
	case QL_CMP_EQ: // = 
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_OR type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_OR type error")
		}
		ctx.out.I64 = Btoi(add1 == add2)
	case QL_CMP_LE:// <= 
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_OR type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_OR type error")
		}
		ctx.out.I64 = Btoi(add1 <= add2)

	// unary ops
	case QL_NOT:
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			if ctx.out.I64 == 0 {
    				ctx.out.I64 = 1
			} else {
    				ctx.out.I64 = 0
			}
		} else {
			qlErr(ctx, "QL_NEG type error")
		}
	// others
	case QL_TUP: // tuple
	case QL_ERR: // error; from parsing or evaluation
	
	case QL_OR:
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_OR type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_OR type error")
		}
		ctx.out.I64 = Btoi(Itob(add1) || Itob(add2))
	case QL_AND:
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_AND type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_AND type error")
		}
		ctx.out.I64 = Btoi(Itob(add1) && Itob(add2))
	case QL_MUL:
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_MUL type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_MUL type error")
		}
		ctx.out.I64 = add1 * add2
	case QL_DIV:
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_DIV type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_DIV type error")
		}
		ctx.out.I64 = add1 / add2
	case QL_MOD:
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_MOD type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_MOD type error")
		}
		ctx.out.I64 = add1 % add2
	
	case QL_ADD:
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_ADD type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_ADD type error")
		}
		ctx.out.I64 = add1 + add2
	case QL_SUB:
		var add1 int
		var add2 int
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			add1 = ctx.out.I64
		} else {
			qlErr(ctx, " left QL_SUB type error")
		}
		qlEval(ctx, node.Kids[1])
		if ctx.out.Type == TYPE_INT64 {
			add2 = ctx.out.I64
		} else {
			qlErr(ctx, "right QL_SUB type error")
		}
		ctx.out.I64 = add1 - add2
	default:
		panic("not implemented")
	}
}

func qlErr(ctx *QLEvalContex, err string){
	ctx.err = errors.New(err)
}


func Btoi(b bool) int {
    if b {
        return 1
    }
    return 0
 }
 
 func Itob(b int) bool {
    if b != 0 {
        return true
    }
    return false
 }
 
func clear(ctx *QLEvalContex) {
    ctx.out.Type = 0
    ctx.out.I64 = 0
    ctx.out.Str = []byte("")
    ctx.err = nil
}




func qlInsert(req *QLInsert) string {
	var output string
	
	
	
	// Find Table
	var TableNum int
	for i, val := range tst_db.Tables {
		if val.Name == req.Table {
		 TableNum = i
		 break
		} else {
			output += "Error: Table " + req.Table + " not found\n"
			return output
		}
	}
	
	
	
	
	//tst_db.Tables[TableNum].Rows = append(tst_db.Tables[TableNum].Rows, Row{Id: 0, UserName: "", Login: "", Pswd: "", UserRole: 0})
	tst_db.Tables[TableNum].Rows = append(tst_db.Tables[TableNum].Rows, []string{""})
	/*for i, node := range req.Values {
    		var ctx QLEvalContex
    		qlEval(&ctx, node)
    		if ctx.err != nil {
    			fmt.Println(ctx.err)
    		} else {
    			fmt.Println(ctx.out)
    			tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1].Id, _ = strconv.Atoi(string(req.Values[0].Str))
    		}
    	}*/
    	var ctx QLEvalContex
    	
    	for i, value := range req.Values {
    		qlEval(&ctx, value)
    		if ctx.err != nil {
    			fmt.Println(ctx.err)
    		} else {
    			switch value.Type {
    			case TYPE_INT64: 
    				//fmt.Println("int64" + strconv.Itoa(ctx.out.I64))
    				if i == 0 {
    					tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1][0] = strconv.Itoa(ctx.out.I64)
    				} else {
    					tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1] = append(tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1], strconv.Itoa(ctx.out.I64))
    				}
    			case QL_SYM: 
    				//fmt.Println("bytes" + string(ctx.out.Str))
    				if i == 0 {
    					tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1][0] = string(ctx.out.Str)
    				} else {
    					tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1] = append(tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1], string(ctx.out.Str))
    				}
    			}
    		}
    		clear(&ctx)
    	}
    	output = "Inserted: OK" + "\n"
    	//fmt.Println(tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1])
    	
    	
/*    	qlEval(&ctx, req.Values[0])
    	fmt.Println("test 1")
   	
    	if ctx.err != nil {
    		fmt.Println(ctx.err)
    	} else {
    		tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1] = append(tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1], string(ctx.out.I64))
    		//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1][0] = string(ctx.out.I64)//.Id = ctx.out.I64
    	}
    	clear(&ctx)
	//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1].Id, _ = strconv.Atoi(string(req.Values[0].Str))
	qlEval(&ctx, req.Values[1])
	
	fmt.Println("test 2")
	
    	if ctx.err != nil {
    		fmt.Println(ctx.err)
    	} else {
    		tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1] = append(tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1], string(ctx.out.Str))
    		//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1][1] = string(ctx.out.Str)//.UserName = string(ctx.out.Str)
    	}
    	clear(&ctx)
   	
    	//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1].UserName = string(req.Values[1].Str)
    	qlEval(&ctx, req.Values[2])
    	
    	fmt.Println("test 3")
    	
    	if ctx.err != nil {
    		fmt.Println(ctx.err)
    	} else {
    		tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1] = append(tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1], string(ctx.out.Str))
    		//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1][2] = string(ctx.out.Str)//.Login = string(ctx.out.Str)
    	}
    	clear(&ctx)

    	//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1].Login = string(req.Values[2].Str)
    	qlEval(&ctx, req.Values[3])
    	
    	fmt.Println("test 4")
    	
    	if ctx.err != nil {
    		fmt.Println(ctx.err)
    	} else {
    		tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1] = append(tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1], string(ctx.out.Str))
    		//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1][3] = string(ctx.out.Str)//.Pswd = string(ctx.out.Str)
    	}
    	clear(&ctx)
    	//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1].Pswd = string(req.Values[3].Str)
    	qlEval(&ctx, req.Values[4])
    	
    	fmt.Println("test 5")
    	
    	if ctx.err != nil {
    		fmt.Println(ctx.err)
    	} else {
    		tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1] = append(tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1], string(ctx.out.I64))
    		//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1][4] = string(ctx.out.I64)//.UserRole = ctx.out.I64
    	}
    	clear(&ctx)
    	//tst_db.Tables[TableNum].Rows[len(tst_db.Tables[TableNum].Rows)-1].UserRole, _ = strconv.Atoi(string(req.Values[4].Str))
    	output = "Inserted: OK" + "\n"
*/    	
    	//fmt.Println(req)

    	
    	
    	
 /*   	
    type QLInsert
	//Table string
	//Mode int
	//Names []string
	Values []QLNode
	
    type QLNode
	Value // Type, I64, Str
	Kids []QLNode
	
    type Value
	Type uint32
	I64 int
	//Str []byte

*/
	return output
}

func qlDelete(req *QLDelete) string {
	var output string
	
			/*var Id_num int = -1
    			var UserName_num int = -1
    			var Login_num int = -1
    			var Pswd_num int = -1
    			var UserRole_num int = -1
    		for i, val := range query.Fields {
    			switch val {
    			case "Id": Id_num = i
    			case "UserName": UserName_num = i
    			case "Login": Login_num = i
    			case "Pswd": Pswd_num = i
    			case "UserRole": UserRole_num = i
    			} 
    		}*/
    		
    		
    	// Find Table
	var TableNum int
	for i, val := range tst_db.Tables {
		if val.Name == req.Table {
		 TableNum = i
		 break
		} else {
			output += "Error: Table " + req.Table + " not found\n"
			return output
		}
	}
    		
    		
    		
    		
    		
    		var counters []int
		for i := range tst_db.Tables[TableNum].Rows {		
//			var Id int
//			var UserRole int
counters = append(counters, i)			
/*			if Id_num >= 0 {Id, _ = strconv.Atoi(query.Values[Id_num])}
    			if Id_num >= 0 && tst_db.Tables[0].Rows[i].Id == Id {
    				counters = append(counters, i)
        			continue
    			}
    			if UserName_num >= 0 && tst_db.Tables[0].Rows[i].UserName == query.Values[UserName_num] {
        			counters = append(counters, i)
        			continue
    			}
    			if Login_num >= 0 && tst_db.Tables[0].Rows[i].Login == query.Values[Login_num] {
        			counters = append(counters, i)
        			continue
    			}
    			if Pswd_num >= 0 && tst_db.Tables[0].Rows[i].Pswd == query.Values[Pswd_num] {
        			counters = append(counters, i)
        			continue
    			}
    			if UserRole_num >= 0 {UserRole, _ = strconv.Atoi(query.Values[UserRole_num])}
    			if UserRole_num >= 0 && tst_db.Tables[0].Rows[i].UserRole == UserRole {
        			counters = append(counters, i)
        			continue
    			}*/
		}
		
		
		for j := len(counters)-1; j >= 0; j-- {
			i := counters[j]
////////////			output += "Deleted OK: " + tst_db.Tables[TableNum].Rows[i].UserName + "\n"
			tst_db.Tables[TableNum].Rows = append(tst_db.Tables[TableNum].Rows[:i], tst_db.Tables[TableNum].Rows[i+1:]...)
        	}

	return output
}
