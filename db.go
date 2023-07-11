package main

import (
    "fmt"
    "strings"
    "strconv"
    "os"
    "bufio"
    "encoding/binary"
    //"github.com/stretchr/testify/assert"
    //"github.com/marianogappa/sqlparser"
)


type BNode struct {
	data []byte // can be dumped to the disk
}


const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)


type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64) // deallocate a page
}


const HEADER = 4
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

/*func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	//assert(node1max <= BTREE_PAGE_SIZE)
}*/

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}


// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	//assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	//assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	//assert(1 <= idx && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	//assert(idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	//assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	//assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}




type UserFromDB struct {
    Id int
    UserName string
    Login string
    Pswd string
    UserRole int
}

var users_table []UserFromDB



const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// table cell
type Value struct {
	Type uint32
	I64 int64
	Str []byte
}

// syntax tree
type QLNode struct {
	Value // Type, I64, Str
	Kids []QLNode
}

// syntax tree node types
const (
	QL_UNINIT = 0
// scalar
	QL_STR = TYPE_BYTES
	QL_I64 = TYPE_INT64
// binary ops
	QL_CMP_GE = 10 // >=
	QL_CMP_GT = 11 // >
// more operators; omitted...
// unary ops
	QL_NOT = 50
	QL_NEG = 51
// others
	QL_SYM = 100 // column
	QL_TUP = 101 // tuple
	QL_ERR = 200 // error; from parsing or evaluation
)


type Query struct {
	Type string
	TableName string
	Fields []string
	Values []string
	Set_Fields []string
	Set_Values []string
}


// SELECT * FROM table_name;
// SELECT column1, column2, ...
// FROM table_name;
func cmd_select(words []string) string {
	var output string
	var query Query
	var i int = 1
	//fmt.Println(words)
	query.Type = "select"
	
	
	for ; words[i] != "from"; i++ {
		query.Fields = append(query.Fields, words[i])
	}
	
	if words[i] == "from" {
		query.TableName = words[i+1]
	}
	

	cmd_load(words)
	output = exec(query)
	return output 
}


// INSERT INTO table_name (column1, column2, column3, ...)
// VALUES (value1, value2, value3, ...);
//
// INSERT INTO table_name
// VALUES (value1, value2, value3, ...);
func cmd_insert(words []string) string {
	var output string
	var query Query
	var i int = 1

	query.Type = "insert"
	
	if words[i] == "into" {i++}
	query.TableName = words[i]
	i++
	
	//fmt.Println(words)
	
	if words[i] == "values" {
		i++
		for ; i < len(words); i++ {
			query.Values = append(query.Values, words[i])
		}	
	}
	cmd_load(words)
	output = exec(query)	
	cmd_save(words)
	return output 
}


func exec(query Query) string {
	var output string
	
	switch query.Type {
	case "select":
		if query.Fields[0] == "*" {
			query.Fields[0] = "Id"
			query.Fields = append(query.Fields, "UserName")
			query.Fields = append(query.Fields, "Login")
			query.Fields = append(query.Fields, "Pswd")
			query.Fields = append(query.Fields, "UserRole")
		} 
		for i := 0; i < len(users_table); i++{
		for _, val := range query.Fields{
			switch val {
			case "Id":
			output += strconv.Itoa(users_table[i].Id) + " "
			case "UserName":
			output += string(users_table[i].UserName) + " "
			case "Login":
    			output += string(users_table[i].Login) + " "
    			case "Pswd":
    			output += string(users_table[i].Pswd) + " " 
    			case "UserRole":
    			output += strconv.Itoa(users_table[i].UserRole) + " " 
    			default:
			fmt.Println("Don't find column ", val)
			}
		}
		if len(output) > 0 {output += "\n"}
		}
		
		//if len(output) > 0 {output += "\n"}
		//fmt.Println(output)
	case "insert":
		//fmt.Println("insert")
		users_table = append(users_table, UserFromDB{Id: 0, UserName: "", Login: "", Pswd: "", UserRole: 0})
		users_table[len(users_table)-1].Id, _ = strconv.Atoi(query.Values[0])
    		users_table[len(users_table)-1].UserName = query.Values[1]
    		users_table[len(users_table)-1].Login = query.Values[2]
    		users_table[len(users_table)-1].Pswd = query.Values[3]
    		users_table[len(users_table)-1].UserRole, _ = strconv.Atoi(query.Values[4])
    		output = "Inserted: OK" + "\n"
    		//fmt.Println(users_table)
    		//fmt.Println(users_table)
    	case "delete":
    			var Id_num int = -1
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
    		}
    		
    		var counters []int
		for i := range users_table {		
			var Id int
			var UserRole int
			
			if Id_num >= 0 {Id, _ = strconv.Atoi(query.Values[Id_num])}
    			if Id_num >= 0 && users_table[i].Id == Id {
    				counters = append(counters, i)
        			continue
    			}
    			if UserName_num >= 0 && users_table[i].UserName == query.Values[UserName_num] {
        			counters = append(counters, i)
        			continue
    			}
    			if Login_num >= 0 && users_table[i].Login == query.Values[Login_num] {
        			counters = append(counters, i)
        			continue
    			}
    			if Pswd_num >= 0 && users_table[i].Pswd == query.Values[Pswd_num] {
        			counters = append(counters, i)
        			continue
    			}
    			if UserRole_num >= 0 {UserRole, _ = strconv.Atoi(query.Values[UserRole_num])}
    			if UserRole_num >= 0 && users_table[i].UserRole == UserRole {
        			counters = append(counters, i)
        			continue
    			}
		}
		
		
		for j := len(counters)-1; j >= 0; j-- {
			i := counters[j]
			output += "Deleted OK: " + users_table[i].UserName + "\n"
			users_table = append(users_table[:i], users_table[i+1:]...)
        	}
        case "update":
        		var Id_num int = -1
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
    		}
    		
    		var Id_num_set int = -1
    			var UserName_num_set int = -1
    			var Login_num_set int = -1
    			var Pswd_num_set int = -1
    			var UserRole_num_set int = -1
    		for i, val := range query.Set_Fields {
    			switch val {
    			case "Id": Id_num_set = i
    			case "UserName": UserName_num_set = i
    			case "Login": Login_num_set = i
    			case "Pswd": Pswd_num_set = i
    			case "UserRole": UserRole_num_set = i
    			} 
    		}
    		
    		//var counters []int
		for i := range users_table {		
			var Id int
			var UserRole int
			
			if Id_num >= 0 {Id, _ = strconv.Atoi(query.Values[Id_num])}
    			if Id_num >= 0 && users_table[i].Id != Id {
        			continue
    			}
    			if UserName_num >= 0 && users_table[i].UserName != query.Values[UserName_num] {
        			continue
    			}
    			if Login_num >= 0 && users_table[i].Login != query.Values[Login_num] {
        			continue
    			}
    			if Pswd_num >= 0 && users_table[i].Pswd != query.Values[Pswd_num] {
        			continue
    			}
    			if UserRole_num >= 0 {UserRole, _ = strconv.Atoi(query.Values[UserRole_num])}
    			if UserRole_num >= 0 && users_table[i].UserRole != UserRole {
        			continue
    			}
    			
    			if Id_num_set >= 0 {users_table[i].Id, _ = strconv.Atoi(query.Set_Values[Id_num_set])}
    			if UserName_num_set >= 0 {users_table[i].UserName = query.Set_Values[UserName_num_set]}
    			if Login_num_set >= 0 {users_table[i].Login = query.Set_Values[Login_num_set]}
    			if Pswd_num_set >= 0 {users_table[i].Pswd = query.Set_Values[Pswd_num_set]}
    			if UserRole_num_set >= 0 {users_table[i].UserRole, _ = strconv.Atoi(query.Set_Values[UserRole_num_set])}
    			
    			output += "Updated OK: " + users_table[i].UserName + "\n"
    			//counters = append(counters, i)
		}
		
		
		/*for j := len(counters)-1; j >= 0; j-- {
			i := counters[j]
			output += "Updated OK: " + users_table[i].UserName + "\n"
			
			
			
			
			
			
			//users_table = append(users_table[:i], users_table[i+1:]...)
        	}*/
	default:
		fmt.Println("Unrecognized type")
		return output
	}

	return  output
}


func init_db() {
	var words []string
	//users_table[0] = UserFromDB{Id: 0, UserName: "admin", Login: "admin", Pswd: "pass", UserRole: 1}
	//users_table = append(users_table, UserFromDB{Id: 0, UserName: "admin", Login: "admin", Pswd: "pass", UserRole: 1})
	//fmt.Println(users_table)
	cmd_load(words)
}


// UPDATE table_name
// SET column1 = value1, column2 = value2, ...
// WHERE condition;
func cmd_update(words []string) string {
	var output string
	var query Query
	var i int = 1
	
	query.Type = "update"
	query.TableName = words[i]
	i++
	
	if words[i] == "set" {
		i++
		for ; i < len(words); i++ {
			if words[i] == "where" {break}
			
			query.Set_Fields = append(query.Set_Fields, strings.SplitAfter(words[i], "=")[0][:len(strings.SplitAfter(words[i], "=")[0])-1])
			query.Set_Values = append(query.Set_Values, strings.SplitAfter(words[i], "=")[1])
		}
	}
	
		i++
		for ; i < len(words); i++ {
			query.Fields = append(query.Fields, strings.SplitAfter(words[i], "=")[0][:len(strings.SplitAfter(words[i], "=")[0])-1])
			query.Values = append(query.Values, strings.SplitAfter(words[i], "=")[1])
		}

	cmd_load(words)
	output = exec(query)	
	cmd_save(words)
	return output 
}


// DELETE FROM table_name WHERE condition;
func cmd_delete(words []string) string {
	var output string
	var query Query
	var i int = 1
	
	//fmt.Println(words)
	query.Type = "delete"
	
	if words[i] == "from" {
		query.TableName = words[i+1]
		i += 2
	}
	
	if words[i] == "where" {
		i++
		for ; i < len(words); i++ {
			query.Fields = append(query.Fields, strings.SplitAfter(words[i], "=")[0][:len(strings.SplitAfter(words[i], "=")[0])-1])
			query.Values = append(query.Values, strings.SplitAfter(words[i], "=")[1])
		}
	}

	cmd_load(words)
	output = exec(query)	
	cmd_save(words)
	return output 
}


func create_table(words []string) string {
	var output string
	
	output = "Test"
	/*query, err := sqlparser.Parse("SELECT a, b, c FROM 'd' WHERE e = '1' AND f > '2'")
	if err != nil {
		//log.Fatal(err)
		fmt.Printf("Fatal err")
	}
	fmt.Printf("%+#v", query)*/
	
	return output 
}

/*
func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(data)
	return err
}*/


func cmd_save(words []string) string {
	var output string
	
	err := buildData("files/base.db", users_table)
	if err == nil {
                output = "Saved OK\n"
        } else {
       		output = "Saved FAIL\n"
        }
	
	return output
}



func cmd_load(words []string) string {
	var output string
	var line string
	var values = make([]string, 0)
	
	file, err := os.Open("files/base.db")
    	if err != nil {
        	output = "Saved FAIL\n"
        	return output
    	}
    	defer file.Close()
    	
    	scanner := bufio.NewScanner(file)
    	users_table = nil
    	for scanner.Scan() {
    		line = scanner.Text()
    		values = strings.Fields(line)
    		
    		for i, _ := range values {
       			values[i] = strings.ReplaceAll(values[i], "{", "")
    			values[i] = strings.ReplaceAll(values[i], "}", "")
    		}
    		
    		//values[0] = strings.ReplaceAll(values[0], "{", "")
    		//values[4] = strings.ReplaceAll(values[4], "}", "")
    		
		Id, _ := strconv.Atoi(values[0])
		UserRole, _ := strconv.Atoi(values[4])
				
		bk := UserFromDB{Id: Id, UserName: values[1], Login: values[2], Pswd: values[3], UserRole: UserRole}

        	users_table = append(users_table, bk)
    	}
    	
 

	return output
}


func buildData(filename string, data []UserFromDB) error {

        f, err := os.Create(filename)
        if err != nil {
                return err
        }
        defer f.Close()
        
        
        for _, value := range data {
       		fmt.Fprintln(f, value)  // print values to f, one per line
    	}
        
        
        /*for _, v := range data {
                if _, err := f.WriteString(string(v)); err != nil {
                        return err
                }
        }*/
        return nil
}

