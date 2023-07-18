package main

import (
    "fmt"
    //"strings"
    "strconv"
    "os"
    //"bufio"
    "encoding/binary"
    
    //"unicode"
    "errors"
    
    "encoding/gob"
    "bytes"
    //"reflect"
)


func cmd_lstbl(words []string) string {
	var output string
	
	for _, val := range tst_db.Tables {
		output += ": " + val.Name + "\n"
	}

	
	/*output += tst_db.Name + "\n" 
	output += tst_db.File + "\n" 
	output += tst_db.Tables[0].Name + "\n" 
	output += tst_db.Tables[0].Cols[4] + "\n"
	output += tst_db.Tables[0].Rows[0][1] + "\n"*/
	return output 
}


func cmd_vtbl(words []string) string {
	var output string
	
	num := findTable(tst_db.Tables, words[1])
	if num < 0 {
		output = "Table not found\n"
		return output 
	}
	
	for i := 0; i < len(tst_db.Tables[num].Cols); i++ {
		output += tst_db.Tables[num].Cols[i] + ": " + strconv.Itoa(tst_db.Tables[num].Types[i]) + "\n"
	}
	
	return output 
}


func cmd_alter(words []string) string {
	var output string
	
	var p Parser
	
	p.input = []byte(words[0])
	for i := 1; i < len(words); i++  {
		p.input = append(p.input, []byte(" ")...)
		p.input = append(p.input, []byte(words[i])...)
	}
	
	outStmt := pStmt(&p)
	if p.err != nil {
    		fmt.Println(p.err)
  	} 
	
	var t *TableDef = outStmt.(*TableDef)
	
	/*for i, val := t.Cols {
		val
	}*/
	
	num := findTable(tst_db.Tables, t.Name)
	
	if t.Types[0] < 0 {
		col := findIndex(tst_db.Tables[num].Cols, t.Cols[0])
		tst_db.Tables[num].Cols = append(tst_db.Tables[num].Cols[:col], tst_db.Tables[num].Cols[col+1:]...)
		tst_db.Tables[num].Types = append(tst_db.Tables[num].Types[:col], tst_db.Tables[num].Types[col+1:]...)
	
		output = "Column '" + t.Cols[0] + "' deleted from '" + t.Name + "' table\n"
	} else {
		//col = findIndex(tst_db.Tables[num].Cols, t.Cols[0])
		tst_db.Tables[num].Cols = append(tst_db.Tables[num].Cols, t.Cols[0])
		tst_db.Tables[num].Types = append(tst_db.Tables[num].Types, t.Types[0])
	
		output = "Column '" + t.Cols[0] + "' add to '" + t.Name + "' table\n"
	}
	return output 
}


func cmd_drop(words []string) string {
	var output string
	
	var p Parser
	
	p.input = []byte(words[0])
	for i := 1; i < len(words); i++  {
		p.input = append(p.input, []byte(" ")...)
		p.input = append(p.input, []byte(words[i])...)
	}
	
	outStmt := pStmt(&p)
	if p.err != nil {
    		fmt.Println(p.err)
  	} 
	
	var t *TableDef = outStmt.(*TableDef)
	
	num := findTable(tst_db.Tables, t.Name)
	tst_db.Tables = append(tst_db.Tables[:num], tst_db.Tables[num+1:]...)
	
	output = "Table '" + t.Name + "' deleted\n"
	return output 
}


func cmd_create(words []string) string {
	var output string
	
	var p Parser
	
	p.input = []byte(words[0])
	for i := 1; i < len(words); i++  {
		p.input = append(p.input, []byte(" ")...)
		p.input = append(p.input, []byte(words[i])...)
	}
	
	outStmt := pStmt(&p)
	if p.err != nil {
    		fmt.Println(p.err)
  	} 
	
	
	var t *TableDef = outStmt.(*TableDef)
	//output = qlDelete(t)
	
	tst_db.Tables = append(tst_db.Tables, *t)
	
	output = "Table '" + tst_db.Tables[len(tst_db.Tables)-1].Name + "' created\n"
	
	//fmt.Println(tst_db.Tables[len(tst_db.Tables)-1])
	
	return output 
}


func cmd_save(words []string) string {
	var output string
		
	err := buildData(tst_db.File, tst_db)
	if err == nil {
                output = "Saved OK\n"
        } else {
       		output = "Saved FAIL\n"
        }
	
	return output
}



func cmd_load(words []string) string {
	var output string
    	var buff bytes.Buffer
	
	file, err := os.Open(tst_db.File)
    	if err != nil {
        	output = "Load FAIL\n"
        	return output
    	}
    	defer file.Close()
    	
	buff.ReadFrom(file)
    	if err != nil {
        	output = "Load FAIL\n"
        	return output
    	}

	dec := gob.NewDecoder(&buff)
	dec.Decode(&tst_db)
	
	//fmt.Printf("%X\n", buff.Bytes())
    	//fmt.Println(tst_db.Tables[0].Rows[0].Login)
 
	return output
}


func buildData(filename string, data DB) error {

        f, err := os.Create(filename)
        if err != nil {
                return err
        }
        defer f.Close()
       		
       	var buff bytes.Buffer
       	enc := gob.NewEncoder(&buff)
       	enc.Encode(data)
	//fmt.Printf("%X\n", buff.Bytes())

       	err = binary.Write(f, binary.BigEndian, buff.Bytes())
    	if err != nil {
        	return errors.New("Save FAIL")
    	}
        return nil
}



func cmd_select(words []string) string {
	var output string

	var p Parser
	
	//copy(words[0:], words[1:])
	p.input = []byte(words[0])
	
	for i := 1; i < len(words); i++  {
		p.input = append(p.input, []byte(" ")...)
		p.input = append(p.input, []byte(words[i])...)
	}
	
	outStmt := pStmt(&p)
	if p.err != nil {
    		fmt.Println(p.err)
  	} 
	
	//fmt.Println(outStmt)
	
	var t *QLSelect = outStmt.(*QLSelect)
	output = qlSelect(t)
	
	return output
}


// UPDATE table_name
// SET column1 = value1, column2 = value2, ...
// WHERE condition;
func cmd_update(words []string) string {
	var output string

	var p Parser
	
	p.input = []byte(words[0])
	for i := 1; i < len(words); i++  {
		p.input = append(p.input, []byte(" ")...)
		p.input = append(p.input, []byte(words[i])...)
	}
	
	outStmt := pStmt(&p)
	if p.err != nil {
    		fmt.Println(p.err)
  	} 
	
	//fmt.Println(outStmt)
	
	var t *QLUpdate = outStmt.(*QLUpdate)
	output = qlUpdate(t)
	
	return output
}


// INSERT INTO table_name (column1, column2, column3, ...)
// VALUES (value1, value2, value3, ...);
//
// INSERT INTO table_name
// VALUES (value1, value2, value3, ...);
func cmd_insert(words []string) string {
	var output string

	var p Parser
	
	p.input = []byte(words[0])
	for i := 1; i < len(words); i++  {
		p.input = append(p.input, []byte(" ")...)
		p.input = append(p.input, []byte(words[i])...)
	}
	
	outStmt := pStmt(&p)
	if p.err != nil {
    		fmt.Println(p.err)
  	} 
	
	//fmt.Println(outStmt)
	
	var t *QLInsert = outStmt.(*QLInsert)
	output = qlInsert(t)
	
	return output
}

// DELETE FROM table_name WHERE condition;
func cmd_delete(words []string) string {
	var output string

	var p Parser
	
	p.input = []byte(words[0])
	for i := 1; i < len(words); i++  {
		p.input = append(p.input, []byte(" ")...)
		p.input = append(p.input, []byte(words[i])...)
	}
	
	outStmt := pStmt(&p)
	if p.err != nil {
    		fmt.Println(p.err)
  	} 
	
	//fmt.Println(outStmt)
	
	var t *QLDelete = outStmt.(*QLDelete)
	output = qlDelete(t)
	
	return output
}
