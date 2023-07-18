package main

import (
    //"fmt"
    "strings"
    "strconv"
    //"os"
    //"bufio"
    //"encoding/binary"
    
    "unicode"
    "errors"
    
    //"encoding/gob"
    //"bytes"
    //"reflect"
)


const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)


// table cell
type Value struct {
	Type uint32
	I64 int
	Str []byte
}

// modes of the updates
const (
	MODE_UPSERT = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

type Parser struct {
	input []byte
	idx int
	err error
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
	QL_CMP_LT = 12 // < 
	QL_CMP_EQ = 13 // = 
	QL_CMP_LE = 14 // <= 
	// more operators; omitted...
	// unary ops
	QL_NOT = 50
	QL_NEG = 51
	// others
	QL_SYM = 100 // column
	QL_TUP = 101 // tuple
	QL_ERR = 200 // error; from parsing or evaluation
	
	QL_OR =  221
	QL_AND = 222
	
	QL_MUL = 201
	QL_DIV = 202
	QL_MOD = 203
	
	QL_ADD = 210
	QL_SUB = 211
)

// common structure for queries: `INDEX BY`, `FILTER`, `LIMIT`
type QLScan struct {
	Table string
	// INDEX BY xxx
	Key1 QLNode // comparison, optional
	Key2 QLNode // comparison, optional
	// FILTER xxx
	Filter QLNode // boolean, optional
	// LIMIT x, y
	Offset int
	Limit int
}

// stmt: select
type QLSelect struct {
	QLScan
	Names []string 	// expr AS name
	Output []QLNode // expression list
}

// stmt: update
type QLUpdate struct {
	QLScan
	Names []string
	Values []QLNode
}
// stmt: insert
type QLInsert struct {
	Table string
	Mode int
	Names []string
	Values []QLNode
	//Values [][]QLNode
}
// stmt: delete
type QLDelete struct {
	QLScan
}
// stmt: create table
/*type QLCreateTable struct {
	TableDef
}*/

// for evaluating expressions
type QLEvalContex struct {
	env Record // optional row values
	out Value
	err error
}

type Record struct {
	Cols []string
	Vals []Value
}


func pStmt(p *Parser) interface{} {
	switch {
	case pKeyword(p, "create", "table"):
		return pCreateTable(p)
	case pKeyword(p, "select"):
		return pSelect(p)
	case pKeyword(p, "insert", "into"):
		return pInsert(p, MODE_INSERT_ONLY)
	case pKeyword(p, "replace", "into"):
		return pInsert(p, MODE_UPDATE_ONLY)
	case pKeyword(p, "upsert", "into"):
		return pInsert(p, MODE_UPSERT)
	case pKeyword(p, "delete", "from"):
		return pDelete(p)
	case pKeyword(p, "update"):
		return pUpdate(p)
	case pKeyword(p, "drop", "table"):
		return pDropTable(p)
	case pKeyword(p, "alter", "table"):
		return pAlterTable(p)
	default:
		pErr(p, nil, "unknown stmt")
		return nil
	}
}


func pAlterTable(p *Parser) *TableDef {
	stmt := TableDef{}
	
	stmt.Name = pMustSym(p)
	
	if pKeyword(p, "drop", "column") {
		stmt.Cols = append(stmt.Cols, pMustSym(p))
		stmt.Types = append(stmt.Types, -1)
		for pKeyword(p, ",") {
			stmt.Cols = append(stmt.Cols, pMustSym(p))
			stmt.Types = append(stmt.Types, -1)
		}	
	} else if pKeyword(p, "add"){
		stmt.Cols = append(stmt.Cols, pMustSym(p))
		if pKeyword(p, "integer") {
			stmt.Types = append(stmt.Types, TYPE_INT64)
		} else if pKeyword(p, "varchar"){
			stmt.Types = append(stmt.Types, TYPE_BYTES)
		} else {
			pErr(p, nil, "expect `INTEGER` or `VARCHAR` " + strconv.Itoa(p.idx))
		}
		for pKeyword(p, ",") {
			stmt.Cols = append(stmt.Cols, pMustSym(p))
			if pKeyword(p, "integer") {
				stmt.Types = append(stmt.Types, TYPE_INT64)
			} else if pKeyword(p, "varchar"){
				stmt.Types = append(stmt.Types, TYPE_BYTES)
			} else {
				pErr(p, nil, "expect `INTEGER` or `VARCHAR` " + strconv.Itoa(p.idx))
			}
		}
	} else {
		pErr(p, nil, "expect `DROP COLUMN` or `ADD` " + strconv.Itoa(p.idx) )
	}
	
	
	if p.err != nil {
		return nil
	}
	return &stmt	
}


func pDropTable(p *Parser) *TableDef {
	stmt := TableDef{}
	
	stmt.Name = pMustSym(p)
	
	if !pKeyword(p, ";") {
		pErr(p, nil, "expect `;` " + strconv.Itoa(p.idx) )
	}

	
	if p.err != nil {
		return nil
	}
	return &stmt	
}


func pCreateTable(p *Parser) *TableDef {
	stmt := TableDef{}
	
	stmt.Name = pMustSym(p)
	
	if !pKeyword(p, "(") {
		pErr(p, nil, "expect `(` " + strconv.Itoa(p.idx) )
	}
	
	
	pCreateExprList(p, &stmt)

	
	
	if !pKeyword(p, ");") {
		pErr(p, nil, "expect `);` " + strconv.Itoa(p.idx) )
	}

	
	if p.err != nil {
		return nil
	}
	return &stmt	
}


func pCreateExprList(p *Parser, stmt *TableDef) {
	stmt.Cols = append(stmt.Cols, pMustSym(p))
	if pKeyword(p, "integer") {
		stmt.Types = append(stmt.Types, TYPE_INT64)
	} else if pKeyword(p, "varchar"){
		stmt.Types = append(stmt.Types, TYPE_BYTES)
	} else {
		pErr(p, nil, "expect `INTEGER` or `VARCHAR` " + strconv.Itoa(p.idx))
	}
	for pKeyword(p, ",") {
		stmt.Cols = append(stmt.Cols, pMustSym(p))
		if pKeyword(p, "integer") {
			stmt.Types = append(stmt.Types, TYPE_INT64)
		} else if pKeyword(p, "varchar"){
			stmt.Types = append(stmt.Types, TYPE_BYTES)
		} else {
			pErr(p, nil, "expect `INTEGER` or `VARCHAR` " + strconv.Itoa(p.idx))
		}
	}
}





func pSelect(p *Parser) *QLSelect {
	stmt := QLSelect{}
		
	// SELECT xxx
	pSelectExprList(p, &stmt)
	// FROM table
	if !pKeyword(p, "from") {
		pErr(p, nil, "expect `FROM` table " + strconv.Itoa(p.idx) )
	}
	stmt.Table = pMustSym(p)
	
	// INDEX BY xxx FILTER yyy LIMIT zzz
//	pScan(p, &stmt.QLScan)
	
	if p.err != nil {
		return nil
	}
	return &stmt
}

func pMustSym(p *Parser) string {
	var input []byte
	
	//fmt.Println("Must ", string(p.input), strconv.Itoa(p.idx))
	
	skipSpace(p)
	//for ; p.input[p.idx] != byte(' ') && p.input[p.idx] != byte(',') && p.input[p.idx] != byte(';') && p.input[p.idx] != byte('=') && p.input[p.idx] != byte(')'); p.idx++ {
	for ; isSym(p.input[p.idx]) || p.input[p.idx] == byte('*') ; p.idx++ {
		input = append(input, byte(p.input[p.idx]))
	}
	return string(input)
}

func pSelectExprList(p *Parser, node *QLSelect) {
	pSelectExpr(p, node)
	for pKeyword(p, ",") {
		pSelectExpr(p, node)
	}
}

func pSelectExpr(p *Parser, node *QLSelect) {
	expr := QLNode{}
//	pExprOr(p, &expr)
	expr.Str = []byte(pMustSym(p)) ////////
	expr.Type = QL_STR ////
	name := ""
	if pKeyword(p, "as") {
		name = pMustSym(p)
	}
	node.Names = append(node.Names, name)
	node.Output = append(node.Output, expr)
}

func pExprOr(p *Parser, node *QLNode) {
	pExprBinop(p, node, []string{"or"}, []uint32{QL_OR}, pExprAnd)
}

func pExprAnd(p *Parser, node *QLNode) {
	pExprBinop(p, node, []string{"and"}, []uint32{QL_AND}, pExprNot)
}
func pExprNot(p *Parser, node *QLNode){ // NOT a
	if pKeyword(p, "not"){
		node.Type = QL_NOT
		node.Kids = []QLNode{{}}
		pExprAtom(p, &node.Kids[0])
	}
	pExprCmp(p, node)
	//pExprBinop(p, node, []string{"not"}, []uint32{QL_NOT}, pExprCmp)
}
func pExprCmp(p *Parser, node *QLNode){ // a < b, ...
	pExprBinop(p, node, []string{"<", ">", "=", "<=", ">="}, []uint32{QL_CMP_LT, QL_CMP_GT, QL_CMP_EQ, QL_CMP_LE, QL_CMP_GE}, pExprAdd)
}
func pExprAdd(p *Parser, node *QLNode){ // a + b, a - b
	pExprBinop(p, node, []string{"+", "-"}, []uint32{QL_ADD, QL_SUB, QL_MOD}, pExprMul)
}

func pExprMul(p *Parser, node *QLNode) {
	pExprBinop(p, node, []string{"*", "/", "%"}, []uint32{QL_MUL, QL_DIV, QL_MOD}, pExprUnop)
}

func pExprUnop(p *Parser, node *QLNode) {
	switch {
	case pKeyword(p, "-"):
		node.Type = QL_NEG
		node.Kids = []QLNode{{}}
		pExprAtom(p, &node.Kids[0])
	default:
		pExprAtom(p, node)
	}
}

func pExprBinop(p *Parser, node *QLNode,ops []string, types []uint32, next func(*Parser, *QLNode)) {
//	assert(len(ops) == len(types))
	left := QLNode{}
	next(p, &left)
	for more := true; more; {
		more = false
		for i := range ops {
			if pKeyword(p, ops[i]) {
				new := QLNode{Value: Value{Type: types[i]}}
				new.Kids = []QLNode{left, {}}
				next(p, &new.Kids[1])
				left = new
				more = true
				break
			}
		}
	}
	*node = left
}

func pExprAtom(p *Parser, node *QLNode) {
	switch {
	/*case pKeyword(p, "("):
		pExprTuple(p, node)
		if !pKeyword(p, ")") {
			pErr(p, node, "unclosed parenthesis")
		}*/
	case pSym(p, node):
	case pNum(p, node):
	case pStr(p, node):
	default:
		pErr(p, node, "expect symbol, number or string")
	}
}


func pExprTuple(p *Parser, node *QLNode) {
	kids := []QLNode{{}}
	pExprMul(p, &kids[len(kids)-1])////////////////
	for pKeyword(p, ",") {
		kids = append(kids, QLNode{})
		pExprMul(p, &kids[len(kids)-1])
	}
	if len(kids) > 1 {
		node.Type = QL_TUP
		node.Kids = kids
	} else {
		*node = kids[0] // not a tuple
	}
}

func pSym(p *Parser, node *QLNode) bool {
	skipSpace(p)
	end := p.idx
	if !(end < len(p.input) && isSymStart(p.input[end])) {
		return false
	}
	end++
	for end < len(p.input) && isSym(p.input[end]) {
		end++
	}
	if pKeywordSet[strings.ToLower(string(p.input[p.idx:end]))] {
		return false // not allowed
	}
	node.Type = QL_SYM
	node.Str = p.input[p.idx:end]
	p.idx = end
	return true
}


func pNum(p *Parser, node *QLNode) bool {
	skipSpace(p)
	end := p.idx
	if !(end < len(p.input) && isNum(p.input[end])) {
		return false
	}
	end++
	for end < len(p.input) && isNum(p.input[end]) {
		end++
	}
	if pKeywordSet[strings.ToLower(string(p.input[p.idx:end]))] {
		return false // not allowed
	}
	node.Type = QL_I64
	node.I64, _ = strconv.Atoi(string(p.input[p.idx:end]))
	p.idx = end
	return true
}

func pStr(p *Parser, node *QLNode) bool {
	skipSpace(p)
	
	end := p.idx
	if !(end < len(p.input) && p.input[p.idx] == 39) {
		return false
	}
	end++
	p.idx++
	for end < len(p.input) && p.input[end] != 39 {
		end++
	}
	
	node.Type = QL_STR
	node.Str = p.input[p.idx:end]
	p.idx = end+1	
	return true
}


var pKeywordSet = map[string]bool{
	"from": true,
	"index": true,
	"filter": true,
	"limit": true,
}


func isSymStart(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_' || ch == '@'
}


func pInsert(p *Parser, mode int32) *QLInsert {
	stmt := QLInsert{}
	//fmt.Printf("insert\n", string(mode))
	
	
	
	stmt.Table = pMustSym(p)
	

	// VALUES
	if !pKeyword(p, "values") {
		pErr(p, nil, "expect `VALUES` " + strconv.Itoa(p.idx) )
	}
	
	// SET xxx
//	pSelectExprList(p, &stmt)
	pInsertExprList(p, &stmt)
	
	//fmt.Println(stmt)
	
	
	// INDEX BY xxx FILTER yyy LIMIT zzz
//	pScan(p, &stmt.QLScan)
	
	if p.err != nil {
		return nil
	}
	return &stmt
}

func pDelete(p *Parser) *QLDelete {
	stmt := QLDelete{}
	//fmt.Printf("delete\n")
	

	stmt.Table = pMustSym(p)
	

	// table SET
	/*if !pKeyword(p, "set") {
		pErr(p, nil, "expect `SET` table " + strconv.Itoa(p.idx) )
	}
	
	// SET xxx
//	pSelectExprList(p, &stmt)
	pUpdateExprList(p, &stmt)
	
	
	// INDEX BY xxx FILTER yyy LIMIT zzz
//	pScan(p, &stmt.QLScan)
	*/
	if p.err != nil {
		return nil
	}
	return &stmt
}

func pUpdate(p *Parser) *QLUpdate {
	stmt := QLUpdate{}
	//fmt.Printf("update\n")
	
	stmt.Table = pMustSym(p)
	

	// table SET
	if !pKeyword(p, "set") {
		pErr(p, nil, "expect `SET` " + strconv.Itoa(p.idx) )
	}
	
	// SET xxx
//	pSelectExprList(p, &stmt)
	pUpdateExprList(p, &stmt)
	
	
	// INDEX BY xxx FILTER yyy LIMIT zzz
//	pScan(p, &stmt.QLScan)
	
	if p.err != nil {
		return nil
	}
	return &stmt
}


func pUpdateExprList(p *Parser, node *QLUpdate) {
	pUpdateExpr(p, node)
	for pKeyword(p, ",") {
		pUpdateExpr(p, node)
	}
}

func pUpdateExpr(p *Parser, node *QLUpdate) {
	expr := QLNode{}
//	pExprOr(p, &expr)
	name := pMustSym(p) ////////
	
	if !pKeyword(p, "=") {
		pErr(p, nil, "expect `=` " + strconv.Itoa(p.idx) )
	}
	
	expr.Str = []byte(pMustSym(p)) ////////
	expr.Type = QL_STR ////
	
	//name := ""
	/*if pKeyword(p, "as") {
		name = pMustSym(p)
	}*/
	node.Names = append(node.Names, name)
	node.Values = append(node.Values, expr)
}

func pInsertExprList(p *Parser, node *QLInsert) {
	pInsertExpr(p, node)
	for pKeyword(p, ",") {
		pInsertExpr(p, node)
	}
}

func pInsertExpr(p *Parser, node *QLInsert) {
	expr := QLNode{}
	
	
	//pExprUnop(p, &expr)
	pExprOr(p, &expr)
	//pExprMul(p, &expr)
	//name := pMustSym(p) ////////
	
	//if !pKeyword(p, "=") {
	//	pErr(p, nil, "expect `=` " + strconv.Itoa(p.idx) )
	//}
	
	//pExprTuple(p, &expr)
	
//	expr.Str = []byte(pMustSym(p)) ////////
//	expr.Type = QL_STR ////
	
	name := ""
	/*if pKeyword(p, "as") {
		name = pMustSym(p)
	}*/
	node.Names = append(node.Names, name)
	node.Values = append(node.Values, expr)
}


// match multiple keywords sequentially
func pKeyword(p *Parser, kwds ...string) bool {
	save := p.idx
	for _, kw := range kwds {
		skipSpace(p)
		end := p.idx + len(kw)
		if end > len(p.input) {
			p.idx = save
			return false
		}
		// case insensitive matach
		ok := strings.EqualFold(string(p.input[p.idx:end]), kw)
		// token is terminated
		if ok && isSym(kw[len(kw)-1]) && end < len(p.input) {
			ok = !isSym(p.input[end])
		}
		if !ok {
			p.idx = save
			return false
		}
		p.idx += len(kw)
	}
	return true
}


func isSym(ch byte) bool {
	r := rune(ch)
	return unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_'
}

func isNum(ch byte) bool {
	return unicode.IsNumber(rune(ch)) 
}


func skipSpace(p *Parser) {
	if p.input[p.idx] == ' ' {
		p.idx++
	}
}

func pErr(p *Parser, node *QLNode, errStr string) {
	p.err = errors.New(errStr)
}
