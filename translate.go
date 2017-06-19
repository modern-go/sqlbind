package psql

import (
	"bytes"
	"fmt"
	"strings"
	"unicode"
	"unsafe"
)

/*
translate named parameter into string argument (%v) or normal argument (?)
normal argument only support mysql syntax ? yet
*/

const stateNormal = 0
const stateInVar = 1
const stateInSingleQuote = 5 // '
const stateInDoubleQuote = 6 // "

type TranslatedSql struct {
	sql             string
	paramMap        map[string][]int
	strParamCount   int
	totalParamCount int
}

func NewTranslatedSql(sql string, argMap map[string][]int, strParamCount int, totalParamCount int) *TranslatedSql {
	return &TranslatedSql{sql, argMap, strParamCount, totalParamCount}
}

func Translate(sql string, columns ...interface{}) *TranslatedSql {
	columnGroups := spitIntoGroups(columns)
	sqlAsBytes := *(*[]byte)(unsafe.Pointer(&sql))
	buf := bytes.NewBuffer(make([]byte, 0, len(sql)))
	state := stateNormal
	tempVarName := bytes.NewBuffer(make([]byte, 0, 10))
	paramMap := newParamMap()
	strParamMap := newParamMap()
	for i := 0; i < len(sqlAsBytes); i++ {
		c := sqlAsBytes[i]
		switch state {
		case stateInVar:
			if ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_' || c == '-' || ('0' <= c && c <= '9') {
				tempVarName.WriteByte(c)
			} else {
				varName := tempVarName.String()
				addVar(varName, columnGroups, buf, paramMap, strParamMap)
				buf.WriteByte(c)
				state = stateNormal
			}
		case stateInSingleQuote:
			buf.WriteByte(c)
			if c == '\\' {
				i++
				buf.WriteByte(sqlAsBytes[i])
			} else if c == '\'' {
				state = stateNormal
			}
		case stateInDoubleQuote:
			buf.WriteByte(c)
			if c == '\\' {
				i++
				buf.WriteByte(sqlAsBytes[i])
			} else if c == '"' {
				state = stateNormal
			}
		default:
			if c == ':' {
				state = stateInVar
				tempVarName.Reset()
			} else {
				buf.WriteByte(c)
				if c == '\'' {
					state = stateInSingleQuote
				} else if c == '"' {
					state = stateInDoubleQuote
				}
			}

		}
	}
	if state == stateInVar {
		varName := tempVarName.String()
		addVar(varName, columnGroups, buf, paramMap, strParamMap)
	}
	strParamCount := strParamMap.currentPos
	strParamMap.merge(paramMap)
	return &TranslatedSql{buf.String(), strParamMap.paramMap, strParamCount, paramMap.currentPos + strParamCount}
}

func spitIntoGroups(ungrouped []interface{}) map[string]*ColumnGroup {
	grouped := map[string]*ColumnGroup{}
	grouped["COLUMNS"] = &ColumnGroup{"COLUMNS", make([]string, 0), 0}
	for _, columnOrGroup := range ungrouped {
		switch typed := columnOrGroup.(type) {
		case string:
			grouped["COLUMNS"].Columns = append(grouped["COLUMNS"].Columns, typed)
		case ColumnGroup:
			grouped[typed.Group] = &typed
		default:
			panic(fmt.Sprintf("unexpected column argument: %v", columnOrGroup))
		}
	}
	return grouped
}

type nameToPositions struct {
	paramMap   map[string][]int
	currentPos int
}

func newParamMap() *nameToPositions {
	return &nameToPositions{map[string][]int{}, 0}
}

func (ntp *nameToPositions) addParameter(name string) {
	positions, existing := ntp.paramMap[name]
	if existing {
		ntp.paramMap[name] = append(positions, ntp.currentPos)
	} else {
		ntp.paramMap[name] = []int{ntp.currentPos}
	}
	ntp.currentPos++
}

func (ntp *nameToPositions) merge(that *nameToPositions) {
	for k, origPositions := range that.paramMap {
		shiftedPositions := make([]int, 0, len(origPositions))
		for _, e := range origPositions {
			shiftedPositions = append(shiftedPositions, e+ntp.currentPos)
		}
		ntp.paramMap[k] = shiftedPositions
	}
}

func addVar(varName string, columnGroups map[string]*ColumnGroup, buf *bytes.Buffer, paramMap *nameToPositions, strParamMap *nameToPositions) {
	if strings.HasPrefix(varName, "BATCH_INSERT_") {
		addVar_BATCH_INSERT(varName, columnGroups, buf, paramMap, strParamMap)
		return
	}
	if strings.HasPrefix(varName, "INSERT_") {
		addVar_INSERT(varName, columnGroups, buf, paramMap, strParamMap)
		return
	}
	if strings.HasPrefix(varName, "UPDATE_") {
		addVar_UPDATE(varName, columnGroups, buf, paramMap, strParamMap)
		return
	}
	if strings.HasPrefix(varName, "SELECT_") {
		addVar_SELECT(varName, columnGroups, buf, paramMap, strParamMap)
		return
	}
	if strings.HasPrefix(varName, "HINT_") {
		addVar_HINT(varName, columnGroups, buf, paramMap, strParamMap)
		return
	}
	if strings.HasPrefix(varName, "STR_") {
		strParamMap.addParameter(varName)
		buf.WriteByte('%')
		buf.WriteByte('v')
		return
	}
	if unicode.IsUpper(rune(varName[0])) {
		panic("normal parameter should not start with upper case: " + varName)
	}
	paramMap.addParameter(varName)
	buf.WriteByte('?')
}

func addVar_BATCH_INSERT(varName string, columnGroups map[string]*ColumnGroup, buf *bytes.Buffer, paramMap *nameToPositions, strParamMap *nameToPositions) {
	group := varName[len("BATCH_INSERT_"):]
	columns, found := columnGroups[group]
	if !found {
		panic(fmt.Sprintf("%v referenced column group not specified: %v", varName, columnGroups))
	}
	isFirst := true
	buf.WriteByte('(')
	for _, column := range columns.Columns {
		paramMap.addParameter(column)
		if isFirst {
			isFirst = false
		} else {
			buf.WriteString(", ")
		}
		buf.WriteString(column)
	}
	buf.WriteString(") VALUES ")
	valuesIsFirst := true
	for j := 0; j < columns.BatchInsertRowsCount; j++ {
		if valuesIsFirst {
			valuesIsFirst = false
		} else {
			buf.WriteString(", ")
		}
		buf.WriteByte('(')
		isFirst = true
		for i := 0; i < len(columns.Columns); i++ {
			if isFirst {
				isFirst = false
			} else {
				buf.WriteString(", ")
			}
			buf.WriteByte('?')
		}
		buf.WriteByte(')')
	}
}

func addVar_INSERT(varName string, columnGroups map[string]*ColumnGroup, buf *bytes.Buffer, paramMap *nameToPositions, strParamMap *nameToPositions) {
	group := varName[len("INSERT_"):]
	columns, found := columnGroups[group]
	if !found {
		panic(fmt.Sprintf("%v referenced column group not specified: %v", varName, columnGroups))
	}
	isFirst := true
	buf.WriteByte('(')
	for _, column := range columns.Columns {
		paramMap.addParameter(column)
		if isFirst {
			isFirst = false
		} else {
			buf.WriteString(", ")
		}
		buf.WriteString(column)
	}
	buf.WriteString(") VALUES (")
	isFirst = true
	for i := 0; i < len(columns.Columns); i++ {
		if isFirst {
			isFirst = false
		} else {
			buf.WriteString(", ")
		}
		buf.WriteByte('?')
	}
	buf.WriteByte(')')
}

func addVar_UPDATE(varName string, columnGroups map[string]*ColumnGroup, buf *bytes.Buffer, paramMap *nameToPositions, strParamMap *nameToPositions) {
	group := varName[len("UPDATE_"):]
	columns, found := columnGroups[group]
	if !found {
		panic(fmt.Sprintf("%v referenced column group not specified: %v", varName, columnGroups))
	}
	isFirst := true
	for _, column := range columns.Columns {
		paramMap.addParameter(column)
		if isFirst {
			isFirst = false
		} else {
			buf.WriteString(", ")
		}
		buf.WriteString(column)
		buf.WriteByte('=')
		buf.WriteByte('?')
	}
}

func addVar_SELECT(varName string, columnGroups map[string]*ColumnGroup, buf *bytes.Buffer, paramMap *nameToPositions, strParamMap *nameToPositions) {
	group := varName[len("SELECT_"):]
	columns, found := columnGroups[group]
	if !found {
		panic(fmt.Sprintf("%v referenced column group not specified: %v", varName, columnGroups))
	}
	buf.WriteString(Join(columns.Columns...))
}

func addVar_HINT(varName string, columnGroups map[string]*ColumnGroup, buf *bytes.Buffer, paramMap *nameToPositions, strParamMap *nameToPositions) {
	group := varName[len("HINT_"):]
	columns, found := columnGroups[group]
	if !found {
		panic(fmt.Sprintf("%v referenced column group not specified: %v", varName, columnGroups))
	}
	buf.WriteString(`/*{`)
	for i, column := range columns.Columns {
		if i != 0 {
			buf.WriteByte(',')
		}
		strParamMap.addParameter(column)
		buf.WriteByte('"')
		buf.WriteString(column)
		buf.WriteString(`":"%v"`)
	}
	buf.WriteString(`}*/`)
}
