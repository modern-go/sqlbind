package dingo

import (
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"time"
)

func Join(columns ...string) string {
	return strings.Join(columns, ", ")
}

type ColumnGroup struct {
	Group                string
	Columns              []string
	BatchInsertRowsCount int
}

func Columns(group string, columns ...string) ColumnGroup {
	return ColumnGroup{group, columns, 0}
}

func BatchInsertColumns(batchInsertRowsCount int, columns ...string) ColumnGroup {
	return ColumnGroup{"COLUMNS", columns, batchInsertRowsCount}
}

func Tuple(vals ...interface{}) string {
	parts := make([]string, len(vals))
	for i := 0; i < len(vals); i++ {
		switch typedVal := vals[i].(type) {
		case string:
			parts[i] = "'" + typedVal + "'"
		default:
			parts[i] = fmt.Sprintf("%v", vals[i])
		}
	}
	return "(" + strings.Join(parts, ",") + ")"
}

type Conn struct {
	obj             driver.Conn
	tx              driver.Tx
	activeQuerySql  string
	activeQueryArgs []driver.Value
	Error           error
	onClose         func(conn *Conn) error
}

func Open(drv driver.Driver, dsn string) (*Conn, error) {
	conn, err := drv.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &Conn{conn, nil, "", nil, nil, nil}, nil
}

func (conn *Conn) TranslateStatement(sql string, columns ...interface{}) *Stmt {
	translatedSql := Translate(sql, columns...)
	return &Stmt{conn, map[string]driver.Stmt{}, translatedSql}
}

func (conn *Conn) Statement(translatedSql *TranslatedSql) *Stmt {
	return &Stmt{conn, map[string]driver.Stmt{}, translatedSql}
}

func (conn *Conn) Close() error {
	if conn == nil {
		return nil
	}
	if conn.onClose == nil {
		return conn.obj.Close()
	} else {
		return conn.onClose(conn)
	}
}

func (conn *Conn) BeginTx() error {
	tx, err := conn.obj.Begin()
	if err != nil {
		return err
	}
	conn.tx = tx
	return nil
}

func (conn *Conn) CommitTx() error {
	return conn.tx.Commit()
}

func (conn *Conn) RollbackTx() error {
	return conn.tx.Rollback()
}

func (conn *Conn) Exec(translatedSql *TranslatedSql, inputs ...driver.Value) (driver.Result, error) {
	stmt := conn.Statement(translatedSql)
	defer stmt.Close()
	return stmt.Exec(inputs...)
}

type Stmt struct {
	conn          *Conn
	objs          map[string]driver.Stmt
	translatedSql *TranslatedSql
}

func (stmt *Stmt) Close() error {
	if stmt == nil {
		return nil
	}
	var err error
	for _, obj := range stmt.objs {
		err = obj.Close()
	}
	return err
}

func BatchInsertRow(inputs ...driver.Value) []driver.Value {
	return inputs
}

func (stmt *Stmt) Exec(inputs ...driver.Value) (driver.Result, error) {
	args, prepared := stmt.toArgs(inputs)
	formattedSql := stmt.format(args)
	execArgs := args[stmt.translatedSql.strParamCount:]
	var result driver.Result
	var err error
	var obj driver.Stmt
	if prepared {
		obj, err = stmt.prepare(formattedSql)
		if err != nil {
			return nil, fmt.Errorf("%s\nsql: %v\n", err.Error(), formattedSql)
		}
		result, err = obj.Exec(execArgs)
	} else {
		execer := stmt.conn.obj.(driver.Execer)
		result, err = execer.Exec(formattedSql, execArgs)
	}
	if err != nil {
		stmt.conn.Error = err
		return nil, fmt.Errorf("%s\nsql: %v\nargs: %v", err.Error(), formattedSql, execArgs)
	}
	return result, err
}

func (stmt *Stmt) Query(inputs ...driver.Value) (*Rows, error) {
	if stmt.conn.activeQuerySql != "" {
		return nil, fmt.Errorf("there is another active query in progress\nsql: %v\nargs: %v",
			stmt.conn.activeQuerySql, stmt.conn.activeQueryArgs)
	}
	args, prepared := stmt.toArgs(inputs)
	formattedSql := stmt.format(args)
	queryArgs := args[stmt.translatedSql.strParamCount:]
	var rows driver.Rows
	var err error
	var obj driver.Stmt
	if prepared {
		obj, err = stmt.prepare(formattedSql)
		if err != nil {
			return nil, fmt.Errorf("%s\nsql: %v\n", err.Error(), formattedSql)
		}
		rows, err = obj.Query(queryArgs)
	} else {
		queryer := stmt.conn.obj.(driver.Queryer)
		rows, err = queryer.Query(formattedSql, queryArgs)
	}
	if err != nil {
		stmt.conn.Error = err
		return nil, fmt.Errorf("%s\nsql: %v\nargs: %v", err.Error(), formattedSql, queryArgs)
	}
	columns := map[string]ColumnIndex{}
	for idx, column := range rows.Columns() {
		columns[column] = ColumnIndex(idx)
	}
	stmt.conn.activeQuerySql = formattedSql
	stmt.conn.activeQueryArgs = queryArgs
	return &Rows{stmt.conn, rows, columns, make([]driver.Value, len(columns))}, nil
}

func (stmt *Stmt) toArgs(inputs []driver.Value) ([]driver.Value, bool) {
	if len(inputs) == 0 {
		return []driver.Value{}, true
	}
	_, isBatchInsert := inputs[0].([]driver.Value)
	if isBatchInsert {
		args := make([]driver.Value, 0, 64)
		for _, batchInsertRow := range inputs {
			rowArgs, _ := stmt.toArgs(batchInsertRow.([]driver.Value))
			args = append(args, rowArgs...)
		}
		return args, true
	}
	prepared := true
	args := make([]driver.Value, stmt.translatedSql.totalParamCount)
	for i := 0; i < len(inputs); i += 2 {
		argName := inputs[i].(string)
		argValue := inputs[i+1]
		switch argName {
		case "ROW":
			row := argValue.(*Rows)
			// bind row to args, if row has extra column, ignore
			for column, columnIdx := range row.columns {
				argIndices, found := stmt.translatedSql.paramMap[column]
				if found {
					for _, argIdx := range argIndices {
						args[argIdx] = row.Get(columnIdx)
					}
				}
			}
		case "PREPARED":
			prepared = argValue.(bool)
		default:
			argIndices, found := stmt.translatedSql.paramMap[argName]
			if !found {
				panic("argument not found in sql: " + argName)
			}
			for _, argIdx := range argIndices {
				args[argIdx] = argValue
			}
		}
	}
	return args, prepared
}

func (stmt *Stmt) format(args []driver.Value) string {
	formattedSql := stmt.translatedSql.sql
	if stmt.translatedSql.strParamCount > 0 {
		formatArgs := make([]interface{}, stmt.translatedSql.strParamCount)
		for i, v := range args[:stmt.translatedSql.strParamCount] {
			formatArgs[i] = v
		}
		formattedSql = fmt.Sprintf(stmt.translatedSql.sql, formatArgs...)
	}
	if "true" == os.Getenv("SQLXX_DEBUG") {
		fmt.Fprintln(os.Stderr, fmt.Sprintf(">>> %v\n%s\n%v\n", time.Now(), formattedSql,
			args[stmt.translatedSql.strParamCount:]))
	}
	return formattedSql
}

func (stmt *Stmt) prepare(formattedSql string) (driver.Stmt, error) {
	obj := stmt.objs[formattedSql]
	if obj == nil {
		var err error
		obj, err = stmt.conn.obj.Prepare(formattedSql)
		if err != nil {
			stmt.conn.Error = err
			return nil, err
		}
		stmt.objs[formattedSql] = obj
	}
	return obj, nil
}
