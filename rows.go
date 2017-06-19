package psql

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var DefaultLocation *time.Location

func init() {
	var err error
	DefaultLocation, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic(err)
	}
}

type ColumnIndex int

type Rows struct {
	conn    *Conn
	obj     driver.Rows
	columns map[string]ColumnIndex
	row     []driver.Value
}

func (rows *Rows) Columns() []string {
	return rows.obj.Columns()
}

func (rows *Rows) C(column string) ColumnIndex {
	idx, found := rows.columns[column]
	if !found {
		panic("column " + column + " not found")
	}
	return idx
}

func (rows *Rows) Close() error {
	if rows == nil {
		return nil
	}
	rows.conn.activeQuerySql = ""
	rows.conn.activeQueryArgs = nil
	return rows.obj.Close()
}

type Batch struct {
	len     int
	data    map[string]interface{}
	columns []string
}

func NewBatch() *Batch {
	return &Batch{0, map[string]interface{}{}, nil}
}

func (batch *Batch) Len() int {
	return batch.len
}

func (batch *Batch) Columns() []string {
	return batch.columns
}

func (batch *Batch) GetStringColumn(column string) []string {
	colData := batch.data[column].([]string)
	return colData[:batch.len]
}

func (batch *Batch) GetString(row int, column string) string {
	return batch.GetStringColumn(column)[row]
}

func (batch *Batch) GetInt64Column(column string) []int64 {
	colData := batch.data[column].([]int64)
	return colData[:batch.len]
}

func (batch *Batch) GetInt(row int, column string) int {
	return int(batch.GetInt64Column(column)[row])
}

func (rows *Rows) NextBatch(batch *Batch, maxToRead int) error {
	batch.len = 0
	err := rows.obj.Next(rows.row)
	if err != nil {
		return err
	}
	readers := make([]columnReader, 0, 8)
	for columnName, columnIndex := range rows.columns {
		switch rows.row[columnIndex].(type) {
		case []byte:
			data, found := batch.data[columnName]
			if found {
				typedData, typeOk := data.([]string)
				if typeOk {
					if len(typedData) >= maxToRead {
						readers = append(readers, &byteArrayColumnReader{typedData, int(columnIndex)})
						continue
					}
				}
			}
			typedData := make([]string, maxToRead)
			batch.data[columnName] = typedData
			readers = append(readers, &byteArrayColumnReader{typedData, int(columnIndex)})
		case int64:
			data, found := batch.data[columnName]
			if found {
				typedData, typeOk := data.([]int64)
				if typeOk {
					if len(typedData) >= maxToRead {
						readers = append(readers, &int64ColumnReader{typedData, int(columnIndex)})
						continue
					}
				}
			}
			typedData := make([]int64, maxToRead)
			batch.data[columnName] = typedData
			readers = append(readers, &int64ColumnReader{typedData, int(columnIndex)})
		default:
			panic(fmt.Sprintf("unsupported type: %v", reflect.TypeOf(rows.row[columnIndex])))
		}
	}
	for _, reader := range readers {
		reader.read(rows.row, 0)
	}
	rowIndex := 1
	for rowIndex < maxToRead {
		err = rows.Next()
		if err != nil {
			break
		}
		for _, reader := range readers {
			reader.read(rows.row, rowIndex)
		}
		rowIndex++
	}
	batch.len = rowIndex
	if err != nil && strings.Contains("EOF", err.Error()) {
		err = nil
	}
	return err
}

type columnReader interface {
	read(row []driver.Value, rowIndex int)
}

type byteArrayColumnReader struct {
	data        []string
	columnIndex int
}

func (reader *byteArrayColumnReader) read(row []driver.Value, rowIndex int) {
	val := row[reader.columnIndex].([]byte)
	reader.data[rowIndex] = string(val)
}

type int64ColumnReader struct {
	data        []int64
	columnIndex int
}

func (reader *int64ColumnReader) read(row []driver.Value, rowIndex int) {
	val := row[reader.columnIndex].(int64)
	reader.data[rowIndex] = val
}

func (rows *Rows) Next() error {
	return rows.obj.Next(rows.row)
}

func (rows *Rows) Get(idx ColumnIndex) interface{} {
	obj := rows.row[idx]
	switch val := obj.(type) {
	case []byte:
		// copy the bytes to avoid being modified when buffer reused
		return string(val)
	}
	return obj
}

func (rows *Rows) GetByName(name string) interface{} {
	return rows.row[rows.C(name)]
}

func (rows *Rows) GetString(idx ColumnIndex) string {
	obj := rows.row[idx]
	switch val := obj.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	}
	panic(fmt.Sprintf("%v can not convert to string", obj))
}

func (rows *Rows) GetTime(idx ColumnIndex) time.Time {
	obj := rows.row[idx]
	asTime, ok := obj.(time.Time)
	if ok {
		return asTime
	}
	val, err := time.ParseInLocation("2006-01-02 15:04:05", rows.GetString(idx), DefaultLocation)
	if err != nil {
		panic(err)
	}
	return val
}

func (rows *Rows) GetInt64(idx ColumnIndex) int64 {
	obj := rows.Get(idx)
	switch val := obj.(type) {
	case int64:
		return val
	case string:
		if len(val) == 0 {
			return 0
		}
		int64Val, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			panic(err)
		}
		return int64Val
	case []byte:
		if len(val) == 0 {
			return 0
		}
		int64Val, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			panic(err)
		}
		return int64Val
	}
	panic(fmt.Sprintf("%v can not convert to int", obj))
}

func (rows *Rows) GetInt(idx ColumnIndex) int {
	return int(rows.GetInt64(idx))
}
