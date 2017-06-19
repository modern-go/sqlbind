package psql

import (
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_select(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	stmt := conn.TranslateStatement(
		`SELECT :SELECT_COLUMNS FROM account_event
	WHERE entity_id=:entity_id`, "entity_id", "event_id", "data")
	should.Nil(err)
	defer stmt.Close()
	rows, err := stmt.Query(
		"PREPARED", true,
		"entity_id", "account1")
	should.Nil(err)
	defer rows.Close()
	entity_id := rows.C("entity_id")
	event_id := rows.C("event_id")
	for rows.Next() == nil {
		fmt.Println(rows.Get(entity_id))
		fmt.Println(rows.Get(event_id))
	}
}

func Test_select_batch(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := Open(drv, "root:123456@tcp(10.94.66.30:3206)/gulfstream")
	should.Nil(err)
	defer conn.Close()
	stmt := conn.TranslateStatement(`SELECT district, passenger_id FROM g_order_:STR_DISTRICT LIMIT 6`)
	should.Nil(err)
	defer stmt.Close()
	rows, err := stmt.Query(
		"STR_DISTRICT", "010")
	should.Nil(err)
	defer rows.Close()
	batch := NewBatch()
	should.Nil(rows.NextBatch(batch, 5))
	should.Equal(5, batch.Len())
	should.Equal("010", batch.GetString(2, "district"))
	should.Nil(rows.NextBatch(batch, 5))
	should.Equal(1, batch.Len())
	should.Equal("010", batch.GetString(0, "district"))
}

func Test_select_in(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	conn.Exec(Translate("TRUNCATE account"))
	conn.Exec(Translate("INSERT account :INSERT_COLUMNS",
		"entity_id", "version", "data"),
		"entity_id", "account1",
		"version", int64(1),
		"data", "{}")
	stmt := conn.TranslateStatement(
		"SELECT * FROM account")
	defer stmt.Close()
	rows, err := stmt.Query()
	should.Nil(err)
	defer rows.Close()
	should.Nil(rows.Next())
	should.Equal("{}", rows.Get(rows.C("data")))
}

func Test_update(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	conn.Exec(Translate("TRUNCATE account"))
	conn.Exec(Translate("INSERT account :INSERT_COLUMNS",
		"entity_id", "version", "data"),
		"entity_id", "account1",
		"version", int64(1),
		"data", "{}")
	result, err := conn.Exec(Translate("UPDATE account SET :UPDATE_COLUMNS WHERE entity_id=:entity_id",
		"version", "data"),
		"entity_id", "account1",
		"version", int64(2),
		"data", "{}")
	should.Nil(err)
	rowsAffected, err := result.RowsAffected()
	should.Nil(err)
	should.Equal(int64(1), rowsAffected)
}

func Test_insert(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	conn.Exec(Translate("TRUNCATE account_event"))
	stmt := conn.TranslateStatement(
		"INSERT account_event :INSERT_COLUMNS",
		"entity_id", "event_id", "event_name", "data")
	defer stmt.Close()
	result, err := stmt.Exec(
		"entity_id", "account1",
		"event_id", int64(1),
		"event_name", "created",
		"data", "{}")
	should.Nil(err)
	rowsAffected, err := result.RowsAffected()
	should.Nil(err)
	should.Equal(int64(1), rowsAffected)
}

func Test_batch_insert(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	conn.Exec(Translate("TRUNCATE account_event"))
	stmt := conn.TranslateStatement("INSERT account_event :BATCH_INSERT_COLUMNS",
		BatchInsertColumns(2, "entity_id", "event_id", "event_name", "data"))
	should.Nil(err)
	defer stmt.Close()
	result, err := stmt.Exec(
		BatchInsertRow(
			"entity_id", "account1",
			"event_id", int64(1),
			"event_name", "created",
			"data", "{}"),
		BatchInsertRow(
			"entity_id", "account1",
			"event_id", int64(2),
			"event_name", "bill1_transfer",
			"data", "{}"))
	should.Nil(err)
	rowsAffected, err := result.RowsAffected()
	should.Nil(err)
	should.Equal(int64(2), rowsAffected)
}
