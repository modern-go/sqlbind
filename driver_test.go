package dingo

import (
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"testing"
	"io"
)

func Test_select(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	conn.Exec(Translate("TRUNCATE account"))
	stmt := conn.TranslateStatement(
		`SELECT :SELECT_COLUMNS FROM account
	WHERE entity_id=:entity_id`, "entity_id", "event_id", "state")
	should.Nil(err)
	defer stmt.Close()
	rows, err := stmt.Query(
		"PREPARED", true,
		"entity_id", "account1")
	should.Nil(err)
	defer rows.Close()
	should.Equal(io.EOF, rows.Next())
}

func Test_select_in(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	conn.Exec(Translate("TRUNCATE account"))
	conn.Exec(Translate("INSERT account :INSERT_COLUMNS",
		"entity_id", "event_id", "event_name", "command", "state"),
		"entity_id", "account1",
		"event_id", int64(1),
		"event_name", "created",
		"command", "{}",
		"state", "{}")
	stmt := conn.TranslateStatement(
		"SELECT * FROM account WHERE entity_id IN :STR_ENTITY_IDS")
	defer stmt.Close()
	rows, err := stmt.Query(
		"STR_ENTITY_IDS", Tuple("account1"))
	should.Nil(err)
	defer rows.Close()
	should.Nil(rows.Next())
	should.Equal("{}", rows.Get(rows.C("state")))
}

func Test_update(t *testing.T) {
	should := require.New(t)
	drv := mysql.MySQLDriver{}
	conn, err := Open(drv, "root:123456@tcp(127.0.0.1:3306)/v2pro")
	should.Nil(err)
	defer conn.Close()
	conn.Exec(Translate("TRUNCATE account"))
	conn.Exec(Translate("INSERT account :INSERT_COLUMNS",
		"entity_id", "event_id", "event_name", "command", "state"),
		"entity_id", "account1",
		"event_id", int64(1),
		"event_name", "created",
		"command", "{}",
		"state", "{}")
	result, err := conn.Exec(Translate("UPDATE account SET :UPDATE_COLUMNS WHERE entity_id=:entity_id",
		"event_id", "state"),
		"entity_id", "account1",
		"event_id", int64(2),
		"state", "{}")
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
	conn.Exec(Translate("TRUNCATE account"))
	stmt := conn.TranslateStatement(
		"INSERT account :INSERT_COLUMNS",
		"entity_id", "event_id", "event_name", "command", "state")
	defer stmt.Close()
	result, err := stmt.Exec(
		"entity_id", "account1",
		"event_id", int64(1),
		"event_name", "created",
		"command", "{}",
		"state", "{}")
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
	conn.Exec(Translate("TRUNCATE account"))
	stmt := conn.TranslateStatement("INSERT account :BATCH_INSERT_COLUMNS",
		BatchInsertColumns(2, "entity_id", "event_id", "event_name", "command", "state"))
	should.Nil(err)
	defer stmt.Close()
	result, err := stmt.Exec(
		BatchInsertRow(
			"entity_id", "account1",
			"event_id", int64(1),
			"event_name", "created",
			"command", "{}",
			"state", "{}"),
		BatchInsertRow(
			"entity_id", "account1",
			"event_id", int64(2),
			"event_name", "bill1_transfer",
			"command", "{}",
			"state", "{}"))
	should.Nil(err)
	rowsAffected, err := result.RowsAffected()
	should.Nil(err)
	should.Equal(int64(2), rowsAffected)
}
