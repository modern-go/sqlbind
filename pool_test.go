package psql

import (
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_borrow_and_close(t *testing.T) {
	should := require.New(t)
	pool := NewPool(mysql.MySQLDriver{}, "root:123456@tcp(127.0.0.1:3306)/v2pro", 2)
	conn, err := pool.Borrow()
	should.Nil(err)
	should.Nil(conn.Close())
}

func Test_borrow_close_borrow_close(t *testing.T) {
	should := require.New(t)
	pool := NewPool(mysql.MySQLDriver{}, "root:123456@tcp(127.0.0.1:3306)/v2pro", 2)
	conn, err := pool.Borrow()
	should.Nil(err)
	should.Nil(conn.Close())
	conn, err = pool.Borrow()
	should.Nil(err)
	should.Nil(conn.Close())
	conn, err = pool.Borrow()
	should.Nil(err)
	should.Nil(conn.Close())
	should.Equal(int32(0), pool.activeCount)
}
