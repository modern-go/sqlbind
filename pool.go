package dingo

import (
	"database/sql/driver"
	"errors"
	"sync/atomic"
)

/*
a minimal connection pool, backed by channel
*/

type Pool struct {
	conns          chan *Conn
	drv            driver.Driver
	dsn            string
	maxActiveCount int32
	activeCount    int32
}

var TooManyConcurrentConnections = errors.New("TooManyConcurrentConnections")

func NewPool(drv driver.Driver, dsn string, size int32) *Pool {
	return &Pool{make(chan *Conn, size), drv, dsn, size, 0}
}

func (pool *Pool) Borrow() (*Conn, error) {
	select {
	case conn := <-pool.conns:
		return conn, nil
	default:
		if atomic.AddInt32(&pool.activeCount, 1) > pool.maxActiveCount {
			return nil, TooManyConcurrentConnections
		}
		conn, err := Open(pool.drv, pool.dsn)
		if err != nil {
			return nil, err
		}
		conn.(*Conn).onClose = func(conn *Conn) error {
			atomic.AddInt32(&pool.activeCount, -1)
			conn.onClose = pool.release
			return pool.release(conn)
		}
		return conn.(*Conn), nil
	}
}

func (pool *Pool) release(conn *Conn) error {
	if conn.Error != nil {
		return conn.obj.Close()
	}
	select {
	case pool.conns <- conn:
		return nil
	default:
		return conn.obj.Close()
	}
}
