package dingo

import (
	"github.com/v2pro/plz"
	"github.com/v2pro/plz/sql"
)

func init() {
	sql.Translate = Translate
	plz.OpenSqlConn = Open
}
