package dingo

import "github.com/v2pro/plz"

func init() {
	plz.TranslateSql = Translate
	plz.SqlOpen = Open
}
