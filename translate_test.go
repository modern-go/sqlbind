package dingo

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_translate_sql_with_arg(t *testing.T) {
	should := require.New(t)
	translatedSql := Translate(
		"SELECT * FROM account_010 WHERE passenger_id=:pid AND driver_id=:did")
	should.Equal("SELECT * FROM account_010 WHERE passenger_id=? AND driver_id=?", translatedSql.sql)
	should.Equal(map[string][]int{
		"pid": {0},
		"did": {1},
	}, translatedSql.paramMap)
	should.Equal(2, translatedSql.totalParamCount)
}

func Test_translate_sql_with_sub(t *testing.T) {
	should := require.New(t)
	translatedSql := Translate(
		"SELECT * FROM account_:STR_district WHERE passenger_id=:pid AND driver_id=:did")
	should.Equal("SELECT * FROM account_%v WHERE passenger_id=? AND driver_id=?", translatedSql.sql)
	should.Equal(map[string][]int{
		"pid":          {1},
		"did":          {2},
		"STR_district": {0},
	}, translatedSql.paramMap)
	should.Equal(1, translatedSql.strParamCount)
}

func Test_translate_UPDATE_COLUMNS(t *testing.T) {
	should := require.New(t)
	translatedSql := Translate(
		"UPDATE account_:STR_district SET "+
			"_modify_time=now(), :UPDATE_COLUMNS WHERE order_id=:oid", "passenger_count")
	should.Equal("UPDATE account_%v SET _modify_time=now(), passenger_count=? WHERE order_id=?", translatedSql.sql)
	should.Equal(map[string][]int{
		"passenger_count": {1},
		"oid":             {2},
		"STR_district":    {0},
	}, translatedSql.paramMap)
	should.Equal(1, translatedSql.strParamCount)
}

func Test_translate_INSERT_COLUMNS(t *testing.T) {
	should := require.New(t)
	translatedSql := Translate(
		`INSERT test :INSERT_COLUMNS`, "name")
	should.Equal(`INSERT test (name) VALUES (?)`, translatedSql.sql)
	should.Equal(map[string][]int{
		"name": {0},
	}, translatedSql.paramMap)
	should.Equal(1, translatedSql.totalParamCount)
}

func Test_translate_sql_with_duplicated_args(t *testing.T) {
	should := require.New(t)
	translatedSql := Translate(
		"SELECT * FROM account_010 WHERE passenger_id=:pid AND passenger_id=:pid")
	should.Equal("SELECT * FROM account_010 WHERE passenger_id=? AND passenger_id=?", translatedSql.sql)
	should.Equal(map[string][]int{
		"pid": {0, 1},
	}, translatedSql.paramMap)
	should.Equal(2, translatedSql.totalParamCount)
}

func Test_translate_skip_quote(t *testing.T) {
	should := require.New(t)
	should.Equal("SELECT * FROM account_010 WHERE passenger_id=':pid'", Translate(
		`SELECT * FROM account_010 WHERE passenger_id=':pid'`).sql)
	should.Equal(`SELECT * FROM account_010 WHERE passenger_id='\':pid'`, Translate(
		`SELECT * FROM account_010 WHERE passenger_id='\':pid'`).sql)
	should.Equal(`SELECT * FROM account_010 WHERE passenger_id=":pid"`, Translate(
		`SELECT * FROM account_010 WHERE passenger_id=":pid"`).sql)
	should.Equal(`SELECT * FROM account_010 WHERE passenger_id="\":pid"`, Translate(
		`SELECT * FROM account_010 WHERE passenger_id="\":pid"`).sql)
}

func Test_translate_column_group(t *testing.T) {
	should := require.New(t)
	should.Equal("(a, b) VALUES (?, ?) (c, d) VALUES (?, ?)", Translate(
		`:INSERT_COLUMNS1 :INSERT_COLUMNS2`,
		Columns("COLUMNS1", "a", "b"),
		Columns("COLUMNS2", "c", "d")).sql)
}

func Test_translate_SELECT_COLUMNS(t *testing.T) {
	should := require.New(t)
	should.Equal("a, b", Translate(
		`:SELECT_COLUMNS`, "a", "b").sql)
}

func Test_translate_HINT_COLUMNS(t *testing.T) {
	should := require.New(t)
	should.Equal(`/*{"a":"%v","b":"%v"}*/`, Translate(
		`:HINT_COLUMNS`, "a", "b").sql)
}
