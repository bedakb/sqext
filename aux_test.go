package sqext

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
)

func TestAuxBuilderToSql(t *testing.T) {
	b := Aux(sq.Select("*").From("events").Where(sq.Eq{"is_completed": true})).
		Alias("completed_events").
		Columns("a", "b").
		Recursive()

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "RECURSIVE completed_events(a, b) AS (SELECT * FROM events WHERE is_completed = ?)"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{true}, args)
}

func TestAuxBuilderToSqlNoColumns(t *testing.T) {
	b := Aux(sq.Select("*").From("a").Where(sq.Eq{"x": 20})).
		Alias("a_data").
		Recursive()

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "RECURSIVE a_data AS (SELECT * FROM a WHERE x = ?)"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{20}, args)
}

func TestAuxBuilderToSqlBasicAux(t *testing.T) {
	b := Aux(sq.Select("a", "b").From("x").Where(sq.Eq{"y": 1})).
		Alias("a_data")

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "a_data AS (SELECT a, b FROM x WHERE y = ?)"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{1}, args)
}

func TestAuxBuilderToSqlErr(t *testing.T) {
	b := Aux(sq.Select("*").From("events").Where(sq.Gt{"is_completed": true})).Recursive()

	_, _, err := b.ToSql()
	assert.Error(t, err)
}
