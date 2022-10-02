package sqext

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
)

func TestCTEBuilderToSql(t *testing.T) {
	insertA := sq.Insert("a").
		Columns("a", "b").
		Values(1, 3).
		Values(5, 8)

	selectB := sq.Select("a", "b").From("c").Where(sq.Eq{"a": 1})

	b := CTE(
		Aux(insertA).Alias("a_inserted"),
		Aux(selectB).Alias("b_selected").Columns("a").Recursive(),
	)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "WITH a_inserted AS (INSERT INTO a (a,b) VALUES (?,?),(?,?)), RECURSIVE b_selected(a) AS (SELECT a, b FROM c WHERE a = ?)"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{1, 3, 5, 8, 1}, args)
}

func TestCTEBuilderToSqlErr(t *testing.T) {
	b := CTE()
	_, _, err := b.ToSql()
	assert.Error(t, err)
}
