package sqext

import (
	"bytes"
	"fmt"
	"strings"

	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"github.com/lann/builder"
)

type auxData struct {
	PlaceholderFormat sq.PlaceholderFormat
	RunWith           sq.BaseRunner
	Alias             string
	Columns           []string
	Recursive         bool
	Statement         sq.Sqlizer
}

func (a *auxData) Exec() (sql.Result, error) {
	if a.RunWith == nil {
		return nil, sq.RunnerNotSet
	}
	return sq.ExecWith(a.RunWith, a)
}

func (a *auxData) ToSql() (string, []interface{}, error) {
	if a.Alias == "" {
		return "", nil, fmt.Errorf("auxillary statement must contain alias")
	}

	var sql bytes.Buffer

	if a.Recursive {
		sql.WriteString("RECURSIVE ")
	}

	sql.WriteString(a.Alias)

	if len(a.Columns) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(a.Columns, ", "))
		sql.WriteString(")")
	}

	sql.WriteString(" AS (")
	var args []interface{}
	var err error
	args, err = appendToSql(a.Statement, &sql, args)
	if err != nil {
		return "", []interface{}{}, err
	}
	sql.WriteString(")")

	sqlStr, err := a.PlaceholderFormat.ReplacePlaceholders(sql.String())
	if err != nil {
		return "", []interface{}{}, err
	}

	return sqlStr, args, nil
}

// Builder

// AuxBuilder builds auxillary statements used by CTEs.
type AuxBuilder builder.Builder

func init() {
	builder.Register(AuxBuilder{}, auxData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b AuxBuilder) PlaceholderFormat(f sq.PlaceholderFormat) AuxBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(AuxBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
// For most cases runner will be a database connection.
func (b AuxBuilder) RunWith(runner sq.BaseRunner) AuxBuilder {
	return setRunWith(b, runner).(AuxBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b AuxBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(auxData)
	return data.Exec()
}

// ToSql builds the query into a SQL string and bound args.
func (b AuxBuilder) ToSql() (string, []interface{}, error) {
	data := builder.GetStruct(b).(auxData)
	return data.ToSql()
}

// Alias assigns an alias for the auxillary statements.
func (b AuxBuilder) Alias(alias string) AuxBuilder {
	return builder.Set(b, "Alias", alias).(AuxBuilder)
}

// Recursive adds RECURSIVE modifier to the auxillary statments.
func (b AuxBuilder) Recursive() AuxBuilder {
	return builder.Set(b, "Recursive", true).(AuxBuilder)
}

// Columns adds result columns of auxillary statement.
func (b AuxBuilder) Columns(columns ...string) AuxBuilder {
	return builder.Extend(b, "Columns", columns).(AuxBuilder)
}

// Statement sets a subquery into auxillary statement.
func (b AuxBuilder) Statement(stmt sq.Sqlizer) AuxBuilder {
	return builder.Set(b, "Statement", stmt).(AuxBuilder)
}
