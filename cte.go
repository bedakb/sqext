package sqext

import (
	"bytes"
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/lann/builder"
)

type cteData struct {
	PlaceholderFormat sq.PlaceholderFormat
	RunWith           sq.BaseRunner
	AuxStatements     []sq.Sqlizer
}

func (c *cteData) Exec() (sql.Result, error) {
	if c.RunWith == nil {
		return nil, sq.RunnerNotSet
	}
	return sq.ExecWith(c.RunWith, c)
}

func (c *cteData) ToSql() (string, []interface{}, error) {
	if len(c.AuxStatements) == 0 {
		return "", []interface{}{}, fmt.Errorf("CTE must contain at least one auxillary statement")
	}

	var sql bytes.Buffer
	var args []interface{}

	sql.WriteString("WITH ")

	for i, stmt := range c.AuxStatements {
		var err error
		args, err = appendToSql(stmt, &sql, args)
		if err != nil {
			return "", []interface{}{}, err
		}

		if i != len(c.AuxStatements)-1 {
			sql.WriteString(", ")
		}
	}

	sqlStr, err := c.PlaceholderFormat.ReplacePlaceholders(sql.String())
	if err != nil {
		return "", []interface{}{}, err
	}

	return sqlStr, args, nil
}

// Builder

// CTEBuilder builds SQL WITH statement, also known as Common Table Expression (CTE).
type CTEBuilder builder.Builder

func init() {
	builder.Register(CTEBuilder{}, cteData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b CTEBuilder) PlaceholderFormat(f sq.PlaceholderFormat) CTEBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(CTEBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
// For most cases runner will be a database connection.
func (b CTEBuilder) RunWith(runner sq.BaseRunner) CTEBuilder {
	return setRunWith(b, runner).(CTEBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b CTEBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(cteData)
	return data.Exec()
}

// ToSql builds the query into a SQL string and bound args.
func (b CTEBuilder) ToSql() (string, []interface{}, error) {
	data := builder.GetStruct(b).(cteData)
	return data.ToSql()
}

// AuxStatements assigns auxillary statements to CTE.
func (b CTEBuilder) AuxStatements(auxStmts ...AuxBuilder) CTEBuilder {
	return builder.Extend(b, "AuxStatements", auxStmts).(CTEBuilder)
}
