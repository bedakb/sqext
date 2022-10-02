package sqext

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/lann/builder"
)

// StatementBuilderType is the type of StatementBuilder.
type StatementBuilderType builder.Builder

// PlaceholderFormat sets the PlaceholderFormat field for any child builders.
func (b StatementBuilderType) PlaceholderFormat(f sq.PlaceholderFormat) StatementBuilderType {
	return builder.Set(b, "PlaceholderFormat", f).(StatementBuilderType)
}

// Aux returns a AuxBuilder for this StatementBuilderType.
func (b StatementBuilderType) Aux(stmt sq.Sqlizer) AuxBuilder {
	return AuxBuilder(b).Statement(stmt)
}

// CTE returns a CTEBuilder for this StatementBuilderType.
func (b StatementBuilderType) CTE(auxStmts ...AuxBuilder) CTEBuilder {
	return CTEBuilder(b).AuxStatements(auxStmts...)
}

// StatementBuilder is a parent builder for other builders.
var StatementBuilder = StatementBuilderType(builder.EmptyBuilder).PlaceholderFormat(sq.Question)

// Aux returns a new AuxBuilder with a given sq.Sqlizer.
//
// See AuxBuilder.Statement.
func Aux(stmt sq.Sqlizer) AuxBuilder {
	return StatementBuilder.Aux(stmt)
}

// CTE returns a new CTEBulder with a list of given auxillary statements.
//
// See AuxBuilder.Statments.
func CTE(auxStmts ...AuxBuilder) CTEBuilder {
	return StatementBuilder.CTE(auxStmts...)
}

func setRunWith(b interface{}, runner sq.BaseRunner) interface{} {
	switch r := runner.(type) {
	case sq.StdSqlCtx:
		runner = sq.WrapStdSqlCtx(r)
	case sq.StdSql:
		runner = sq.WrapStdSql(r)
	}
	return builder.Set(b, "RunWith", runner)
}
