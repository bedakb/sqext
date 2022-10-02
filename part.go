package sqext

import (
	"io"

	sq "github.com/Masterminds/squirrel"
)

func appendToSql(q sq.Sqlizer, w io.Writer, args []interface{}) ([]interface{}, error) {
	sql, qArgs, err := q.ToSql()
	if err != nil {
		return nil, err
	}
	if sql == "" {
		return nil, nil
	}

	_, err = io.WriteString(w, sql)
	if err != nil {
		return nil, err
	}

	args = append(args, qArgs...)
	return args, nil
}
