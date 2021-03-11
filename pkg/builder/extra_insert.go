package builder

import (
	"fmt"
	parser "github.com/EchoUtopia/pg2oracle/pkg/postgres"
)

func (cb *CustomBuilder) convertInsert(insert *parser.Insert) error {
	if err := cb.convertTable(insert.Table); err != nil {
		return err
	}
	for _, v := range insert.Columns {
		cl, err := convertUnresolvedName(v)
		if err != nil {
			return err
		}
		cb.insertCols = append(cb.insertCols, cl)
	}
	if _, err := cb.convertSelect(insert.Rows); err != nil {
		return err
	}
	return nil
}

func (cb *CustomBuilder) getInsertValuesByCols(cols []string) ([]interface{}, error) {
	out := make([]interface{}, 0, len(cols))
	m := map[string]interface{}{}
	for k, v := range cb.insertCols {
		m[v] = cb.insertVals[k]
	}
	for _, v := range cols {
		iv, ok := m[v]
		if !ok {
			return nil, fmt.Errorf(`conflict column %s not in insert columns`, v)
		}
		out = append(out, iv)
	}
	return out, nil
}
