package builder

import (
	parser "github.com/EchoUtopia/pg2oracle/pkg/postgres"
	"github.com/pkg/errors"
	"strings"
)

func (cb *CustomBuilder) convertSelectStatement(stmt parser.SelectStatement) (*CustomBuilder, error) {
	switch s := stmt.(type) {
	case *parser.ValuesClause:
		if cb.optype == insertType {
			if len(s.Tuples) > 1 || len(s.Tuples) == 0 {
				return nil, errors.New(`only one values supported`)
			}
			tuple := s.Tuples[0]
			for _, v := range tuple.Exprs {
				value, err := getValueFromExpr(v)
				if err != nil {
					return nil, err
				}
				cb.insertVals = append(cb.insertVals, value)
			}
		}
		return cb, nil
	case *parser.SelectClause:
		columns, err := cb.convertSelectExpr(s.Exprs)
		if err != nil {
			return nil, err
		}
		if err := cb.convertFrom(s.From); err != nil {
			return nil, err
		}
		if err := cb.convertWhere(s.Where); err != nil {
			return nil, err
		}
		cb.convertGroupBy(s.GroupBy)

		if s.Distinct {
			columns = `DISTINCT ` + columns
		}
		cb.Select(columns)
		return cb, nil
	case *parser.UnionClause:
		if err := cb.convertUnion(s); err != nil {
			return nil, err
		}
		return cb, nil
	case *parser.ParenSelect:
		return cb.convertSelect(s.Select)
	default:
		return nil, errors.Wrapf(NotImplemented, "%#v\n", s)
	}
}

func (cb *CustomBuilder) convertSelect(slt *parser.Select) (*CustomBuilder, error) {
	if slt == nil {
		return nil, nil
	}
	if err := cb.convertOrderBy(slt.OrderBy);err != nil {
		return nil, err
	}
	if err := cb.convertLimit(slt.Limit); err != nil {
		return nil, err
	}
	return cb.convertSelectStatement(slt.Select)
}

func (cb *CustomBuilder) convertUnion(expr *parser.UnionClause) error {

	if _, err := cb.convertSelect(expr.Right); err != nil {
		return err
	}
	ncb := &CustomBuilder{
		Builder: &Builder{
			cond:    NewCond(),
			dialect: cb.dialect,
		},
	}
	left, err := ncb.convertSelect(expr.Left)
	if err != nil {
		return err
	}
	distinctType := ``
	if expr.All {
		distinctType = `ALL`
	}
	switch expr.Type {
	case parser.UnionOp:
		cb.Builder = cb.Union(distinctType, left.Builder)
	case parser.IntersectOp:
		cb.Builder = cb.Intersect(distinctType, left.Builder)
	case parser.ExceptOp:
		cb.Builder = cb.Except(distinctType, left.Builder)
	default:
		return errors.New(`invalid union type`)
	}
	return nil
}

func (cb *CustomBuilder) convertSelectExpr(exprs parser.SelectExprs) (string, error) {
	convertedCols := ``

	for k, v := range exprs {
		if k > 0 {
			convertedCols += `, `
		}
		switch t := v.Expr.(type) {
		case *parser.CastExpr:
			c, err := getValueFromExpr(t)
			if err != nil {
				return ``, err
			}
			convertedCols += getDisplayValue(c)
		case *parser.CaseExpr:
			if _, ok := t.Expr.(*parser.CastExpr); ok {
				return ``, errors.Wrap(NotImplemented, `cast in case`)
			}
			convertedCols += t.String()
		case *parser.FuncExpr:
			cond, err := convertFunc(t)
			if err != nil {
				return ``, err
			}
			return getDisplayValue(cond), nil
		case *parser.BinaryExpr:
			cond, err := convertBinary(t)
			if err != nil {
				return ``, err
			}
			return getDisplayValue(cond), nil
		case parser.UnresolvedName:
			c, err := convertUnresolvedName(t)
			if err != nil {
				return ``, err
			}
			convertedCols += c
		default:
			convertedCols += t.String()
		}
		if v.As != `` {
			convertedCols += ` ` + string(v.As)
		}
	}
	return convertedCols, nil
}

var binaryOpName = [...]string{
	parser.Bitand:   "&",
	parser.Bitor:    "|",
	parser.Bitxor:   "#",
	parser.Plus:     "+",
	parser.Minus:    "-",
	parser.Mult:     "*",
	parser.Div:      "/",
	parser.FloorDiv: "//",
	parser.Mod:      "%",
	parser.Pow:      "^",
	parser.Concat:   "||",
	parser.LShift:   "<<",
	parser.RShift:   ">>",
}

func (cb *CustomBuilder) convertOrderBy(orders parser.OrderBy) error{
	if len(orders) == 0 {
		return nil
	}
	var builder strings.Builder
	for k, v := range orders {
		if v.OrderType != 0 || v.Table.TableNameReference != nil {
			return errors.Wrap(NotImplemented, `order by`)
		}
		expr, ok := v.Expr.(parser.UnresolvedName)
		if !ok {
			return errors.Wrap(NotImplemented, ` order by`)
		}
		clStr, err := convertUnresolvedName(expr)
		if err != nil {
			return err
		}
		builder.WriteString(clStr)
		if k != len(orders)-1{
			builder.WriteString(`, `)
		}
	}

	cb.OrderBy(builder.String())
	return nil
}

func (cb *CustomBuilder) convertLimit(limit *parser.Limit) error {
	if limit == nil {
		return nil
	}
	o := []int{}
	if limit.Offset != nil {
		oi, err := getValueFromExpr(limit.Offset)
		if err != nil {
			return err
		}
		o = append(o, int(oi.(int64)))
	}
	li, err := getValueFromExpr(limit.Count)
	if err != nil {
		return err
	}
	l := int(li.(int64))
	cb.Limit(l, o...)
	return nil
}

func (cb *CustomBuilder) convertGroupBy(groupBy parser.GroupBy) {
	if groupBy == nil {
		return
	}
	cb.GroupBy(`"` + formatNode(groupBy)[10:] + `"`)
}

func  convertUnresolvedName(name parser.UnresolvedName) (string, error){
	builder := strings.Builder{}
	if _, ok := name[0].(parser.Name); !ok {
		return ``, errors.Wrap(NotImplemented, `convertUnresolvedName`)
	}
	if len(name) == 1 {
		builder.WriteByte('"')
		builder.WriteString(name.String())
		builder.WriteByte('"')
	}else if len(name) == 2 {
		if _, ok := name[1].(parser.Name); !ok {
			return ``, errors.Wrap(NotImplemented, `convertUnresolvedName`)
		}
		first := name[0].(parser.Name)
		second := name[1].(parser.Name)
		builder.WriteString(first.String())
		builder.WriteString(`."`)
		builder.WriteString(strings.Trim(second.String(), `"`))
		builder.WriteByte('"')

	}else {
		return ``, errors.Wrap(NotImplemented, `unResolvedName len`)
	}
	return builder.String(), nil

}
