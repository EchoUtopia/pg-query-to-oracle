package builder

import (
	"fmt"
	parser "github.com/EchoUtopia/pg2oracle/pkg/postgres"
	"github.com/pkg/errors"
	"reflect"
)

func (cb *CustomBuilder) convertWhere(where *parser.Where) error {
	if where == nil {
		return nil
	}
	switch where.Type {
	case `WHERE`:
		cond, err := convertExprToCond(where.Expr)
		if err != nil {
			return err
		}
		cb.Where(cond)
	case `HAVING`:
		return errors.Wrap(NotImplemented, `having`)
	}
	return nil
}

func convertExprToCond(expr parser.Expr) (Cond, error) {
	switch e := expr.(type) {
	case *parser.AndExpr:
		left, err := convertExprToCond(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertExprToCond(e.Right)
		if err != nil {
			return nil, err
		}
		return And(left, right), nil
	case *parser.OrExpr:
		left, err := convertExprToCond(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertExprToCond(e.Right)
		if err != nil {
			return nil, err
		}
		return Or(left, right), nil
	case *parser.ComparisonExpr:
		return convertComparisonExpr(e)
	case *parser.ParenExpr:
		return convertExprToCond(e.Expr)
	case *parser.RangeCond:
		return convertRangeCond(e)
	default:
		return nil, errors.Wrapf(NotImplemented, `sql: %s, type: %s`, e.String(), reflect.TypeOf(e))
	}
}

func convertRangeCond(expr *parser.RangeCond) (Cond, error) {
	left, ok := expr.Left.(parser.UnresolvedName)
	if !ok {
		return nil, errors.Wrap(NotImplemented, `RangeCond Left`)
	}
	leftStr, err := convertUnresolvedName(left)
	if err != nil {
		return nil, err
	}
	from, err := getValueFromExpr(expr.From)
	if err != nil {
		return nil, err
	}
	to, err := getValueFromExpr(expr.To)
	if err != nil {
		return nil, err
	}
	not := ``
	if expr.Not {
		not = ` NOT`
	}
	return Expr(fmt.Sprintf(`%s%s BETWEEN %s AND %s`, leftStr, not, getDisplayValue(from), getDisplayValue(to))), nil
}

func convertComparisonExpr(expr *parser.ComparisonExpr) (Cond, error) {
	if expr.SubOperator != 0 {
		return nil, errors.Wrap(NotImplemented, `subOperator`)
	}
	value, err := getValueFromExpr(expr.Right)
	if err != nil {
		return nil, err
	}
	leftValue, err := getExprDisplayValue(expr.Left)
	if err != nil {
		return nil, errors.Wrap(NotImplemented, `comparisonExpr Left`)
	}
	switch expr.Operator {
	case parser.EQ:
		return Eq{leftValue: value}, nil
	case parser.GT:
		return Gt{leftValue: value}, nil
	case parser.GE:
		return Gte{leftValue: value}, nil
	case parser.LT:
		return Lt{leftValue: value}, nil
	case parser.LE:
		return Lte{leftValue: value}, nil
	case parser.NE:
		return Neq{leftValue: value}, nil
	case parser.Is, parser.IsNot:
		if expr.Right != parser.DNull {
			return nil, errors.Wrap(NotImplemented, expr.Right.String())
		}
		if expr.Operator == parser.Is {
			return IsNull{leftValue}, nil
		} else {
			return NotNull{leftValue}, nil
		}
	case parser.NotILike, parser.ILike:
		not := ``
		if expr.Operator == parser.NotILike {
			not = ` NOT`
		}
		return Expr(fmt.Sprintf(`UPPER(%s)%s LIKE UPPER(%s)`, leftValue, not, expr.Right.String())), nil
	default:
		return Expr(expr.String()), nil
	}
}
