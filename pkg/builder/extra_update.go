package builder

import (
	parser "github.com/EchoUtopia/pg2oracle/pkg/postgres"
	"github.com/pkg/errors"
)

func (cb *CustomBuilder) convertUpdate(update *parser.Update) error {
	if err := cb.convertTable(update.Table); err != nil {
		return err
	}
	eq, err := generateUpdateExpr(update.Exprs)
	if err != nil {
		return err
	}
	cb.Update(eq)
	cb.cond = NewCond()
	if update.Where != nil {
		if err := cb.convertWhere(update.Where); err != nil {
			return err
		}
	}
	if err := cb.convertReturning(update.Returning); err != nil {
		return err
	}
	return nil
}

func generateUpdateExpr(exprs parser.UpdateExprs) (Eq, error) {
	eq := make(Eq)
	for _, v := range exprs {
		if !v.Tuple {
			if len(v.Names) != 1 {
				return nil, errors.Wrap(NotImplemented, `generateUpdateExpr`)
			}
			value, err := getValueFromExpr(v.Expr)
			if err != nil {
				return nil, err
			}
			leftStr, err := convertUnresolvedName(v.Names[0])
			eq[leftStr] = value
		} else {
			for k, name := range v.Names {
				value, err := getValueFromExpr(v.Expr.(*parser.Tuple).Exprs[k])
				if err != nil {
					return nil, err
				}
				leftStr, err := convertUnresolvedName(name)
				eq[leftStr] = value
			}
		}
	}
	return eq, nil
}
