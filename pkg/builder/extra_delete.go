package builder

import (
	parser "github.com/EchoUtopia/pg2oracle/pkg/postgres"
	"github.com/pkg/errors"
)

func (cb *CustomBuilder) convertDelete(delete *parser.Delete) error {
	if err := cb.convertTable(delete.Table); err != nil {
		return err
	}
	if err := cb.convertWhere(delete.Where); err != nil {
		return err
	}
	if delete.Returning != nil {
		return errors.Wrap(NotImplemented, `delete returning`)
	}
	return nil
}
