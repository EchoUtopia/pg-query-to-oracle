package builder

import (
	"errors"
	"fmt"
	parser "github.com/EchoUtopia/pg2oracle/pkg/postgres"
	"strings"
	"time"
)

var (
	MoreThanOneStatement = errors.New(`more than one statement`)
)

func convertDollar(input string) (string, error) {
	out := &strings.Builder{}
	currentPlaceHolder := false
	for k, v := range input {
		if v == '$' && k != len(input)-1 && input[k+1] <= '9' && input[k+1] >= '0' && k != 0 && input[k-1] != '\'' {
			if _, err := fmt.Fprintf(out, `'%s`, CustomPlaceHolder); err != nil {
				return ``, err
			}
			currentPlaceHolder = true
		} else {
			if (v > '9' || v < '0') && currentPlaceHolder {
				if _, err := out.WriteString(`'`); err != nil {
					return ``, err
				}
				currentPlaceHolder = false
			}
			if _, err := out.WriteRune(v); err != nil {
				return ``, err
			}
		}
	}
	if currentPlaceHolder {
		out.WriteByte('\'')
	}
	return out.String(), nil
}

func convertPlaceHolder(input string) (string, error) {
	out := &strings.Builder{}
	for idx := strings.Index(input, CustomPlaceHolder); idx != -1; idx = strings.Index(input, CustomPlaceHolder) {
		if _, err := fmt.Fprintf(out, `%s:arg`, input[:idx-1]); err != nil {
			return ``, err
		}
		idx += len(CustomPlaceHolder)
		for _, v := range input[idx:] {
			idx++
			if v <= '9' && v >= '0' {
				out.WriteRune(v)
			} else {
				break
			}
		}
		input = input[idx:]
	}
	if _, err := out.WriteString(input); err != nil {
		return ``, err
	}
	return out.String(), nil
}

// func (cb *CustomBuilder)CheckNeedConvert(input string) bool {
//
// }

func (cb *CustomBuilder) Convert(input string) error {
	start := time.Now()
	if cb.dialect != ORACLE {
		return fmt.Errorf(`dialect %s not supported`, cb.dialect)
	}
	// clean
	var err error
	input, err = convertDollar(input)
	if err != nil {
		return err
	}
	// fmt.Println(input)
	stmts, err := parser.Parse(input)
	if err != nil {
		return err
	}
	if len(stmts) > 1 {
		return MoreThanOneStatement
	}
	fmt.Printf("clean and parser cost: %s\n", time.Since(start))
	start = time.Now()
	stmt := stmts[0]
	switch st := stmt.(type) {
	case parser.SelectStatement:
		_, ok := st.(*parser.SelectClause)
		if !ok {
			return errors.New(`not select struct`)
		}

	case *parser.Insert:
		cb.optype = insertType
		cb.OnConflict = st.OnConflict
		if err := cb.convertInsert(st); err != nil {
			return err
		}
		if st.OnConflict != nil {
			sqlStr, err := cb.convertOnConflict(st)
			if err != nil {
				return err
			}
			cb.CustomSqlStr = sqlStr
		}
		if err := cb.convertReturning(st.Returning); err != nil {
			return err
		}
	case *parser.Update:
		cb.optype = updateType
		if err := cb.convertUpdate(st); err != nil {
			return err
		}
	case *parser.Delete:
		cb.optype = deleteType
		if err := cb.convertDelete(st); err != nil {
			return err
		}
	case *parser.Select:
		cb.optype = selectType
		if _, err := cb.convertSelect(st); err != nil {
			return err
		}
	}
	fmt.Printf("convert cost: %s\n", time.Since(start))
	return nil
}
