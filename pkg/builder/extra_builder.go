package builder

import (
	"bytes"
	"fmt"
	parser "github.com/EchoUtopia/pg2oracle/pkg/postgres"
	"github.com/pkg/errors"
	"strings"
	"text/template"
)

const (
	ConflictNoAction     = `noAction`
	ConflictUpdateAction = `updateAction`
)

type CustomBuilder struct {
	*Builder
	Returning    parser.ReturningExprs
	OnConflict   *parser.OnConflict
	CustomSqlStr string
	InTx         bool
	*onConflictOracleParams
}

func (cb *CustomBuilder) ToBoundSQL() (string, error) {
	if cb.CustomSqlStr != `` {
		return convertPlaceHolder(cb.CustomSqlStr)
	}
	innerSql, err := cb.Builder.ToBoundSQL()
	if err != nil {
		return ``, err
	}
	return convertPlaceHolder(innerSql)
}

var startTransactionDialect = map[string]string{
	ORACLE: `Savepoint a;`,
}

func (cb *CustomBuilder) convertReturning(returning parser.ReturningClause) error {
	if _, ok := returning.(*parser.NoReturningClause); ok {
		return nil
	}
	if _, ok := returning.(*parser.ReturningNothing); ok {
		return nil
	}
	if cb.optype == insertType {

		if len(cb.OnConflict.Columns) == 0 {
			return errors.Wrap(NotImplemented, `on conflict must specify columns`)
		}
		if cb.onConflictOracleParams == nil {
			return errors.Wrap(NotImplemented, `only support returning when on conflict`)
		}
		if len(cb.selects) > 0 {
			return errors.Wrap(NotImplemented, `insert ** select *** returning *** not supported`)
		}
	}
	rcb := &CustomBuilder{
		Builder: &Builder{
			cond:    NewCond(),
			dialect: cb.dialect,
		},
	}
	columns, err := cb.convertSelectExpr(parser.SelectExprs(*returning.(*parser.ReturningExprs)))
	if err != nil {
		return err
	}
	rcb.Select(columns)
	if strings.Contains(columns, `excluded.`) {
		return errors.New(`returning not support excluded table`)
	}
	if cb.optype == updateType {
		rcb.cond = cb.cond
		rcb.from = cb.from
	} else {
		columns := []string{}
		for _, v := range cb.OnConflict.Columns {
			columns = append(columns, string(v))
		}
		ivs, err := cb.getInsertValuesByCols(columns)
		if err != nil {
			return err
		}
		eq := Eq{}
		for k, v := range columns {
			if ivs[k] == parser.DNull {
				rcb.And(IsNull{v})
				continue
			}
			eq[v] = ivs[k]
		}
		rcb.from = cb.into
		rcb.Where(eq)
	}
	sql, err := rcb.ToBoundSQL()
	if err != nil {
		return err
	}
	os := cb.CustomSqlStr
	if os == `` {
		os, err = cb.ToBoundSQL()
		if err != nil {
			return err
		}
	}
	ns := ``
	if !cb.InTx {
		ns += startTransactionDialect[cb.dialect] + "\n"
	}
	ns += os + `;
`
	ns += sql
	if !cb.InTx {
		ns += `;
commit;`
	}
	cb.CustomSqlStr = ns
	return nil
}

type onConflictOracleParams struct {
	TableName     string
	UsingValues   string
	OnCondition   string
	UpdateValues  string
	DoNothing     bool
	InsertColumns string
	InsertValues  string
}

var OnConflictTemplate *template.Template

func init() {
	var err error
	OnConflictTemplate, err = template.New(`upsert`).Parse(onConflictTemplateStr)
	if err != nil {
		panic(err)
	}
}

const onConflictTemplateStr = `MERGE INTO {{.TableName}} t
USING (select {{.UsingValues}} FROM DUAL) s
ON ({{.OnCondition}})
{{if .DoNothing}}
{{else}}WHEN MATCHED THEN
UPDATE SET {{.UpdateValues}}
{{end}}WHEN NOT MATCHED THEN
INSERT ({{.InsertColumns}}) VALUES({{.InsertValues}})`

func (cb *CustomBuilder) convertOnConflict(insert *parser.Insert) (string, error) {
	if cb.dialect != ORACLE {
		return ``, errors.Wrap(NotImplemented, `can only convert on conflict to oracle`)
	}
	params := &onConflictOracleParams{
		TableName:     cb.into,
		UsingValues:   "",
		OnCondition:   ``,
		UpdateValues:  "",
		DoNothing:     insert.OnConflict.DoNothing,
		InsertColumns: "",
		InsertValues:  "",
	}
	cb.onConflictOracleParams = params
	cb.OnConflict = insert.OnConflict
	if len(insert.OnConflict.Columns) == 0 {
		return ``, errors.New(`must specify on conflict columns`)
	}
	onConditionBuilder := &strings.Builder{}
	usingValuesBuilder := &strings.Builder{}
	updateValuesBuilder := NewWriter()
	insertValuesBuilder := &strings.Builder{}
	for k, v := range insert.OnConflict.Columns {
		if _, err := fmt.Fprintf(onConditionBuilder, `(SELECT t.%s FROM DUAL) = s.%s`, v, v); err != nil {
			return ``, err
		}
		if k != len(insert.OnConflict.Columns)-1 {
			if _, err := onConditionBuilder.WriteString(`, `); err != nil {
				return ``, err
			}
		}
	}
	eq, err := generateUpdateExpr(cb.OnConflict.Exprs)
	if err != nil {
		return ``, err
	}
	eqCount := 0
	for k, v := range eq {
		if _, err := fmt.Fprintf(updateValuesBuilder, `%s = %s`, k, getDisplayValue(v)); err != nil {
			return ``, err
		}
		if eqCount != len(eq)-1 {
			if _, err := updateValuesBuilder.WriteString(`, `); err != nil {
				return ``, err
			}
		}
		eqCount++
	}
	for k, v := range cb.insertCols {
		if _, err := fmt.Fprintf(usingValuesBuilder, `%s %s`, getDisplayValue(cb.insertVals[k]), v); err != nil {
			return ``, err
		}
		if _, err := fmt.Fprintf(insertValuesBuilder, `s.%s`, v); err != nil {
			return ``, err
		}
		// params.
		if k != len(cb.insertVals)-1 {
			usingValuesBuilder.WriteString(`, `)
			insertValuesBuilder.WriteString(`, `)
		}
	}
	params.OnCondition = onConditionBuilder.String()
	params.UsingValues = usingValuesBuilder.String()
	params.UpdateValues = updateValuesBuilder.String()
	params.InsertValues = insertValuesBuilder.String()

	params.InsertColumns = strings.Join(cb.insertCols, `,`)
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	if err := OnConflictTemplate.Execute(buf, params); err != nil {
		return ``, err
	}
	return buf.String(), nil
}

func Q(s string) string {
	s = strings.Replace(s, "'", "''", -1)
	s = strings.Replace(s, "\000", "", -1)
	return "'" + s + "'"
}
