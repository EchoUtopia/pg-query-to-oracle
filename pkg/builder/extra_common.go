package builder

import (
	"bytes"
	"encoding/json"
	"fmt"
	parser "github.com/EchoUtopia/pg2oracle/pkg/postgres"
	"github.com/pkg/errors"
	"regexp"
	"strings"
)

const (
	CustomPlaceHolder = `--cvted-phs`

	Join      = "JOIN"
	FullJoin  = "FULL JOIN"
	LeftJoin  = "LEFT JOIN"
	RightJoin = "RIGHT JOIN"
	CrossJoin = "CROSS JOIN"
	InnerJoin = "INNER JOIN"
)

var (
	NotImplemented     = errors.New(`not implemented`)
	TypeNotImplemented = errors.New(`type not implemented`)
)

func formatNode(nf parser.NodeFormatter) string {
	buf := bytes.NewBuffer(make([]byte, 0, 30))
	nf.Format(buf, parser.FmtSimple)
	return buf.String()
}

func (cb *CustomBuilder) convertAliasedTable(table *parser.AliasedTableExpr) (string, error) {
	ts := ``
	var err error
	switch t := table.Expr.(type) {
	case *parser.NormalizableTableName:
		ts, err = cb.convertNormalizableTableName(t)
		if err != nil {
			return ``, err
		}
	case *parser.Subquery:
		ncb := &CustomBuilder{
			Builder: &Builder{
				cond:    NewCond(),
				dialect: cb.dialect,
				optype:  selectType,
			},
		}
		if _, err := ncb.convertSelectStatement(t.Select); err != nil {
			return ``, err
		}
		ts, err = ncb.ToBoundSQL()
		if err != nil {
			return ``, err
		}
		ts = `(` + ts + `)`
	default:
		return ``, errors.Wrapf(NotImplemented, `convertAliasedTable: %#v`, t)
	}
	if cb.optype == selectType && table.As.Alias != `` {
		ts += ` ` + formatNode(table.As)
	}
	return ts, nil
}

var nowRe = regexp.MustCompile(`now\(\)`)
var funcSupported = map[string]bool{
	`now`:     true,
	`count`:   true,
	`extract`: true,
	`sum`:     true,
	`avg`:     true,
}

func convertFunc(expr *parser.FuncExpr) (Cond, error) {
	f := strings.ToLower(expr.Func.String())
	if !funcSupported[f] {
		return nil, errors.Wrap(NotImplemented, `func`)
	}
	if expr.Filter != nil || expr.WindowDef != nil {
		return nil, errors.Wrap(NotImplemented, `func filter or window`)
	}
	var builder strings.Builder
	if f == `extract` {
		builder.WriteString(`EXTRACT(`)
		builder.WriteString(strings.Trim(expr.Exprs[0].String(), `'`))
		builder.WriteString(` FROM `)
		p1 := expr.Exprs[1]
		switch v := p1.(type) {
		case parser.UnresolvedName:
			builder.WriteByte('"')
			str, err := convertUnresolvedName(v)
			if err != nil {
				return nil, err
			}
			builder.WriteString(str)
			builder.WriteString(`")`)
		case *parser.FuncExpr:
			f := v.Func.FunctionReference
			if _, ok := f.(parser.UnresolvedName); !ok {
				return nil, errors.Wrap(NotImplemented, `convertfunc`)
			}
			fs := f.String()
			if fs == `now`{
				fs = `SYSTIMESTAMP`
			}
			builder.WriteString(fs)
			builder.WriteByte(')')
		default:
			return nil, errors.Wrap(NotImplemented, `extract from `)
		}
	}else {
		builder.WriteString(expr.Func.FunctionReference.String())
		builder.WriteByte('(')
		if len(expr.Exprs) > 1 {
			return nil, errors.Wrap(NotImplemented, `convert func`)
		}
		fv, ok := expr.Exprs[0].(parser.UnresolvedName);
		if ! ok {
			return nil, errors.Wrap(NotImplemented, `convert func`)
		}
		str, err := convertUnresolvedName(fv)
		if err != nil {
			return nil, err
		}
		builder.WriteString(str)
		builder.WriteByte(')')
	}
	return Expr(builder.String()), nil
}

func (cb *CustomBuilder) convertNormalizableTableName(table *parser.NormalizableTableName) (string, error) {
	switch t := table.TableNameReference.(type) {
	case parser.UnresolvedName:
		return convertUnresolvedName(t)
	default:
		return ``, errors.Wrap(NotImplemented, `convertNormalizableTableName`)
	}
}

func (cb *CustomBuilder) convertFrom(from *parser.From) error {
	if from == nil {
		return nil
	}
	if from.AsOf.Expr != nil {
		return errors.Wrap(NotImplemented, `as of`)
	}
	if len(from.Tables) > 1 {
		return errors.Wrap(NotImplemented, `select multiple tables`)
	} else if len(from.Tables) == 1 {
		return cb.convertTable(from.Tables[0])
	}
	return nil
}

func (cb *CustomBuilder) convertTable(table parser.TableExpr) error {
	if table == nil {
		return nil
	}
	ts := ``
	var err error
	switch t := table.(type) {
	case *parser.AliasedTableExpr:
		ts, err = cb.convertAliasedTable(t)
		if err != nil {
			return err
		}
	case *parser.NormalizableTableName:
		ts, err = cb.convertNormalizableTableName(t)
		if err != nil {
			return err
		}
	case *parser.JoinTableExpr:
		if err := cb.convertJoin(t); err != nil {
			return err
		}
		ts = cb.from
	default:
		return errors.Wrap(NotImplemented, `convertTable`)
	}
	if cb.optype == insertType {
		cb.Into(ts)
	} else {
		cb.From(ts)
	}
	return nil
}

func getDisplayValue(vi interface{}) string {
	switch v := vi.(type) {
	case string:
		return Q(v)
	case expr:
		return v.sql
	default:
		if s, ok := vi.(fmt.Stringer); ok {
			return s.String()
		} else {
			return fmt.Sprintf(`%v`, v)
		}
	}
}

func getValueFromExpr(vi parser.Expr) (interface{}, error) {
	switch v := vi.(type) {
	case *parser.DTimestamp, *parser.DTimestampTZ:
		return formatNode(v), nil
	case *parser.NumVal:
		value, err := v.ParseValue()
		if err != nil {
			return nil, err
		}
		return value, nil
	case *parser.CastExpr:
		pattern := `CAST(%s AS %s)`
		ct, err := ConvertPGTypeToOracle(v.Type.String())
		if err != nil {
			return nil, err
		}
		value, err := getValueFromExpr(v.Expr)
		if err != nil {
			return nil, err
		}
		return Expr(fmt.Sprintf(pattern, getDisplayValue(value), ct)), nil

	case *parser.DString:
		return string(*v), nil
	case *parser.StrVal:
		return v.OriginalString(), nil
	case *parser.FuncExpr:
		return convertFunc(v)
	case *parser.BinaryExpr:
		return convertBinary(v)
	case *parser.ParenExpr:
		value, err := getValueFromExpr(v.Expr)
		if err != nil {
			return ``, err
		}
		return Expr(getDisplayValue(value)), nil
	case *parser.DBool:
		value := 1
		if ! *v {
			value = 0
		}
		return value, nil
	case parser.UnresolvedName:
		vs, err := convertUnresolvedName(v)
		if err != nil {
			return ``, err
		}
		return Expr(vs), nil
	default:
		// fmt.Println(`uncaught value of expr: `, reflect.TypeOf(vi), `value: `, v.String())
		return Expr(v.String()), nil
	}
}

func convertBinary(v *parser.BinaryExpr) (Cond, error) {
	left, err := getValueFromExpr(v.Left)
	if err != nil {
		return nil, err
	}
	right, err := getValueFromExpr(v.Right)
	if err != nil {
		return nil, err
	}
	return Expr(fmt.Sprintf(`%s %s %s`, getDisplayValue(left), binaryOpName[v.Operator], getDisplayValue(right))), nil
}

var PGOracleTypeMap = map[string]string{
	`CHAR`:                     `VARCHAR2(4000)`,
	`VARCHAR`:                  `VARCHAR2(4000)`,
	`TEXT`:                     `CLOB`,
	`JSON`:                     `CLOB`,
	`UUID`:                     `RAW`,
	`BYTEA`:                    `BLOB`,
	`NUMERIC`:                  `NUMBER`,
	`DECIMAL`:                  `NUMBER`,
	`INTEGER`:                  `NUMBER`,
	`INT`:                      `NUMBER`,
	`BIGINT`:                   `NUMBER`,
	`DEC`:                      `NUMBER`,
	`FLOAT4`:                   `FLOAT`,
	`FLOAT8`:                   `FLOAT`,
	`FLOAT`:                    `FLOAT`,
	`TIMESTAMP`:                `TIMESTAMP`,
	`TIMESTAMP WITH TIME ZONE`: `TIMESTAMP WITH TIME ZONE`,
	`DATE`:                     `DATE`,
}

func ConvertPGTypeToOracle(in string) (string, error) {
	idx := strings.Index(in, `NUMERIC`)
	if idx == 0 {
		return `NUMBER` + in[7:], nil
	}
	out, ok := PGOracleTypeMap[in]
	if !ok {
		return ``, errors.Wrap(NotImplemented, `convert `+in)
	}
	return out, nil
}

func (cb *CustomBuilder) convertJoin(expr *parser.JoinTableExpr) error {
	if cb.optype != selectType {
		return errors.Wrap(NotImplemented, `join support select only`)
	}
	bj := join{joinType: cleanJoinStr(expr.Join)}
	_, ok := expr.Left.(*parser.JoinTableExpr)
	if ok {
		if err := cb.convertJoin(expr.Left.(*parser.JoinTableExpr)); err != nil {
			return err
		}
	} else {
		if _, ok := expr.Left.(*parser.AliasedTableExpr); !ok {
			return errors.Wrapf(NotImplemented, `join table: %#v`, expr.Left)
		}
		ts, err := cb.convertAliasedTable(expr.Left.(*parser.AliasedTableExpr))
		if err != nil {
			return err
		}
		cb.From(ts)
	}
	if _, ok := expr.Right.(*parser.AliasedTableExpr); !ok {
		return errors.Wrapf(NotImplemented, `join table: %#v`, expr.Right)
	}
	ts, err := cb.convertAliasedTable(expr.Right.(*parser.AliasedTableExpr))
	if err != nil {
		return err
	}
	bj.joinTable = ts
	jCond, err := convertJoinCond(expr.Cond)
	if err != nil {
		return err
	}
	bj.joinCond = jCond
	cb.joins = append(cb.joins, bj)
	return nil
}

func convertJoinCond(cond parser.JoinCond) (Cond, error) {
	switch c := cond.(type) {
	case *parser.OnJoinCond:
		v, ok := c.Expr.(*parser.ComparisonExpr)
		if ! ok {
			return nil, errors.Wrap(NotImplemented, `join cond`)
		}
		return convertExprToCond(v)
	default:
		return nil, errors.Wrap(NotImplemented, `join cond `)
	}
}

func cleanJoinStr(in string) string {
	m := map[string]string{
		Join:      `INNER`,
		InnerJoin: `INNER`,
		LeftJoin:  `LEFT`,
		RightJoin: `RIGHT`,
		CrossJoin: `CROSS`,
	}
	return m[in]
}

func PrintInJson(data interface{}, ident string) error {
	mared, err := json.MarshalIndent(data, ``, ident)
	if err != nil {
		return err
	}
	fmt.Println(string(mared))
	return nil
}

var (
	oracleReservedWords = map[string]bool{
		"ACCESS":                    true,
		"ACCOUNT":                   true,
		"ACTIVATE":                  true,
		"ADD":                       true,
		"ADMIN":                     true,
		"ADVISE":                    true,
		"AFTER":                     true,
		"ALL":                       true,
		"ALL_ROWS":                  true,
		"ALLOCATE":                  true,
		"ALTER":                     true,
		"ANALYZE":                   true,
		"AND":                       true,
		"ANY":                       true,
		"ARCHIVE":                   true,
		"ARCHIVELOG":                true,
		"ARRAY":                     true,
		"AS":                        true,
		"ASC":                       true,
		"AT":                        true,
		"AUDIT":                     true,
		"AUTHENTICATED":             true,
		"AUTHORIZATION":             true,
		"AUTOEXTEND":                true,
		"AUTOMATIC":                 true,
		"BACKUP":                    true,
		"BECOME":                    true,
		"BEFORE":                    true,
		"BEGIN":                     true,
		"BETWEEN":                   true,
		"BFILE":                     true,
		"BITMAP":                    true,
		"BLOB":                      true,
		"BLOCK":                     true,
		"BODY":                      true,
		"BY":                        true,
		"CACHE":                     true,
		"CACHE_INSTANCES":           true,
		"CANCEL":                    true,
		"CASCADE":                   true,
		"CAST":                      true,
		"CFILE":                     true,
		"CHAINED":                   true,
		"CHANGE":                    true,
		"CHAR":                      true,
		"CHAR_CS":                   true,
		"CHARACTER":                 true,
		"CHECK":                     true,
		"CHECKPOINT":                true,
		"CHOOSE":                    true,
		"CHUNK":                     true,
		"CLEAR":                     true,
		"CLOB":                      true,
		"CLONE":                     true,
		"CLOSE":                     true,
		"CLOSE_CACHED_OPEN_CURSORS": true,
		"CLUSTER":                   true,
		"COALESCE":                  true,
		"COLUMN":                    true,
		"COLUMNS":                   true,
		"COMMENT":                   true,
		"COMMIT":                    true,
		"COMMITTED":                 true,
		"COMPATIBILITY":             true,
		"COMPILE":                   true,
		"COMPLETE":                  true,
		"COMPOSITE_LIMIT":           true,
		"COMPRESS":                  true,
		"COMPUTE":                   true,
		"CONNECT":                   true,
		"CONNECT_TIME":              true,
		"CONSTRAINT":                true,
		"CONSTRAINTS":               true,
		"CONTENTS":                  true,
		"CONTINUE":                  true,
		"CONTROLFILE":               true,
		"CONVERT":                   true,
		"COST":                      true,
		"CPU_PER_CALL":              true,
		"CPU_PER_SESSION":           true,
		"CREATE":                    true,
		"CURRENT":                   true,
		"CURRENT_SCHEMA":            true,
		"CURREN_USER":               true,
		"CURSOR":                    true,
		"CYCLE":                     true,
		"DANGLING":                  true,
		"DATABASE":                  true,
		"DATAFILE":                  true,
		"DATAFILES":                 true,
		"DATAOBJNO":                 true,
		"DATE":                      true,
		"DBA":                       true,
		"DBHIGH":                    true,
		"DBLOW":                     true,
		"DBMAC":                     true,
		"DEALLOCATE":                true,
		"DEBUG":                     true,
		"DEC":                       true,
		"DECIMAL":                   true,
		"DECLARE":                   true,
		"DEFAULT":                   true,
		"DEFERRABLE":                true,
		"DEFERRED":                  true,
		"DEGREE":                    true,
		"DELETE":                    true,
		"DEREF":                     true,
		"DESC":                      true,
		"DIRECTORY":                 true,
		"DISABLE":                   true,
		"DISCONNECT":                true,
		"DISMOUNT":                  true,
		"DISTINCT":                  true,
		"DISTRIBUTED":               true,
		"DML":                       true,
		"DOUBLE":                    true,
		"DROP":                      true,
		"DUMP":                      true,
		"EACH":                      true,
		"ELSE":                      true,
		"ENABLE":                    true,
		"END":                       true,
		"ENFORCE":                   true,
		"ENTRY":                     true,
		"ESCAPE":                    true,
		"EXCEPT":                    true,
		"EXCEPTIONS":                true,
		"EXCHANGE":                  true,
		"EXCLUDING":                 true,
		"EXCLUSIVE":                 true,
		"EXECUTE":                   true,
		"EXISTS":                    true,
		"EXPIRE":                    true,
		"EXPLAIN":                   true,
		"EXTENT":                    true,
		"EXTENTS":                   true,
		"EXTERNALLY":                true,
		"FAILED_LOGIN_ATTEMPTS":     true,
		"FALSE":                     true,
		"FAST":                      true,
		"FILE":                      true,
		"FIRST_ROWS":                true,
		"FLAGGER":                   true,
		"FLOAT":                     true,
		"FLOB":                      true,
		"FLUSH":                     true,
		"FOR":                       true,
		"FORCE":                     true,
		"FOREIGN":                   true,
		"FREELIST":                  true,
		"FREELISTS":                 true,
		"FROM":                      true,
		"FULL":                      true,
		"FUNCTION":                  true,
		"GLOBAL":                    true,
		"GLOBALLY":                  true,
		"GLOBAL_NAME":               true,
		"GRANT":                     true,
		"GROUP":                     true,
		"GROUPS":                    true,
		"HASH":                      true,
		"HASHKEYS":                  true,
		"HAVING":                    true,
		"HEADER":                    true,
		"HEAP":                      true,
		"IDENTIFIED":                true,
		"IDGENERATORS":              true,
		"IDLE_TIME":                 true,
		"IF":                        true,
		"IMMEDIATE":                 true,
		"IN":                        true,
		"INCLUDING":                 true,
		"INCREMENT":                 true,
		"INDEX":                     true,
		"INDEXED":                   true,
		"INDEXES":                   true,
		"INDICATOR":                 true,
		"IND_PARTITION":             true,
		"INITIAL":                   true,
		"INITIALLY":                 true,
		"INITRANS":                  true,
		"INSERT":                    true,
		"INSTANCE":                  true,
		"INSTANCES":                 true,
		"INSTEAD":                   true,
		"INT":                       true,
		"INTEGER":                   true,
		"INTERMEDIATE":              true,
		"INTERSECT":                 true,
		"INTO":                      true,
		"IS":                        true,
		"ISOLATION":                 true,
		"ISOLATION_LEVEL":           true,
		"KEEP":                      true,
		"KEY":                       true,
		"KILL":                      true,
		"LABEL":                     true,
		"LAYER":                     true,
		"LESS":                      true,
		"LEVEL":                     true,
		"LIBRARY":                   true,
		"LIKE":                      true,
		"LIMIT":                     true,
		"LINK":                      true,
		"LIST":                      true,
		"LOB":                       true,
		"LOCAL":                     true,
		"LOCK":                      true,
		"LOCKED":                    true,
		"LOG":                       true,
		"LOGFILE":                   true,
		"LOGGING":                   true,
		"LOGICAL_READS_PER_CALL":    true,
		"LOGICAL_READS_PER_SESSION": true,
		"LONG":                      true,
		"MANAGE":                    true,
		"MASTER":                    true,
		"MAX":                       true,
		"MAXARCHLOGS":               true,
		"MAXDATAFILES":              true,
		"MAXEXTENTS":                true,
		"MAXINSTANCES":              true,
		"MAXLOGFILES":               true,
		"MAXLOGHISTORY":             true,
		"MAXLOGMEMBERS":             true,
		"MAXSIZE":                   true,
		"MAXTRANS":                  true,
		"MAXVALUE":                  true,
		"MIN":                       true,
		"MEMBER":                    true,
		"MINIMUM":                   true,
		"MINEXTENTS":                true,
		"MINUS":                     true,
		"MINVALUE":                  true,
		"MLSLABEL":                  true,
		"MLS_LABEL_FORMAT":          true,
		"MODE":                      true,
		"MODIFY":                    true,
		"MOUNT":                     true,
		"MOVE":                      true,
		"MTS_DISPATCHERS":           true,
		"MULTISET":                  true,
		"NATIONAL":                  true,
		"NCHAR":                     true,
		"NCHAR_CS":                  true,
		"NCLOB":                     true,
		"NEEDED":                    true,
		"NESTED":                    true,
		"NETWORK":                   true,
		"NEW":                       true,
		"NEXT":                      true,
		"NOARCHIVELOG":              true,
		"NOAUDIT":                   true,
		"NOCACHE":                   true,
		"NOCOMPRESS":                true,
		"NOCYCLE":                   true,
		"NOFORCE":                   true,
		"NOLOGGING":                 true,
		"NOMAXVALUE":                true,
		"NOMINVALUE":                true,
		"NONE":                      true,
		"NOORDER":                   true,
		"NOOVERRIDE":                true,
		"NOPARALLEL":                true,
		"NOREVERSE":                 true,
		"NORMAL":                    true,
		"NOSORT":                    true,
		"NOT":                       true,
		"NOTHING":                   true,
		"NOWAIT":                    true,
		"NULL":                      true,
		"NUMBER":                    true,
		"NUMERIC":                   true,
		"NVARCHAR2":                 true,
		"OBJECT":                    true,
		"OBJNO":                     true,
		"OBJNO_REUSE":               true,
		"OF":                        true,
		"OFF":                       true,
		"OFFLINE":                   true,
		"OID":                       true,
		"OIDINDEX":                  true,
		"OLD":                       true,
		"ON":                        true,
		"ONLINE":                    true,
		"ONLY":                      true,
		"OPCODE":                    true,
		"OPEN":                      true,
		"OPTIMAL":                   true,
		"OPTIMIZER_GOAL":            true,
		"OPTION":                    true,
		"OR":                        true,
		"ORDER":                     true,
		"ORGANIZATION":              true,
		"OSLABEL":                   true,
		"OVERFLOW":                  true,
		"OWN":                       true,
		"PACKAGE":                   true,
		"PARALLEL":                  true,
		"PARTITION":                 true,
		"PASSWORD":                  true,
		"PASSWORD_GRACE_TIME":       true,
		"PASSWORD_LIFE_TIME":        true,
		"PASSWORD_LOCK_TIME":        true,
		"PASSWORD_REUSE_MAX":        true,
		"PASSWORD_REUSE_TIME":       true,
		"PASSWORD_VERIFY_FUNCTION":  true,
		"PCTFREE":                   true,
		"PCTINCREASE":               true,
		"PCTTHRESHOLD":              true,
		"PCTUSED":                   true,
		"PCTVERSION":                true,
		"PERCENT":                   true,
		"PERMANENT":                 true,
		"PLAN":                      true,
		"PLSQL_DEBUG":               true,
		"POST_TRANSACTION":          true,
		"PRECISION":                 true,
		"PRESERVE":                  true,
		"PRIMARY":                   true,
		"PRIOR":                     true,
		"PRIVATE":                   true,
		"PRIVATE_SGA":               true,
		"PRIVILEGE":                 true,
		"PRIVILEGES":                true,
		"PROCEDURE":                 true,
		"PROFILE":                   true,
		"PUBLIC":                    true,
		"PURGE":                     true,
		"QUEUE":                     true,
		"QUOTA":                     true,
		"RANGE":                     true,
		"RAW":                       true,
		"RBA":                       true,
		"READ":                      true,
		"READUP":                    true,
		"REAL":                      true,
		"REBUILD":                   true,
		"RECOVER":                   true,
		"RECOVERABLE":               true,
		"RECOVERY":                  true,
		"REF":                       true,
		"REFERENCES":                true,
		"REFERENCING":               true,
		"REFRESH":                   true,
		"RENAME":                    true,
		"REPLACE":                   true,
		"RESET":                     true,
		"RESETLOGS":                 true,
		"RESIZE":                    true,
		"RESOURCE":                  true,
		"RESTRICTED":                true,
		"RETURN":                    true,
		"RETURNING":                 true,
		"REUSE":                     true,
		"REVERSE":                   true,
		"REVOKE":                    true,
		"ROLE":                      true,
		"ROLES":                     true,
		"ROLLBACK":                  true,
		"ROW":                       true,
		"ROWID":                     true,
		"ROWNUM":                    true,
		"ROWS":                      true,
		"RULE":                      true,
		"SAMPLE":                    true,
		"SAVEPOINT":                 true,
		"SB4":                       true,
		"SCAN_INSTANCES":            true,
		"SCHEMA":                    true,
		"SCN":                       true,
		"SCOPE":                     true,
		"SD_ALL":                    true,
		"SD_INHIBIT":                true,
		"SD_SHOW":                   true,
		"SEGMENT":                   true,
		"SEG_BLOCK":                 true,
		"SEG_FILE":                  true,
		"SELECT":                    true,
		"SEQUENCE":                  true,
		"SERIALIZABLE":              true,
		"SESSION":                   true,
		"SESSION_CACHED_CURSORS":    true,
		"SESSIONS_PER_USER":         true,
		"SET":                       true,
		"SHARE":                     true,
		"SHARED":                    true,
		"SHARED_POOL":               true,
		"SHRINK":                    true,
		"SIZE":                      true,
		"SKIP":                      true,
		"SKIP_UNUSABLE_INDEXES":     true,
		"SMALLINT":                  true,
		"SNAPSHOT":                  true,
		"SOME":                      true,
		"SORT":                      true,
		"SPECIFICATION":             true,
		"SPLIT":                     true,
		"SQL_TRACE":                 true,
		"STANDBY":                   true,
		"START":                     true,
		"STATEMENT_ID":              true,
		"STATISTICS":                true,
		"STOP":                      true,
		"STORAGE":                   true,
		"STORE":                     true,
		"STRUCTURE":                 true,
		"SUCCESSFUL":                true,
		"SWITCH":                    true,
		"SYS_OP_ENFORCE_NOT_NULL$":  true,
		"SYS_OP_NTCIMG$":            true,
		"SYNONYM":                   true,
		"SYSDATE":                   true,
		"SYSDBA":                    true,
		"SYSOPER":                   true,
		"SYSTEM":                    true,
		"TABLE":                     true,
		"TABLES":                    true,
		"TABLESPACE":                true,
		"TABLESPACE_NO":             true,
		"TABNO":                     true,
		"TEMPORARY":                 true,
		"THAN":                      true,
		"THE":                       true,
		"THEN":                      true,
		"THREAD":                    true,
		"TIMESTAMP":                 true,
		"TIME":                      true,
		"TO":                        true,
		"TOPLEVEL":                  true,
		"TRACE":                     true,
		"TRACING":                   true,
		"TRANSACTION":               true,
		"TRANSITIONAL":              true,
		"TRIGGER":                   true,
		"TRIGGERS":                  true,
		"TRUE":                      true,
		"TRUNCATE":                  true,
		"TX":                        true,
		"TYPE":                      true,
		"UB2":                       true,
		"UBA":                       true,
		"UID":                       true,
		"UNARCHIVED":                true,
		"UNDO":                      true,
		"UNION":                     true,
		"UNIQUE":                    true,
		"UNLIMITED":                 true,
		"UNLOCK":                    true,
		"UNRECOVERABLE":             true,
		"UNTIL":                     true,
		"UNUSABLE":                  true,
		"UNUSED":                    true,
		"UPDATABLE":                 true,
		"UPDATE":                    true,
		"USAGE":                     true,
		"USE":                       true,
		"USER":                      true,
		"USING":                     true,
		"VALIDATE":                  true,
		"VALIDATION":                true,
		"VALUE":                     true,
		"VALUES":                    true,
		"VARCHAR":                   true,
		"VARCHAR2":                  true,
		"VARYING":                   true,
		"VIEW":                      true,
		"WHEN":                      true,
		"WHENEVER":                  true,
		"WHERE":                     true,
		"WITH":                      true,
		"WITHOUT":                   true,
		"WORK":                      true,
		"WRITE":                     true,
		"WRITEDOWN":                 true,
		"WRITEUP":                   true,
		"XID":                       true,
		"YEAR":                      true,
		"ZONE":                      true,
	}
)

func getExprDisplayValue(expr parser.Expr) (string, error){
	value, err := getValueFromExpr(expr)
	if err != nil {
		return ``, err
	}
	return getDisplayValue(value), nil
}
