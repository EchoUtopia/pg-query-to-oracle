package builder

import (
	"github.com/stretchr/testify/require"
	"testing"
)

var expected = map[string]string{
	`select title from a union select title from b`: `(SELECT title FROM b) UNION (SELECT title FROM a)`,

	`select '1'::int from b`: `SELECT CAST('1' AS NUMBER) FROM b`,

	`select aa.name from aa a join (select id, name from bb) b on a.id = b.id`: `SELECT aa."name" FROM aa a INNER JOIN (SELECT id, "name" FROM bb) b ON a.id = b.id`,

	`select * from tasks where title ilike 'sdf%'`: `SELECT * FROM tasks WHERE UPPER(title) LIKE UPPER('sdf%')`,

	`select (extract(year from now()) - extract(year from date_of_birth))::int from dual`: `SELECT CAST(extract(year FROM SYSTIMESTAMP) - extract(year FROM date_of_birth) AS NUMBER) FROM dual`,

	`select count(distinct $1) from b`: `SELECT count(DISTINCT :1) FROM b`,

	`update a set b = b+'1'::int`: `UPDATE a SET b=(b + CAST('1' AS NUMBER))`,

	`insert into a(field1) values('value1') on conflict (field1) do update set b = 'value2'`: `MERGE INTO a t
USING (select 'value1' field1 FROM DUAL) s
ON ((SELECT t.field1 FROM DUAL) = s.field1)
WHEN MATCHED THEN
UPDATE SET b = 'value2'
WHEN NOT MATCHED THEN
INSERT (field1) VALUES(s.field1)`,

	`insert into a(field1) values('value1') on conflict (field1) do update set b = $1`: `MERGE INTO a t
USING (select 'value1' field1 FROM DUAL) s
ON ((SELECT t.field1 FROM DUAL) = s.field1)
WHEN MATCHED THEN
UPDATE SET b = :1
WHEN NOT MATCHED THEN
INSERT (field1) VALUES(s.field1)`,

	`insert into a(field1) values('value1') on conflict (field1) do update set b = 'value2' returning id`: `Savepoint a;
MERGE INTO a t
USING (select 'value1' field1 FROM DUAL) s
ON ((SELECT t.field1 FROM DUAL) = s.field1)
WHEN MATCHED THEN
UPDATE SET b = 'value2'
WHEN NOT MATCHED THEN
INSERT (field1) VALUES(s.field1);
SELECT id FROM a WHERE field1='value1';
commit;`,
}

func convert(sql string) (string, error) {

	cb := &CustomBuilder{Builder: Oracle()}
	if err := cb.Convert(sql); err != nil {
		return ``, err
	}
	convertedSql, err := cb.ToBoundSQL()
	if err != nil {
		return ``, err
	}
	return convertedSql, nil
}

func TestConvert(t *testing.T) {
	for in, expected := range expected {
		converted, err := convert(in)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, expected, converted)
	}
}
