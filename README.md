### How does it works?

i use the existing Postgres compatible parser from cockroachdb to generate the ast,

then i turn the ast to [xorm builder](http://xorm.io/builder), and use the builder to generate the translated sql


### The Postgres query parser:  

Most of the extracted codes origin of Postgres compatible parser: [/cockroachdb/cockroach/tree/master/pkg/sql/parser](https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/parser)


### Progress
the project is  WIP, now only limited grammar of postgres are supported, it's all from my project:

- `select a::int from b` will be translated to :


        SELECT CAST(a AS NUMBER) FROM b
        
- `insert into a(field1) values('value1') on conflict (field1) do update set b = 'value2'`:
        
        
        MERGE INTO a t
        USING (select 'value1' field1 FROM DUAL) s
        ON ((SELECT t.field1 FROM DUAL) = s.field1)
        WHEN MATCHED THEN
        UPDATE SET b = 'value2'
        WHEN NOT MATCHED THEN
        INSERT (field1) VALUES(s.field1)
        
- `insert into a(field1) values('value1') on conflict (field1) do update set b = 'value2' returning id`:

        
          Savepoint a;
          MERGE INTO a t
          USING (select 'value1' field1 FROM DUAL) s
          ON ((SELECT t.field1 FROM DUAL) = s.field1)
          WHEN MATCHED THEN
          UPDATE SET b = 'value2'
          WHEN NOT MATCHED THEN
          INSERT (field1) VALUES(s.field1);
          SELECT id FROM a WHERE field1='value1';
          commit;

- `update a set b = 'value1' where c = 'value2' returning e`:


        Savepoint a;
        UPDATE a SET b='value1' WHERE c='value2';
        SELECT e FROM a WHERE c='value2';
        commit;

    
- `update a set b = 'value1' where c = now()`
(there are plenty of functions that are different, to support them, more works are needed)

        
        UPDATE a SET b='value1' WHERE c=(SYSTIMESTAMP)
        
- `select title from tasks limit 1`

        
        SELECT title FROM (SELECT title,ROWNUM RN FROM tasks) at WHERE at.RN<=1
        

- `select title from tasks limit 1 offset 2`

        
        SELECT title FROM (SELECT * FROM (SELECT title,ROWNUM RN FROM tasks) at WHERE at.RN<=3) att WHERE att.RN>2

- `select * from tasks where title ilike 'sdf%'`:


        SELECT * FROM tasks WHERE UPPER(title)  LIKE UPPER('sdf%')
        
- `select (extract(year from now()) - extract(year from date_of_birth))::int from dual`:

       
        SELECT CAST(extract(year FROM SYSTIMESTAMP) - extract(year FROM date_of_birth) AS NUMBER) FROM dual

- `select count(distinct $1) from b`:
 
        
        SELECT count(DISTINCT :1) FROM b
        
- `update a set b = b+'1'::int`:
    
        
        UPDATE a SET b=(b + CAST('1' AS NUMBER))

### Not Supported Query

- most functions which has no corresponding function in oracle

- insert into *** returning id