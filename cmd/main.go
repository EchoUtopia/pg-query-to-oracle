// Command qbcli takes 2 arguments: an SQL string to parse into Go code in the
// format that the sql/parser uses and a boolean indicating if the parsed query
// should also be printed as SQL.
// DISCLAIMER: This is a work in progress, currently only tested with a very
// narrow set of queries.
package main

import (
	"flag"
	"fmt"
	"github.com/EchoUtopia/pg2oracle/pkg/builder"
	"log"
	"os"
	"time"

	postgresGenerator "github.com/EchoUtopia/pg2oracle/pkg/generators/postgres"
	postgresParser "github.com/EchoUtopia/pg2oracle/pkg/postgres"
)

var (
	syntax   string = "postgres" // mysql or postgres
	sql      string = `SELECT 
       up.full_name,up.email,up.subject,fmd.api_url,up.is_active,up.title,up.user_id,
       up.about_me,up.nationality, up.date_of_birth,up.gender,
       up.resident_card_no,up.phone_mobile,up.phone_home,up.country_id,
       up.state_id,up.city_id,up.created_date,up.changed_date,up.deleted_date,
       up.groups,up.display_name,up.disabilities,up.marital_status,up.driving_license_no,
       up.driving_license_date,up.driving_license_type,up.passport_no,up.tax_no,up.vat_no,
       up.street_address,up.postal_code,up.phone_work,up.metadata,up.is_contact_info_visible,up.primary_language,up.device_token,up.access_token_count,up.dependents_count,
       c.name,r.name
       FROM user_profile as up 
       LEFT JOIN user_company_role as ucr on ucr.user_id = up.user_id
       LEFT JOIN file_manager_document as fmd on fmd.id = up.photo
       LEFT JOIN company as c on c.id = ucr.company_id
       LEFT JOIN role as r on r.id = ucr.role_id
       left join user_tags as ut on ut.subject = up.subject and taxonomy = $1 and reference_id = 1  WHERE up.subject IS NOT NULL  AND  up.full_name ilike '%' || $2 || '%' AND 
up.full_name  in ($3,$4)  AND 
up.user_id = 999 AND 
up.user_id  in ($5,$6)  AND 
up.subject ilike '%' || $7 || '%' AND 
up.subject  in ($8,$9)  AND 
up.email ilike '%' || $10 || '%'  AND 
up.email  in ($11,$12)  AND 
up.gender ilike '%' || $13 || '%'  AND 
up.gender  in ($14,$15)  AND 
up.title ilike '%' || $16 || '%'  AND 
up.title  in ($17,$18)  AND 
up.nationality ilike '%' || $19 || '%'  AND 
up.nationality  in ($20,$21)  AND 
up.resident_card_no ilike '%' || $22 || '%'  AND 
up.resident_card_no  in ($23,$24)  AND 
up.country_id =1 AND 
up.country_id  in ($25,$26)  AND 
up.city_id =24076 AND 
up.city_id  in ($27,$28)  AND 
up.state_id =3424 AND 
up.state_id  in ($29,$30)  AND 
(up.phone_mobile ilike '%' || $31 || '%') AND 
up.phone_mobile ilike '%' || $32 || '%'  AND 
up.phone_mobile  = $33  AND 
up.user_id 
    IN (
     SELECT user_id FROM employment emp
     JOIN company AS c ON emp.company_id = c.id
     WHERE c.name ilike '%' || $34 || '%'
     union
     SELECT user_id FROM experience WHERE company_name ilike '%' || $35 || '%'
    ) AND 
up.user_id 
    IN ( 
     SELECT user_id FROM employment emp JOIN company AS c ON emp.company_id = c.id WHERE c.name  in ($36,$37) 
     union
     SELECT user_id FROM experience WHERE company_name  in ($38,$39)  
    ) AND 
up.user_id 
    IN (
     SELECT user_id FROM employment emp JOIN company AS c ON emp.company_id = c.id WHERE emp.is_current = true AND c.name  in ($40,$41) 
     union
     SELECT user_id FROM experience WHERE is_current = true AND company_name  in ($42,$43) 
    ) AND 
up.user_id IN (SELECT employment.user_id FROM employment
    JOIN company ON employment.company_id = company.id 
    WHERE company.id  in ($44,$45) ) AND 
 up.user_id in (select user_id from user_career_aspiration where  salary_range_min > $46  AND  salary_range_max < $47  AND contract_type_id  = $48  AND employment_type_id  in ($49,$50)  AND industry_id  = $51 ) AND 
 up.user_id in (select user_id from user_education where degree_level_id  in ($52,$53)  AND university_name  in ($54,$55)  AND gpa < $56 AND gpa > $57 AND percentage < $58 AND percentage > $59 ) AND 
up.user_id in (select user_id from experience where id  in ($60,$61)  AND experience <= $62 AND experience >= $63) AND 
 up.user_id in (select user_id from user_language where language_id  in ($64,$65) ) AND 
 up.user_id in (select distinct user_id from user_skill where skill_id  in ($66,$67) ) AND 
 up.user_id in (select user_id from user_skill where skill_id  in ($68,$69)  group by user_id having count(user_id) = 2) AND 
 up.subject in (select subject from user_tags where tag  in ($70,$71)  AND taxonomy = $72 AND reference_id = $73)  ORDER BY ut.tag desc,up.full_name`
	printSQL bool   = true
	convert bool = false
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
	flag.BoolVar(&printSQL, "print", printSQL, "Print the generated SQL after it has been parsed?")
	flag.StringVar(&sql, "sql", sql, "Required. The SQL to parse.")
	// flag.BoolVar(&convert, `convert`, true, ``)
	flag.BoolVar(&convert, `convert`, false, ``)
}

func main() {
	flag.Parse()

	if sql == "" && len(os.Args) != 2 {
		fmt.Println("\nPlease provide an SQL to parse")
		fmt.Println(`Example: sqlparsers -print -sql "SELECT * FROM users"`)
		fmt.Println("\nFlags:")
		flag.PrintDefaults()
		fmt.Println("")
		return
	}

	// TODO: Is this right?
	if sql == "" && len(os.Args) == 2 {
		sql = os.Args[1]
	}
		if convert {
			start := time.Now()
			cb := &builder.CustomBuilder{Builder: builder.Oracle()}
			if err := cb.Convert(sql);err != nil {
				log.Printf("%+v\n", err)
				return
			}

			now := time.Now()
			convertedSql, err := cb.ToBoundSQL()
			if err != nil {
				log.Printf("%+v", err)
				return
			}
			fmt.Println(`generate: `, time.Since(now))

			fmt.Println(`total: `, time.Since(start))
			fmt.Println(convertedSql)
			return
		}
		stmt, err := postgresParser.Parse(sql)
		if err != nil {
			log.Println(err)
			return
		}
		// mared, err := json.MarshalIndent(stmt, ``, ``)
		// fmt.Println(string(mared))
		// return
		// fmt.Println(stmt[0].(*postgresParser.Insert).Table.(*postgresParser.NormalizableTableName).String())
	// return
		fmt.Println(postgresGenerator.Parse(stmt[0]))

		if printSQL || true == true {
			fmt.Println("------------------------------- GENERATED QUERY -------------------------------------")
			fmt.Println(stmt.String())
			return
		}



}
