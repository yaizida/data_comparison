package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type VariableSelect struct {
	SchemaName string `db:"schema_name"`
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
}

func main() {
	devConnStr := "user=user password='password' host=133.33.333.333 dbname=postgres sslmode=disable"
	prodConnStr := "user=user password='password' host=144.44.444.444 dbname=postgres sslmode=disable"

	devDB, err := sqlx.Connect("postgres", devConnStr)
	if err != nil {
		log.Fatalln(err)
	}
	defer devDB.Close()

	prodDB, err := sqlx.Connect("postgres", prodConnStr)
	if err != nil {
		log.Fatalln(err)
	}
	defer prodDB.Close()

	var variables []VariableSelect
	err = devDB.Select(&variables, "SELECT * FROM ods.variables_select")
	if err != nil {
		log.Fatalln(err)
	}

	for _, record := range variables {
		if record.ColumnName != "" {
			pgTable := fmt.Sprintf("%s.%s", record.SchemaName, record.TableName)
			pgQuery := fmt.Sprintf("SELECT %s FROM %s", record.ColumnName, pgTable[:len(pgTable)-4])
			fmt.Println(pgQuery)

			var devRows, prodRows [][]interface{}
			err = devDB.Select(&devRows, pgQuery)
			if err != nil {
				log.Fatalln(err)
			}
			err = prodDB.Select(&prodRows, pgQuery)
			if err != nil {
				log.Fatalln(err)
			}

			devSet := make(map[string]struct{})
			for _, row := range devRows {
				devSet[fmt.Sprint(row...)] = struct{}{}
			}

			prodSet := make(map[string]struct{})
			for _, row := range prodRows {
				prodSet[fmt.Sprint(row...)] = struct{}{}
			}

			fmt.Println(len(prodRows))
			diffCount := len(prodRows) - len(intersectSets(devSet, prodSet))
			fmt.Println(diffCount)
		}
	}
}

func intersectSets(set1, set2 map[string]struct{}) map[string]struct{} {
	intersection := make(map[string]struct{})
	for key := range set1 {
		if _, exists := set2[key]; exists {
			intersection[key] = struct{}{}
		}
	}
	return intersection
}
