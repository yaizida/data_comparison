package main

import (
	"fmt"
	"log"
	"os"

	"data_comparison/models"
	"data_comparison/utils" // Хранит функцию поиска пересечения

	"github.com/joho/godotenv"
	_ "github.com/lib/pq" // Драйвер для PostgreSQL
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Ошибка при загрузке файла .env: ", err)
	}

	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	dbDevHost := os.Getenv("DEV_DB_HOST")
	dbProdHost := os.Getenv("PROD_DB_HOST")
	dbPort := os.Getenv("DB_PORT")

	devConnStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		dbUser, dbPassword, dbName, dbDevHost, dbPort)
	prodConnStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		dbUser, dbPassword, dbName, dbProdHost, dbPort)

	devDB, err := utils.СonnectToDB(devConnStr)
	if err != nil {
		log.Fatal(err)
	}
	defer devDB.Close()

	prodDB, err := utils.СonnectToDB(prodConnStr)
	if err != nil {
		log.Fatal(err)
	}
	defer prodDB.Close()

	var variables []models.VariableSelect
	err = devDB.Select(&variables, "SELECT schema_name, table_name FROM ods.variables_select")
	if err != nil {
		log.Fatal("Ошибка при выполнении запроса: ", err)
	}

	for _, record := range variables {
		pgTable := fmt.Sprintf("%s.%s", record.SchemaName, record.TableName)
		pgQuery := fmt.Sprintf("SELECT * FROM %s", pgTable)

		fmt.Println(pgQuery)

		devRows, err := utils.FetchRowsAsMap(devDB, pgQuery)
		if err != nil {
			log.Fatal("Ошибка при выполнении запроса к dev БД: ", err)
		}

		prodRows, err := utils.FetchRowsAsMap(prodDB, pgQuery)
		if err != nil {
			log.Fatal("Ошибка при выполнении запроса к prod БД: ", err)
		}

		devSet := make(map[string]struct{})
		for _, row := range devRows {
			devSet[fmt.Sprint(row)] = struct{}{}
		}

		prodSet := make(map[string]struct{})
		for _, row := range prodRows {
			prodSet[fmt.Sprint(row)] = struct{}{}
		}

		fmt.Println(len(prodRows))
		diffCount := len(prodRows) - len(utils.IntersectSets(devSet, prodSet))
		fmt.Println(diffCount)
	}
}
