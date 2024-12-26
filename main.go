package main

import (
	"fmt"
	"log"
	"os"
	"strings"

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
	// Здесь замените на имя таблицы где у вас хранятся таблицы для сравнений
	err = devDB.Select(&variables, "SELECT schema_name, table_name FROM ods.variables_select") 
	if err != nil {
		log.Fatal("Ошибка при выполнении запроса: ", err)
	}

	for _, metaData := range variables {
		// Формируем запрос для получения списка колонок
		columnsQuery := `
    	 SELECT column_name
    	 FROM information_schema.columns
    	 WHERE table_name = $1 AND table_schema = $2;
    	`

		// Выполняем запрос
		rows, err := devDB.Query(columnsQuery, metaData.TableName, metaData.SchemaName)
		if err != nil {
			log.Fatalf("Ошибка при получении списка колонок для таблицы %s.%s: %v", metaData.SchemaName, metaData.TableName, err)
		}
		defer rows.Close()

		// Обрабатываем результаты
		var columns []string
		for rows.Next() {
			var columnName string
			err := rows.Scan(&columnName)
			if err != nil {
				log.Fatalf("Ошибка при чтении колонок для таблицы %s.%s: %v", metaData.SchemaName, metaData.TableName, err)
			}
			columns = append(columns, columnName)
		}

		if len(columns) == 0 {
			fmt.Printf("Ошибка: columns пуст. \nТаблица: %s.%s\n", metaData.SchemaName, metaData.TableName)
			continue
		}

		formattedColumns := make([]string, len(columns))
		for i, col := range columns {
			formattedColumns[i] = fmt.Sprintf("COALESCE(%s::text, '')", col) // Используем COALESCE для обработки NULL
		}
		joinedColumns := strings.Join(formattedColumns, " || ',' || ") // Объединяем колонки в одну строку

		pgQuery := fmt.Sprintf(`SELECT md5(string_agg(%s, ',')) AS row_hash
							FROM %s.%s AS as2
							GROUP BY %s;`,
			joinedColumns,
			metaData.SchemaName,
			metaData.TableName,
			columns[0],
		)
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
