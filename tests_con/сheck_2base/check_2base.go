package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	// Загружаем переменные окружения из файла .env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Ошибка при загрузке файла .env")
	}

	// Получаем переменные окружения для PostgreSQL
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	dbDevHost := os.Getenv("DEV_DB_HOST")
	dbProdHost := os.Getenv("PROD_DB_HOST")
	dbPort := os.Getenv("DB_PORT")

	fmt.Println(dbUser)

	connDevStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		dbUser, dbPassword, dbName, dbDevHost, dbPort)

	connProdStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		dbUser, dbPassword, dbName, dbProdHost, dbPort)

	dbDev, err := sql.Open("postgres", connDevStr)
	if err != nil {
		log.Fatal(err)
	}
	defer dbDev.Close()

	// Проверяем соединение c базой данных Dev
	err = dbDev.Ping()
	if err != nil {
		log.Fatal(err)
	}

	dbProd, err := sql.Open("postgres", connProdStr)
	if err != nil {
		log.Fatal(err)
	}
	defer dbProd.Close()

	err = dbProd.Ping()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Успешно подключено к базе данных!")

}
