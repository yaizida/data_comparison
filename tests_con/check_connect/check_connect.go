package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq" // Импортируем драйвер PostgreSQL
)

func main() {
	// Загружаем переменные окружения из файла .env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Ошибка при загрузке файла .env")
	}

	// Получаем переменные окружения
	dbUser := os.Getenv("DEV_DB_USER")
	dbPassword := os.Getenv("DEV_DB_PASSWORD")
	dbName := os.Getenv("DEV_DB_NAME")
	dbHost := os.Getenv("DEV_DB_HOST")
	dbPort := os.Getenv("DB_PORT")

	// Формируем строку подключения
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		dbUser, dbPassword, dbName, dbHost, dbPort)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Проверяем соединение
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Успешно подключено к базе данных!")

	// Выполняем запрос
	rows, err := db.Query("SELECT 1")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Printf("Result: %d\n", rows)

	// Проверяем на наличие ошибок после итерации
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}
