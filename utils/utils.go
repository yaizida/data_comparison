package utils

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

// IntersectSets находит пересечение двух множеств.
// Функции, начинающиеся с заглавной буквы, являются экспортируемыми
func IntersectSets(set1, set2 map[string]struct{}) map[string]struct{} {
	intersection := make(map[string]struct{})
	for key := range set1 {
		if _, exists := set2[key]; exists {
			intersection[key] = struct{}{}
		}
	}
	return intersection
}

func СonnectToDB(connStr string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("ошибка подключения к БД: %w", err)
	}
	return db, nil
}

func FetchRowsAsMap(db *sqlx.DB, query string) ([]map[string]interface{}, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		result := make(map[string]interface{})
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			result[col] = val
		}
		results = append(results, result)
	}

	return results, nil // Возвращаем результаты
}
