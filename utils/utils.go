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
