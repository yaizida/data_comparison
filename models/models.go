package models

type VariableSelect struct {
	SchemaName string `db:"schema_name"`
	TableName  string `db:"table_name"`
}
