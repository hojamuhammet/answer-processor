package db

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func NewDatabase(addr string) (*sql.DB, error) {
	if addr == "" {
		return nil, fmt.Errorf("database address is empty")
	}
	db, err := sql.Open("mysql", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return db, nil
}
