package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func Open(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("Open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Open: ping: %w", err)
	}
	return db, nil
}
