package processor

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

func mustOpenDB(dbURL string) *sql.DB {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("openDB: %v", err)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("pingDB: %v", err)
	}
	return db
}
