package db_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/DATA-DOG/godog"
	godogdb "github.com/martinohmann/godog-db"
	_ "github.com/mattn/go-sqlite3"
)

const (
	createTableFoo = "CREATE TABLE IF NOT EXISTS `foo` (id INTEGER PRIMARY KEY, text TEXT)"
)

func initDB(db *sql.DB) {
	if _, err := db.Exec(createTableFoo); err != nil {
		panic(err)
	}
}

func TestMain(m *testing.M) {
	c := godogdb.NewFeatureContext("sqlite3", "./godog.db", initDB)

	status := godog.RunWithOptions("godog", c.Register, godog.Options{
		Format: "progress",
		Paths:  []string{"features"},
	})

	if st := m.Run(); st > status {
		status = st
	}

	os.Exit(status)
}
