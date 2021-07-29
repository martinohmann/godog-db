package db_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/cucumber/godog"
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

	opts := godog.Options{
		Format: "progress",
		Paths:  []string{"features"},
	}

	suite := godog.TestSuite{
		Options:             &opts,
		ScenarioInitializer: c.Register,
		Name:                "integration",
	}

	status := suite.Run()

	os.Exit(status)
}
