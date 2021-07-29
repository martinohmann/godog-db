// Package db defines a godog feature context which adds steps to setup and
// verify database contents during tests.
package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	txdb "github.com/DATA-DOG/go-txdb"
	"github.com/cucumber/godog"
	"github.com/cucumber/messages-go/v10"
	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
	"github.com/martinohmann/godog-db/queries"
	"github.com/martinohmann/godog-helpers/datatable"
)

// txDBDriver defines a custom driver name for go-txdb.
const txDBDriver = "txdb-godog-db"

type DBInitializeFn func(*sql.DB)

// FeatureContext adds steps to setup and verify database contents during godog
// tests.
type FeatureContext struct {
	dsn, driver string
	db          *sql.DB
	qb          *goqu.Database
	initDB      DBInitializeFn
}

var once sync.Once

// NewFeatureContext create a new feature context. It expects the db driver
// name, a connection DSN and an init functions which receives an instance of
// *sql.DB. The init function is called before every scenario and can be used
// to pass the db handle to the tested application or run setup like database
// migrations.
func NewFeatureContext(driver, dsn string, initDB DBInitializeFn) *FeatureContext {
	return &FeatureContext{
		dsn:    dsn,
		driver: driver,
		initDB: initDB,
	}
}

// registerTxDB registers txdb.
func (c *FeatureContext) registerTxDB() {
	txdb.Register(txDBDriver, c.driver, c.dsn)
}

// beforeScenario is called before each scenario and resets the database.
func (c *FeatureContext) beforeScenario(interface{}) {
	once.Do(c.registerTxDB)
	if c.db != nil {
		c.db.Close()
	}

	db, err := sql.Open(txDBDriver, "godog-feature-contexts")
	if err != nil {
		panic(err)
	}

	c.db = db
	c.qb = goqu.New(c.driver, db)

	if c.initDB != nil {
		c.initDB(db)
	}
}

// theTableIsEmpty deletes all rows from given table.
func (c *FeatureContext) theTableIsEmpty(tableName string) error {
	return queries.DeleteAllRows(c.qb, tableName)
}

// iHaveFollowingRowsInTable inserts all rows from the data table into given table.
func (c *FeatureContext) iHaveFollowingRowsInTable(tableName string, data *godog.Table) error {
	table, err := toDataTable(data)
	if err != nil {
		return err
	}

	return queries.Insert(c.qb, tableName, table)
}

func (c *FeatureContext) iShouldHaveOnlyFollowingRowsInTable(tableName string, data *godog.Table) error {
	return c.diff(tableName, data, true)
}

func (c *FeatureContext) iShouldHaveFollowingRowsInTable(tableName string, data *godog.Table) error {
	return c.diff(tableName, data, false)
}

// diff asserts whether or not all rows present in the data table are also present in given table.
func (c *FeatureContext) diff(tableName string, data *godog.Table, exact bool) error {
	expected, err := toDataTable(data)
	if err != nil {
		return err
	}

	result, err := queries.Diff(c.qb, tableName, expected)
	if err != nil {
		return err
	}

	if result.Matching.Len() < expected.Len() {
		msg := fmt.Sprintf(
			"expected the following rows:\n%s\nFound matching rows:\n%s\nMissing expected rows:\n%s",
			expected.PrettyJSON(),
			result.Matching.PrettyJSON(),
			result.Missing.PrettyJSON(),
		)

		if result.Additional.Len() > 0 {
			msg += fmt.Sprintf(
				"\nFound additional rows:\n%s",
				result.Additional.PrettyJSON(),
			)
		}

		return errors.New(msg)
	} else if exact && result.Additional.Len() > 0 {
		return fmt.Errorf(
			"found unexpected additional rows:\n%s",
			result.Additional.PrettyJSON(),
		)
	}

	return err
}

// theTableShouldBeEmpty asserts whether or not given table is empty.
func (c *FeatureContext) theTableShouldBeEmpty(tableName string) error {
	return c.iShouldHaveCountRowsInTable(0, tableName)
}

// iShouldHaveCountRowsInTable asserts whether or not there is a certain number of rows in given table.
func (c *FeatureContext) iShouldHaveCountRowsInTable(expectedCount int, tableName string) error {
	count, err := queries.CountRows(c.qb, tableName)
	if err != nil {
		return err
	}

	if count != expectedCount {
		return fmt.Errorf("expected to find %d rows in table %q, got %d", expectedCount, tableName, count)
	}

	return nil
}

// Register registers the feature context to the godog suite.
func (c *FeatureContext) Register(ctx *godog.ScenarioContext) {
	ctx.BeforeScenario(func(s *godog.Scenario) {
		c.beforeScenario(s)
	})

	// Given/When
	ctx.Step(`^the table "([^"]*)" is empty$`, c.theTableIsEmpty)
	ctx.Step(`^I have following rows in table "([^"]*)":$`, c.iHaveFollowingRowsInTable)

	// Then
	ctx.Step(`^the table "([^"]*)" should be empty$`, c.theTableShouldBeEmpty)
	ctx.Step(`^I should have (\d+) rows? in table "([^"]*)"$`, c.iShouldHaveCountRowsInTable)
	ctx.Step(`^I should have following rows in table "([^"]*)":$`, c.iShouldHaveFollowingRowsInTable)
	ctx.Step(`^I should have only following rows in table "([^"]*)":$`, c.iShouldHaveOnlyFollowingRowsInTable)
}

func toDataTable(table *godog.Table) (*datatable.DataTable, error) {
	rs := table.GetRows()
	fields, rows := rs[0], rs[1:]

	return datatable.New(collectValues(fields), collectRowsValues(rows)...)
}

func collectRowsValues(rows []*messages.PickleStepArgument_PickleTable_PickleTableRow) [][]string {
	values := make([][]string, 0, len(rows))

	for _, row := range rows {
		values = append(values, collectValues(row))
	}

	return values
}

func collectValues(row *messages.PickleStepArgument_PickleTable_PickleTableRow) []string {
	values := make([]string, 0, row.Size())

	for _, c := range row.GetCells() {
		values = append(values, strings.TrimSpace(c.GetValue()))
	}

	return values
}
