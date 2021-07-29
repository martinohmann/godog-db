package queries

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	"github.com/martinohmann/godog-helpers/datatable"
)

func createMockDB() (*sql.DB, sqlmock.Sqlmock) {
	matcher := sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual)
	db, mock, err := sqlmock.New(matcher)
	if err != nil {
		panic(err)
	}

	return db, mock
}

func TestCountRows(t *testing.T) {
	db, mock := createMockDB()
	qb := goqu.New("sqlite3", db)

	rows := sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(5)
	mock.ExpectQuery("SELECT COUNT(*) AS `count` FROM `users` LIMIT 1").WillReturnRows(rows)

	count, err := CountRows(qb, "users")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if count != 5 {
		t.Fatalf("expected count of 5, got %d", count)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteAllRows(t *testing.T) {
	db, mock := createMockDB()
	qb := goqu.New("sqlite3", db)

	mock.ExpectExec("DELETE FROM `users`").WillReturnResult(sqlmock.NewResult(1, 1))

	if err := DeleteAllRows(qb, "users"); err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInsert(t *testing.T) {
	db, mock := createMockDB()
	qb := goqu.New("sqlite3", db)

	dt, _ := datatable.New(
		[]string{"firstname", "lastname"},
		[][]string{
			{"jane", "doe"},
			{"john", "gopher"},
		}...,
	)

	mock.ExpectExec("INSERT INTO `users` (`firstname`, `lastname`) VALUES (?, ?)").
		WithArgs("jane", "doe").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO `users` (`firstname`, `lastname`) VALUES (?, ?)").
		WithArgs("john", "gopher").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := Insert(qb, "users", dt); err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDiff(t *testing.T) {
	db, mock := createMockDB()
	qb := goqu.New("sqlite3", db)

	dt, _ := datatable.New(
		[]string{"thing"},
		[][]string{
			{"bread"},
			{"butter"},
			{"sausage"},
		}...,
	)

	rows := sqlmock.NewRows([]string{"thing"}).
		AddRow("bread").
		AddRow("sausage").
		AddRow("cheese")

	mock.ExpectQuery("SELECT `thing` FROM `things`").WillReturnRows(rows)

	result, err := Diff(qb, "things", dt)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if result.Matching.Len() != 2 {
		t.Fatalf("expected 1 matching row, got %d", result.Matching.Len())
	}

	if result.Missing.Len() != 1 {
		t.Fatalf("expected 1 missing row, got %d", result.Missing.Len())
	}

	if result.Additional.Len() != 1 {
		t.Fatalf("expected 1 additional row, got %d", result.Additional.Len())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
