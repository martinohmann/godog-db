package queries

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
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

	rows := sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(5)
	mock.ExpectQuery("SELECT COUNT(*) FROM `users`").WillReturnRows(rows)

	count, err := CountRows(db, "users")
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

	mock.ExpectExec("DELETE FROM `users`").WillReturnResult(sqlmock.NewResult(1, 1))

	if err := DeleteAllRows(db, "users"); err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInsert(t *testing.T) {
	db, mock := createMockDB()

	dt := &datatable.DataTable{
		Fields: []string{"firstname", "lastname"},
		Rows: [][]string{
			{"jane", "doe"},
			{"john", "gopher"},
		},
	}

	mock.ExpectExec("INSERT INTO `users` (firstname,lastname) VALUES(?,?)").
		WithArgs("jane", "doe").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO `users` (firstname,lastname) VALUES(?,?)").
		WithArgs("john", "gopher").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := Insert(db, "users", dt); err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDiff(t *testing.T) {
	db, mock := createMockDB()

	dt := &datatable.DataTable{
		Fields: []string{"thing"},
		Rows: [][]string{
			{"bread"},
			{"butter"},
			{"sausage"},
		},
	}

	rows := sqlmock.NewRows([]string{"thing"}).
		AddRow("bread").
		AddRow("sausage").
		AddRow("cheese")

	mock.ExpectQuery("SELECT thing FROM `things`").WillReturnRows(rows)

	result, err := Diff(db, "things", dt)
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
