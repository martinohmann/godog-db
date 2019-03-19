package queries

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/martinohmann/godog-helpers/datatable"
)

// DiffResult is a container for datatables of matching, missing and additional
// rows.
type DiffResult struct {
	Matching, Missing, Additional *datatable.DataTable
}

// CountRows counts the rows in given table.
func CountRows(db *sql.DB, tableName string) (int, error) {
	var count int

	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)

	row := db.QueryRow(query)
	if err := row.Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

// DeleteAllRows deletes all rows from a table.
func DeleteAllRows(db *sql.DB, tableName string) error {
	_, err := db.Exec(fmt.Sprintf("DELETE FROM `%s`", tableName))

	return err
}

// Insert inserts all rows in the datatable into the given table.
func Insert(db *sql.DB, tableName string, data *datatable.DataTable) error {
	marks := make([]string, len(data.Fields))

	for i := range data.Fields {
		marks[i] = "?"
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES(%s)",
		tableName,
		strings.Join(data.Fields, ","),
		strings.Join(marks, ","),
	)

	for _, row := range data.Rows {
		var values []interface{}
		for _, val := range row {
			values = append(values, val)
		}

		if _, err := db.Exec(query, values...); err != nil {
			return err
		}
	}

	return nil
}

// Diff queries for all rows in given table and returns a DiffResult with
// matching, missing and additional rows.
func Diff(db *sql.DB, tableName string, expected *datatable.DataTable) (*DiffResult, error) {
	query := fmt.Sprintf(
		"SELECT %s FROM `%s`",
		strings.Join(expected.Fields, ","),
		tableName,
	)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	rawResult, values := createRawResult(len(cols))

	result := &DiffResult{
		Missing:    expected.Copy(),
		Matching:   datatable.New(expected.Fields),
		Additional: datatable.New(expected.Fields),
	}

	for rows.Next() {
		if err = rows.Scan(values...); err != nil {
			return nil, err
		}

		row := convertRawResult(rawResult)

		if index := result.Missing.FindRow(row); index >= 0 {
			result.Matching.AppendRow(row)
			result.Missing.RemoveRow(index)
		} else {
			result.Additional.AppendRow(row)
		}
	}

	return result, nil
}

func createRawResult(cols int) ([][]byte, []interface{}) {
	rawResult := make([][]byte, cols)

	values := make([]interface{}, cols)
	for i, _ := range rawResult {
		values[i] = &rawResult[i]
	}

	return rawResult, values
}

func convertRawResult(rawResult [][]byte) []string {
	row := make([]string, len(rawResult))

	for i, raw := range rawResult {
		if raw == nil {
			row[i] = "\\N"
		} else {
			row[i] = string(raw)
		}
	}

	return row
}
