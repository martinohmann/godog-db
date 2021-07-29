package queries

import (
	"github.com/doug-martin/goqu/v9"
	"github.com/martinohmann/godog-helpers/datatable"
)

// DiffResult is a container for datatables of matching, missing and additional
// rows.
type DiffResult struct {
	Matching, Missing, Additional *datatable.DataTable
}

// CountRows counts the rows in given table.
func CountRows(db *goqu.Database, tableName string) (int, error) {
	count, err := db.From(tableName).Count()

	return int(count), err
}

// DeleteAllRows deletes all rows from a table.
func DeleteAllRows(db *goqu.Database, tableName string) error {
	query := db.Delete(tableName)
	sql, _, err := query.ToSQL()
	if err != nil {
		return err
	}

	_, err = db.Exec(sql)

	return err
}

// Insert inserts all rows in the datatable into the given table.
func Insert(db *goqu.Database, tableName string, data *datatable.DataTable) error {
	fields := data.Fields()
	cls := make([]interface{}, 0, len(fields))

	for _, val := range fields {
		cls = append(cls, val)
	}

	for _, row := range data.RowValues() {
		vals := make(goqu.Vals, 0, len(row))

		for _, val := range row {
			vals = append(vals, val)
		}

		query := db.Insert(tableName).Prepared(true).Cols(cls...).Vals(vals)
		sql, args, err := query.ToSQL()
		if err != nil {
			return err
		}

		if _, err := db.Exec(sql, args...); err != nil {
			return err
		}
	}

	return nil
}

// Diff queries for all rows in given table and returns a DiffResult with
// matching, missing and additional rows.
func Diff(db *goqu.Database, tableName string, expected *datatable.DataTable) (*DiffResult, error) {
	fields := expected.Fields()
	cls := make([]interface{}, 0, len(fields))

	for _, val := range fields {
		cls = append(cls, val)
	}

	query := db.Select(cls...).From(tableName)
	sql, _, err := query.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	rawResult, values := createRawResult(len(cols))
	missing, _ := datatable.New(expected.Fields())
	additional, _ := datatable.New(expected.Fields())

	result := &DiffResult{
		Missing:    expected.Copy(),
		Matching:   missing,
		Additional: additional,
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
	for i := range rawResult {
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
