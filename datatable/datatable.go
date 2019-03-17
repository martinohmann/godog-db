package datatable

import (
	"encoding/json"
	"errors"

	"github.com/DATA-DOG/godog/gherkin"
	"github.com/jinzhu/copier"
	"github.com/tidwall/pretty"
)

// DataTable defines a table with fields names and rows.
type DataTable struct {
	Fields []string
	Rows   [][]string
}

// New creates a new DataTable with given fields. It optionally accepts initial
// rows.
func New(fields []string, rows ...[]string) *DataTable {
	return &DataTable{Fields: fields, Rows: rows}
}

// FromGherkin creates a new DataTable from *gherkin.DataTable
func FromGherkin(dt *gherkin.DataTable) (*DataTable, error) {
	if len(dt.Rows) < 2 {
		return nil, errors.New("DataTable must have at least two rows")
	}

	return New(values(dt.Rows[0]), rowValues(dt.Rows[1:])...), nil
}

// Copy makes a copy of the data table.
func (t *DataTable) Copy() *DataTable {
	c := &DataTable{
		Fields: make([]string, len(t.Fields)),
		Rows:   make([][]string, len(t.Rows)),
	}

	copier.Copy(&c.Fields, &t.Fields)
	copier.Copy(&c.Rows, &t.Rows)

	return c
}

// FindRow compares given row with all rows in the data table and returns the
// row index if a matching row is found. Returns -1 if row cannot be found.
func (t *DataTable) FindRow(row []string) int {
	for i, r := range t.Rows {
		if matchValues(r, row) {
			return i
		}
	}

	return -1
}

// RemoveRow removes the row at given index.
func (t *DataTable) RemoveRow(index int) {
	t.Rows = append(t.Rows[:index], t.Rows[index+1:]...)
}

// AppendRow appends a row to the data table. Will return an error if the
// number of fields does not match the data table's fields.
func (t *DataTable) AppendRow(row []string) error {
	if len(row) != len(t.Fields) {
		return errors.New("invalid row length")
	}

	t.Rows = append(t.Rows, row)

	return nil
}

// Len returns the row count of the data table.
func (t *DataTable) Len() int {
	return len(t.Rows)
}

// Slice transforms the data table rows into a slice of maps and returns it.
// The map keys are the data table's fields for every row.
func (t *DataTable) Slice() []map[string]string {
	s := make([]map[string]string, len(t.Rows))

	for i, row := range t.Rows {
		m := make(map[string]string)
		for j, field := range t.Fields {
			m[field] = row[j]
		}

		s[i] = m
	}

	return s
}

// PrettyJSON is a convenience function for transforming the data table into
// its prettyprinted json representation. Will panic if json marshalling fails.
func (t *DataTable) PrettyJSON() []byte {
	buf, err := json.Marshal(t.Slice())
	if err != nil {
		panic(err)
	}

	return pretty.Pretty(buf)
}

// rowValues converts a slice of *gherkin.TableRow into a slice of string
// slices.
func rowValues(rows []*gherkin.TableRow) [][]string {
	vals := make([][]string, len(rows))
	for i, row := range rows {
		vals[i] = values(row)
	}

	return vals
}

// values converts a *gherkin.TableRow into a slice of strings.
func values(row *gherkin.TableRow) []string {
	values := make([]string, len(row.Cells))
	for i, cell := range row.Cells {
		values[i] = cell.Value
	}

	return values
}

// matchRow returns true if all values in two string slices match pairwise.
func matchValues(a, b []string) bool {
	for i := range a {
		if b[i] != a[i] {
			return false
		}
	}

	return true
}
