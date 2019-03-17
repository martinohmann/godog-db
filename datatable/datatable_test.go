package datatable

import (
	"reflect"
	"testing"

	"github.com/DATA-DOG/godog/gherkin"
)

func TestFromGherkin(t *testing.T) {
	fields, rows := testData()
	table := append([][]string{fields}, rows...)

	dt, err := FromGherkin(buildTable(table))
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if !reflect.DeepEqual(dt.Fields, fields) {
		t.Fatalf("expected fields %#v, got %#v", fields, dt.Fields)
	}

	if !reflect.DeepEqual(dt.Rows, rows) {
		t.Fatalf("expected fields %#v, got %#v", rows, dt.Rows)
	}
}

func TestFromMalformedGherkin(t *testing.T) {
	_, err := FromGherkin(buildTable([][]string{{"foo"}}))
	if err == nil {
		t.Fatal("expected error but got nil")
	}
}

func TestCopy(t *testing.T) {
	fields, rows := testData()

	dt := New(fields, rows...)
	ct := dt.Copy()

	if ct == dt {
		t.Fatal("copy points to source")
	}

	if !reflect.DeepEqual(dt, ct) {
		t.Fatalf("copy and source do not contain the same data")
	}
}

func TestRowOperations(t *testing.T) {
	fields, rows := testData()

	dt := New(fields, rows...)

	index := dt.FindRow([]string{"4", "5", "6"})
	if index != 1 {
		t.Fatalf("expected index 1, got %d", index)
	}

	dt.RemoveRow(index)

	index = dt.FindRow([]string{"4", "5", "6"})
	if index != -1 {
		t.Fatalf("expected index -1, got %d", index)
	}

	err := dt.AppendRow([]string{"10", "11", "12"})
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	index = dt.FindRow([]string{"10", "11", "12"})
	if index != 2 {
		t.Fatalf("expected index 2, got %d", index)
	}
}

func TestSlice(t *testing.T) {
	fields, rows := testData()

	dt := New(fields, rows...)

	s := dt.Slice()

	expected := []map[string]string{
		{"one": "1", "two": "2", "three": "3"},
		{"one": "4", "two": "5", "three": "6"},
		{"one": "7", "two": "8", "three": "9"},
	}

	if !reflect.DeepEqual(s, expected) {
		t.Fatalf("expected %#v, got %#v", expected, s)
	}
}

func testData() ([]string, [][]string) {
	fields := []string{"one", "two", "three"}
	rows := [][]string{
		{"1", "2", "3"},
		{"4", "5", "6"},
		{"7", "8", "9"},
	}

	return fields, rows
}

func buildTable(src [][]string) *gherkin.DataTable {
	rows := make([]*gherkin.TableRow, len(src))
	for i, row := range src {
		cells := make([]*gherkin.TableCell, len(row))
		for j, value := range row {
			cells[j] = &gherkin.TableCell{Value: value}
		}

		rows[i] = &gherkin.TableRow{Cells: cells}
	}

	return &gherkin.DataTable{Rows: rows}
}
