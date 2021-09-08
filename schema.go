package papergres

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// Schema holds the schema to query along with the Database
type Schema struct {
	Name     string
	Database *Database
}

// GenerateInsert generates the insert query for the given object
func (s *Schema) GenerateInsert(obj interface{}) *Query {
	return s.generateInsertQuery(obj, false)
}

// GenerateInsertWithPK generates the insert query for the given object in which
// PrimaryKey value is also supposed to be populated during insert.
func (s *Schema) GenerateInsertWithPK(obj interface{}) *Query {
	return s.generateInsertQuery(obj, true)
}

// Insert inserts the passed in object in DB.
//
// NOTE: It does not account for client side generated value for a
// primary key and expects that the logic for populating value
// of primary key should reside in the database as sequence.
//
// DO NOT use Insert() if you wish to populate a client side generated
// value in primary key, use InsertWithPK() instead.
func (s *Schema) Insert(obj interface{}) *Result {
	sql := insertSQL(obj, s.Name, false)
	args := insertArgs(obj, false)
	return s.Database.Query(sql, args...).Exec()
}

// InsertWithPK performs inserts on objects and persists the Primary key value
// to DB as well. It will fail to insert duplicate values to DB.
//
// NOTE: Proceed with caution!
// Only use this method you wish to persist a client side generated value in
// Primary key and  don't rely on database sequenece to autogenerate
// PrimaryKey values.
func (s *Schema) InsertWithPK(obj interface{}) *Result {
	sql := insertSQL(obj, s.Name, true)
	args := insertArgs(obj, true)
	return s.Database.Query(sql, args...).ExecNonQuery()
}

// InsertAll inserts a slice of objects concurrently.
// objs must be a slice with items in it.
// the Result slice will be in the same order as objs
// so a simple loop will set all the primary keys if needed:
// 	for i, r := range results {
//		objs[i].Id = r.LastInsertId.ID
//	}
func (s *Schema) InsertAll(objs interface{}) ([]*Result, error) {
	slice, err := convertToSlice(objs)
	if err != nil {
		return nil, err
	}
	if len(slice) == 0 {
		return nil, errors.New("empty slice")
	}

	// now turn the objs into a repeat query and exec
	return s.GenerateInsert(slice[0]).Repeat(len(slice),
		func(i int) (dest interface{}, args []interface{}) {
			args = insertArgs(slice[i], false)
			return
		}).Exec()
}

// generateInsertQuery constructs an insert query for the given object
func (s *Schema) generateInsertQuery(obj interface{}, withPK bool) *Query {
	sql := insertSQL(obj, s.Name, withPK)
	args := insertArgs(obj, withPK)
	q := s.Database.Query(sql, args...)
	q.insert = true
	return q
}

// insertSQL generates insert SQL string for a given object and schema
func insertSQL(obj interface{}, schema string, withPK bool) string {
	// Construct the table name prefixed with schema name
	tname := goToSQLName(getTypeName(obj))
	tname = fmt.Sprintf("%s.%s", schema, tname)

	// Construct the first component of insert statement
	sql := fmt.Sprintf("INSERT INTO %s (", tname)

	// NOTE: An object is represented as a slice of Fields,
	// where each Field represents a column.
	// Get list of columns to populate.
	fields, primary := prepareFields(obj, withPK)

	// Based on the number of columns, create value placeholders
	var values string
	for i, f := range fields {
		sql += fmt.Sprintf("\n\t%s,", getColumnName(f))
		values += fmt.Sprintf("\n\t$%v,", i+1)
	}

	// Add source data placeholders
	// something like this: `VALUES ($1, $2, $3, $4, $5, $6)`
	sql = strings.TrimRight(sql, ",")
	sql += "\n)\nVALUES ("
	sql += values
	sql = strings.TrimRight(sql, ",")
	sql += "\n)\n"

	// Add last line to capture primary key
	sql += fmt.Sprintf("RETURNING %s as LastInsertId;", getColumnName(primary))

	return sql
}

// getColumnName returns a Field's associated Tag name if it is supplied.
// Else, it constructs a snake_case value from Field.Name value and returns it.
// Example:
// For a Field with `Name` as 'TransactionSource' if `db: transaction_source` is
// present in Tag then it'll be used else it'll be constructed.
func getColumnName(f *Field) string {
	var columnName string
	if f.Tag != "" {
		columnName = f.Tag
		return columnName
	}

	columnName = goToSQLName(f.Name)
	return columnName
}

// goToSQLName converts a string from camel case to snake case
// e.g. TransactionSource to transaction_source
func goToSQLName(name string) string {
	var s string
	for _, c := range name {
		if unicode.IsUpper(c) {
			if s != "" {
				s += "_"
			}
		}
		s += strings.ToLower(string(c))
	}
	return s
}

// insertArgs creates the insert arg slice for an object
func insertArgs(obj interface{}, withPK bool) []interface{} {
	final, _ := prepareFields(obj, withPK)
	args := make([]interface{}, len(final))
	for i, f := range final {
		args[i] = f.Value
	}
	return args
}

// prepareFields performs necessary transformations for the insert statement.
// If `withPK` is false: It does not account a primary key Field to list of
// fields to append, else, primary key is also considered.
func prepareFields(obj interface{}, withPK bool) (nfields []*Field, primary *Field) {
	fields := fields(obj)
	for _, f := range fields {

		if f.IsPrimary {
			primary = f
			if !withPK {
				continue
			}
		}
		nfields = append(nfields, f)
	}
	return
}
