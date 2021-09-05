package papergres

import (
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
)

// Query holds the SQL to execute and the connection string
type Query struct {
	SQL      string
	Database *Database
	Args     []interface{}
	insert   bool
}

// SelectParamsFn is a function that takes in the iteration and
// returns the destination and args for a SQL execution.
type SelectParamsFn func(i int) (dest interface{}, args []interface{})

// Exec runs a sql command given a connection and expects LastInsertId or RowsAffected
// to be returned by the script. Use this for INSERTs
func (q *Query) Exec() *Result {
	return exec(q, false)
}

// ExecAll gets many rows and populates the given slice
// dest should be a pointer to a slice
func (q *Query) ExecAll(dest interface{}) *Result {
	all := func(db *sqlx.DB, r *Result) error {
		err := db.Select(dest, q.SQL, q.Args...)

		r.RowsReturned = getLen(dest)

		return err
	}

	return execDB(q, all)
}

// ExecAllIn works with IN queries. It will take a slice of values and
// attach the slice to the query as a list of values.
// One key difference with bindvar used for IN query is a `?` (question mark)
// the query then has to be rebinded to change default bindvar to target bindvar
// like `$1` (dollar sign followed by a number) for postgres etc.
func (q *Query) ExecAllIn(dest interface{}) *Result {
	all := func(db *sqlx.DB, r *Result) error {
		query, args, err := sqlx.In(q.SQL, q.Args...)
		if err != nil {
			return err
		}

		// Rebind the query to replace the ? with $1, $2, etc
		query = db.Rebind(query)

		// Execute a select query using this DB
		err = db.Select(dest, query, args...)

		r.RowsReturned = getLen(dest)

		return err
	}

	return execDB(q, all)
}

// ExecNonQuery runs the SQL and doesn't look for any results
func (q *Query) ExecNonQuery() *Result {
	return exec(q, true)
}

// ExecSingle fetches a single row from the database and puts it into dest.
// If more than 1 row is returned it takes the first one.
// Expects at least 1 row or it will error.
func (q *Query) ExecSingle(dest interface{}) *Result {
	single := func(db *sqlx.DB, r *Result) error {
		err := db.Get(dest, q.SQL, q.Args...)
		if err == nil {
			r.RowsReturned = 1
		}
		return err
	}

	return execDB(q, single)
}

// Repeat will execute a query N times. The param selector function will pass in
// the current iteration and expect back the destination obj and args for that
// index. Make sure to use pointers to ensure the sql results fill your structs.
// Use this when you want to run the same query for many different parameters,
// like getting data for child entities for a collection of parents.
// This function executes the iterations concurrently so each loop should not
// rely on state from a previous loops execution. The function should be
// extremely fast and efficient with DB resources.
// Returned error will contain all errors that occurred in any iterations.
//
// Example usage:
//		params := func(i int) (dest interface{}, args []interface{}) {
//			p := &parents[i] 						// get parent at i to derive parameters
//			args := MakeArgs(p.Id, true, "current") // create arg list, variadic
//			return &p.Child, args 					// &p.Child will be filled with returned data
//		}
//		// len(parents) is parent slice and determines how many times to execute query
//		results, err := db.Query(sql).Repeat(len(parents), params).Exec()
//
func (q *Query) Repeat(times int, pSelectorFn SelectParamsFn) *Repeat {
	return &Repeat{
		q,
		pSelectorFn,
		times,
	}
}

// String returns a SQL query and it's arguments along with connection info in a
// pretty format.
func (q *Query) String() string {
	return fmt.Sprintf(`
	Query:
	%s
	%s
	Connection: %s
	`,
		q.SQL, argsToString(q.Args), prettifyConnString(q.Database.ConnectionString()))
}

// argsToString iterates over each argument and returns them in a neatly
// formatted string.
func argsToString(args []interface{}) string {
	s := ""
	if len(args) > 0 {
		s = "Args:"
	}

	// The idea here is to keep adding each argument on a separate line
	for i, a := range args {
		s += fmt.Sprintf("\n\t$%v: %v", i+1, a)
	}
	return s
}

// getLen returns the number of items in a slice. It can be used to populate
// number of rows returned from a query execution
func getLen(i interface{}) int {
	val := reflect.Indirect(reflect.ValueOf(i))
	if val.Kind() == reflect.Slice {
		return val.Len()
	}
	return 0
}
