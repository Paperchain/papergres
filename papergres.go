package papergres

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // install postgres driver
)

var (
	// Log is the way to log the scripting activity happening from the library to the database
	Log Logger

	errNoDriver        = errors.New("No database driver loaded")
	errMultipleDrivers = errors.New("More than 1 data driver loaded")
	errUnableToOpenDB  = errors.New("Unable to open sql database")

	sqlDriver string

	// sql DBs are meant to stay open indefinitely so we cache them here
	// by connection string. The DB internally manages the connection pool.
	openDBs map[string]*sqlx.DB
)

// PrimaryKey is the type used for primary keys
type PrimaryKey interface{}

// Domain a database schema and the package that uses it.
// It provides the default DomainOwner implementation.
type Domain struct {
	// db is the Database
	db *Database

	// schema is the database schema, database.[schema].table
	schema []string

	// pkg is the package that is in charge of the domain
	pkg string

	// name of the domain
	name string
}

// DomainOwner is an application that is in charge of a database domain
type DomainOwner interface {
	// Database returns back the Database of the domain
	Database() *Database

	// Schema returns back schema of the domain
	Schema() []string

	// Package returns back the package that owns the domain
	Package() string

	// Name returns the name of the domain
	Name() string

	// Namespace returns the full namespace of the domain
	Namespace() string

	// String returns the string representation of the DomainOwner
	String() string
}

// Generator defines behavior needed to create a database
type Generator interface {
	// DomainOwner a generator must be a DomainOwner
	DomainOwner

	// DropSchema will drop all schema
	DropSchema() *Result

	// CreateSchema will create all schema
	CreateSchema() *Result

	// CreateMockData generates and inserts test data
	CreateMockData() *Result
}

// Logger is the required interface for the papergres logger
type Logger interface {
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
}

func logDebug(args ...interface{}) {
	if Log == nil {
		return
	}

	Log.Debug(args)
}

// Schema holds the schema to query along with the Database
type Schema struct {
	Name     string
	Database *Database
}

// Query holds the SQL to execute and the connection string
type Query struct {
	SQL      string
	Database *Database
	Args     []interface{}
	insert   bool
}

// Repeat holds the iteration count and params function
// for executing a query N times.
type Repeat struct {
	Query    *Query
	ParamsFn SelectParamsFn
	N        int
}

// SelectParamsFn is a function that takes in the iteration and
// returns the destination and args for a SQL execution.
type SelectParamsFn func(i int) (dest interface{}, args []interface{})

// Result holds the results of an executed query
type Result struct {
	LastInsertId  LastInsertId
	RowsAffected  RowsAffected
	RowsReturned  int
	ExecutionTime time.Duration
	Err           error
}

// LastInsertId is the last inserted ID from a script
type LastInsertId struct {
	ID  PrimaryKey
	Err error
}

// RowsAffected is the returned rows affected from a script
type RowsAffected struct {
	Count int64
	Err   error
}

// NewDomain creates a new Domain
// func (db *Database) NewDomain(name, pkg string, schema ...string) *Domain {
// 	return &Domain{
// 		db:     db,
// 		schema: schema,
// 		pkg:    pkg,
// 		name:   name,
// 	}
// }

// NewResult returns an empty Result
func NewResult() *Result {
	result := &Result{
		LastInsertId: LastInsertId{},
		RowsAffected: RowsAffected{},
	}
	return result
}

func (q *Query) String() string {
	return fmt.Sprintf(`
	Query:
	%s
	%s
	Connection: %s
	`,
		q.SQL, argsToString(q.Args), prettifyConnString(q.Database.ConnectionString()))
}

func argsToString(args []interface{}) string {
	s := ""
	if len(args) > 0 {
		s = "Args:"
	}
	for i, a := range args {
		s += fmt.Sprintf("\n\t$%v: %v", i+1, a)
	}
	return s
}

func (r *Result) String() string {
	if r == nil {
		fmt.Println("nil result")
	}
	var lid, ra string
	if r.LastInsertId.Err == nil {
		lid = fmt.Sprintf("%v", r.LastInsertId.ID)
	} else {
		lid = r.LastInsertId.Err.Error()
	}
	if r.RowsAffected.Err == nil {
		ra = strconv.FormatInt(r.RowsAffected.Count, 10)
	} else {
		ra = r.RowsAffected.Err.Error()
	}

	return fmt.Sprintf(`
LastInsertId:  %v
RowsAffected:  %v
RowsReturned:  %v
ExecutionTime: %v
Error: %v
`,
		lid, ra, r.RowsReturned,
		r.ExecutionTime, r.Err)
}

func init() {
	Reset()
}

// Reset resets all global variables to start-up values.
// All cached values will be created again when needed.
// Can be used to reset DB instances if one were to unexpectedly fail which I'm
// not sure is possible.
func Reset() {
	Shutdown()
	sqlDriver = ""
	openDBs = make(map[string]*sqlx.DB)
}

// Shutdown performs a graceful shutdown of all DBs
func Shutdown() {
	for _, db := range openDBs {
		if err := db.Close(); err != nil {
			log.Fatalf("Error shutting down DB: %s", err.Error())
		}
	}
}

// Repeat will execute a query N times. The param selector function will pass in the
// current iteration and expect back the destination obj and args for that index.
// Make sure to use pointers to ensure the sql results fill your structs.
// Use this when you want to run the same query for many different parameters, like getting
// data for child entities for a collection of parents.
// This function executes the iterations concurrently so each loop should not rely on state
// from a previous loops execution. The function should be extremely fast and efficient with DB resources.
// Returned error will contain all errors that occurred in any iterations.
//
//		params := func(i int) (dest interface{}, args []interface{}) {
//			p := &parents[i] 						// get parent at i to derive parameters
//			args := MakeArgs(p.Id, true, "current") // create arg list, variadic
//			return &p.Child, args 					// &p.Child will be filled with returned data
//		}
//		// len(parents) is parent slice and determines how many times to execute query
//		results, err := db.Query(sql).Repeat(len(parents), params).Exec()
func (q *Query) Repeat(times int, pSelectorFn SelectParamsFn) *Repeat {
	return &Repeat{
		q,
		pSelectorFn,
		times,
	}
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

// GenerateInsert generates the insert query for the given object
func (s *Schema) GenerateInsert(obj interface{}) *Query {
	return s.generateInsertQuery(obj, false)
}

// GenerateInsertWithPK generates the insert query for the given object in which
// PrimaryKey value is also supposed to be populated during insert.
func (s *Schema) GenerateInsertWithPK(obj interface{}) *Query {
	return s.generateInsertQuery(obj, true)
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

	// NOTE: An object is represented as a slice of Fields, where each Field
	// represents a column.
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

// Exec executes the repeat query command. Internally this will prepare the statement
// with the database and then create go routines to execute each statement.
func (r *Repeat) Exec() ([]*Result, error) {
	db := open(r.Query.Database.ConnectionString())
	stmt, err := db.Preparex(r.Query.SQL)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// set up and exec work in go routines managed by a semaphore
	results := make([]*Result, r.N)
	errs := []error{}
	wg := sync.WaitGroup{}
	for i := 0; i < r.N; i++ {
		wg.Add(1)
		func(i int) { //use go routines here --> go func(i int){} --> commented this here deliberately as we see an error on "too many clients open"
			defer wg.Done()

			dest, args := r.ParamsFn(i)
			// create a new query for each iteration since the args change
			qs := r.Query.Database.Query(r.Query.SQL, args...)

			cmd := func(result *Result) error {
				if r.Query.insert {
					meta := newMeta()
					err := stmt.Get(&meta, qs.Args...)
					result.setMeta(meta)
					return err
				}
				err := stmt.Select(dest, qs.Args...)
				result.RowsReturned = getLen(dest)
				return err
			}

			result := execCommand(qs, cmd, fmt.Sprintf("Repeat Index: %v / %v", i+1, r.N))
			results[i] = result
			if result.Err != nil {
				errs = append(errs, result.Err)
			}
		}(i)
	}
	wg.Wait()

	return results, mergeErrs(errs)
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

// Exec runs a sql command given a connection and expects LastInsertId or RowsAffected
// to be returned by the script. Use this for INSERTs
func (q *Query) Exec() *Result {
	return exec(q, false)
}

// ExecNonQuery runs the SQL and doesn't look for any results
func (q *Query) ExecNonQuery() *Result {
	return exec(q, true)
}

type meta struct {
	LastInsertId PrimaryKey
	RowsAffected int64
}

func newMeta() meta {
	return meta{0, -1}
}

func (r *Result) setMeta(m meta) {
	r.LastInsertId.ID = m.LastInsertId
	r.RowsAffected.Count = m.RowsAffected
	if m.LastInsertId == 0 {
		r.LastInsertId.Err = errors.New("no LastInsertId returned")
	}
	if m.RowsAffected == -1 {
		r.RowsAffected.Err = errors.New("no RowsAffected returned")
	}
}

func open(conn string) *sqlx.DB {
	if db, ok := openDBs[conn]; ok {
		return db
	}

	db, err := sqlx.Open(getDriver(), conn)
	if err != nil {
		logDebug(err, conn)
		panic(errUnableToOpenDB)
	}
	openDBs[conn] = db
	return db
}

func getDriver() string {
	if sqlDriver != "" {
		return sqlDriver
	}

	drivers := sql.Drivers()
	if len(drivers) == 0 {
		panic(errNoDriver)
	} else if len(drivers) > 1 {
		logDebug(drivers)
		panic(errMultipleDrivers)
	}

	sqlDriver = drivers[0]
	return sqlDriver
}

// logQuery debugs out a query and a result.
func logQuery(q *Query, res *Result, start time.Time, logArgs ...interface{}) {
	res.ExecutionTime = time.Now().Sub(start)

	l := fmt.Sprintf("\n== POSTGRES QUERY ==%s\n== RESULT ==%s",
		q.String(), res.String())

	if len(logArgs) >= 1 {
		l += "== ADDITIONAL INFO ==\n"
		for _, a := range logArgs {
			l += fmt.Sprintf("%v\n", a)
		}
	}

	logDebug(l)
}

func mergeErrs(errs []error) error {
	var s string
	for _, e := range errs {
		if e != nil {
			s += fmt.Sprintln(e)
		}
	}
	s = strings.TrimRight(s, "\n")
	if s != "" {
		return errors.New(s)
	}
	return nil
}

func getLen(i interface{}) int {
	val := reflect.Indirect(reflect.ValueOf(i))
	if val.Kind() == reflect.Slice {
		return val.Len()
	}
	return 0
}
