package papergres

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // install postgres driver
)

var (
	ErrNoDriver        = errors.New("No database driver loaded")
	ErrMultipleDrivers = errors.New("More than 1 data driver loaded")
	ErrUnableToOpenDB  = errors.New("Unable to open sql database")

	Log Logger

	sqlDriver string

	// sql DBs are meant to stay open indefinitely so we cache them here
	// by connection string. The DB internally manages the connection pool.
	openDBs map[string]*sqlx.DB
)

// PrimaryKey is the type used for primary keys
type PrimaryKey uint64

// Database contains all required database attributes
// Use papergres.New() to create a Database
type Database struct {
	// conn is all information needed to connect to the database
	conn *Connection

	// connString is the cached database connection string
	connString string
}

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

// New creates a new Database object
func (conn Connection) NewDatabase() *Database {
	return &Database{
		conn: &conn,
	}
}

func NewConnection(databaseUrl string, appName string) Connection {
	regex := regexp.MustCompile("(?i)^postgres://(?:([^:@]+):([^@]*)@)?([^@/:]+):(\\d+)/(.*)$")
	matches := regex.FindStringSubmatch(databaseUrl)

	conn := Connection{
		User:     matches[1],
		Password: matches[2],
		Host:     matches[3],
		Port:     matches[4],
		Database: matches[5],
		AppName:  appName,
	}

	return conn
}

// NewDomain creates a new Domain
func (db *Database) NewDomain(name, pkg string, schema ...string) *Domain {
	return &Domain{
		db:     db,
		schema: schema,
		pkg:    pkg,
		name:   name,
	}
}

// NewResult returns an empty Result
func NewResult() *Result {
	result := &Result{
		LastInsertId: LastInsertId{},
		RowsAffected: RowsAffected{},
	}
	return result
}

// Database returns back the Database
func (d *Domain) Database() *Database {
	return d.db
}

// Schema returns domain schemas
func (d *Domain) Schema() []string {
	return d.schema
}

// Package returns the domain package
func (d *Domain) Package() string {
	return d.pkg
}

// Name returns the domain name
func (d *Domain) Name() string {
	return d.name
}

// Namespace is the fully qualified name of a Domain
func (d *Domain) Namespace() string {
	s := fmt.Sprintf("%s.%s.", d.name, d.pkg)
	for _, schema := range d.schema {
		s += schema + "_"
	}
	s = strings.TrimRight(s, "_")
	return s
}

func (d *Domain) String() string {
	return fmt.Sprintf(`
  Name... %s
  Package %s
  Schema
  	%s
  Database %s`,
		d.Name(),
		d.Package(),
		strings.Join(d.Schema(), "\n\t"),
		prettifyConnString(d.db.ConnectionString()))
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

func prettifyConnString(conn string) string {
	var str string
	props := strings.Split(conn, " ")
	sort.Strings(props)
	for _, s := range props {
		if s != "" {
			str += fmt.Sprintf("\n\t%s", s)
		}
	}
	return str
}

func (r *Result) String() string {
	if r == nil {
		fmt.Println("nil result")
	}
	var lid, ra string
	if r.LastInsertId.Err == nil {
		lid = r.LastInsertId.ID.String()
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

func (i PrimaryKey) String() string {
	base := int64(i)
	return strconv.FormatInt(base, 10)
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
		db.Close()
	}
}

// SetConnection will set the connection to the passed in value
func (db *Database) SetConnection(c Connection) {
	db.conn = &c
	db.connString = ""
}

// CreateDatabase creates a default database
// Good for use during testing and local dev
func (db *Database) CreateDatabase() *Result {
	// if Ping works then DB already exists
	err := db.Ping()
	if err == nil {
		return NewResult()
	}

	sql := fmt.Sprintf(`
CREATE DATABASE %s
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;`, db.Connection().Database)

	conn := db.Connection()
	conn.Database = ""
	db.SetConnection(conn)
	return db.Query(sql).ExecNonQuery()
}

// Ping tests the database connection
func (d *Domain) Ping() error {
	return d.Database().Ping()
}

// Ping tests the database connection
func (db *Database) Ping() error {
	return open(db.ConnectionString()).Ping()
}

// Stats returns DBStats. Right now this only returns OpenConnections
func (db *Database) Stats() sql.DBStats {
	return open(db.ConnectionString()).Stats()
}

// IsLocal determines if a database host URL is local
func IsLocal(hostURL string, localURLs ...string) bool {
	for _, u := range localURLs {
		if strings.Contains(strings.ToLower(hostURL), u) {
			return true
		}
	}
	return strings.Contains(strings.ToLower(hostURL), "localhost") ||
		strings.Contains(strings.ToLower(hostURL), "127.0.0.1")
}

// Schema allows for certain operations that require a specific schema
func (db *Database) Schema(name string) *Schema {
	return &Schema{
		Name:     name,
		Database: db,
	}
}

// Query creates a base new query object that can be used for all database operations
func (db *Database) Query(sql string, args ...interface{}) *Query {
	return &Query{
		SQL:      sql,
		Database: db,
		Args:     args,
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

// heres a struct, insert it
// - function to return back SQL too
// heres a struct, prepare SQL and repeat

// Insert inserts the passed in object
func (db *Database) Insert(obj interface{}) *Result {
	return db.Schema("public").Insert(obj)
}

// InsertAll inserts a slice of objects concurrently.
// objs must be a slice with items in it.
// the Result slice will be in the same order as objs
// so a simple loop will set all the primary keys if needed:
// 	for i, r := range results {
//		objs[i].Id = r.LastInsertId.ID
//	}
func (db *Database) InsertAll(objs interface{}) ([]*Result, error) {
	return db.Schema("public").InsertAll(objs)
}

// InsertQuery generates an insert query for the given object
func (db *Database) InsertQuery(obj interface{}) *Query {
	return db.Schema("public").InsertQuery(obj)
}

// Insert inserts the passed in object
func (s *Schema) Insert(obj interface{}) *Result {
	sql := insertSQL(obj, s.Name)
	args := insertArgs(obj)
	return s.Database.Query(sql, args...).Exec()
}

// InsertQuery generates the insert query for the given object
func (s *Schema) InsertQuery(obj interface{}) *Query {
	sql := insertSQL(obj, s.Name)
	args := insertArgs(obj)
	q := s.Database.Query(sql, args...)
	q.insert = true
	return q
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
		return nil, errors.New("Empty slice")
	}

	// now turn the objs into a repeat query and exec
	return s.InsertQuery(slice[0]).Repeat(len(slice),
		func(i int) (dest interface{}, args []interface{}) {
			args = insertArgs(slice[i])
			return
		}).Exec()
}

// insertFor generates insert SQL string for a given object and schema
func insertSQL(obj interface{}, schema string) string {
	tname := goToSQLName(getTypeName(obj))
	tname = fmt.Sprintf("%s.%s", schema, tname)
	sql := fmt.Sprintf("INSERT INTO %s (", tname)

	fields, primary := prepareFields(obj)
	var values string
	for i, f := range fields {
		sql += fmt.Sprintf("\n\t%s,", getColumnName(f))
		values += fmt.Sprintf("\n\t$%v,", i+1)
	}

	sql = strings.TrimRight(sql, ",")
	sql += "\n)\nVALUES ("
	sql += values
	sql = strings.TrimRight(sql, ",")
	sql += "\n)\n"
	sql += fmt.Sprintf("RETURNING %s as LastInsertId;", goToSQLName(primary.Name))

	return sql
}

func getColumnName(f *Field) string {
	var columnName string
	if f.Tag != "" {
		columnName = f.Tag
	} else {
		columnName = goToSQLName(f.Name)
	}
	return columnName
}

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
func insertArgs(obj interface{}) []interface{} {
	final, _ := prepareFields(obj)
	args := make([]interface{}, len(final))
	for i, f := range final {
		args[i] = f.Value
	}
	return args
}

// prepareFields performs necessary transformations for the insert statement
func prepareFields(obj interface{}) (nfields []*Field, primary *Field) {
	fields := fields(obj)
	for i, f := range fields {
		if i == 0 {
			primary = f
			continue
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

// Exec executes an ad-hoc query against a connection.
// This is only recommended for use if you have a weird case where you need to
// modify the connection string just for this query, like when creating
// a new database. Otherwise just new New() and save the DAtabase instance.
func Exec(sql string, conn Connection, args ...interface{}) *Result {
	return conn.NewDatabase().Query(sql, args).Exec()
}

// ExecNonQuery executes an ad-hoc query against a connection.
// This is only recommended for use if you have a weird case where you need to
// modify the connection string just for this query, like when creating
// a new database. Otherwise just new New() and save the Database instance.
func ExecNonQuery(sql string, conn Connection, args ...interface{}) *Result {
	return conn.NewDatabase().Query(sql, args).ExecNonQuery()
}

// execCommand is the single location that runs a command against the database (with the exception
// of prepare statements).
func execCommand(q *Query, cmd func(*Result) error, logArgs ...interface{}) *Result {
	r := NewResult()

	defer logQuery(q, r, time.Now(), logArgs...)

	r.Err = cmd(r)
	return r
}

// execDB creates the database for a command before passing it on to the execCommand function
func execDB(q *Query, dbcmd func(*sqlx.DB, *Result) error) *Result {
	return execCommand(q, func(r *Result) error {
		return dbcmd(open(q.Database.ConnectionString()), r)
	})
}

// exec sql that expects no results or expects LastInsertId and/or RowsAffected, which
// is still basically a nonquery scripts. This is mostly inserts.
func exec(q *Query, nonQuery bool) *Result {
	cmd := func(db *sqlx.DB, r *Result) error {
		// nonquery is the easy path
		if nonQuery {
			_, err := db.Exec(q.SQL, q.Args...)
			return err
		}

		// use get which will blow up if more than 1 row is returned.
		// if using exec, you don't expect to return rows so this
		// should blow up to indicate a bad script or that the user should
		// be using Single() or Select()
		meta := newMeta()
		err := db.Get(&meta, q.SQL, q.Args...)
		if err != nil {
			return err
		}

		r.setMeta(meta)

		return nil
	}

	return execDB(q, cmd)
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
		r.LastInsertId.Err = errors.New("No LastInsertId returned")
	}
	if m.RowsAffected == -1 {
		r.RowsAffected.Err = errors.New("No RowsAffected returned")
	}
}

func open(conn string) *sqlx.DB {
	if db, ok := openDBs[conn]; ok {
		return db
	}

	db, err := sqlx.Open(getDriver(), conn)
	if err != nil {
		Log.Debug(err, conn)
		panic(ErrUnableToOpenDB)
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
		panic(ErrNoDriver)
	} else if len(drivers) > 1 {
		Log.Debug(drivers)
		panic(ErrMultipleDrivers)
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

	Log.Debug(l)
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

// cutFirstIndex cuts the string on the first occurance of the sep.
// cutFirstIndex("hey.o", ".") => ("hey", "o")
// if index not found, returns (s, "")
func cutFirstIndex(s, sep string) (first, rest string) {
	idx := strings.Index(s, sep)
	if idx == -1 {
		return s, ""
	}
	return s[:idx], s[idx+1:]
}

/* --- Start Connection String Functionality --- */

// SSLMode defines all possible SSL options
type SSLMode string

const (
	// SSLDisable - No SSL
	SSLDisable SSLMode = "disable"
	// SSLRequire - Always SSL, no verification
	SSLRequire SSLMode = "require"
	// SSLVerifyCA - Always SSL, verifies that certificate was signed by trusted CA
	SSLVerifyCA SSLMode = "verify-ca"
	// SSLVerifyFull - Always SSL, verifies that certificate was signed by trusted CA
	// and server host name matches the one in the certificate
	SSLVerifyFull SSLMode = "verify-full"
)

// Connection holds all database connection configuration.
type Connection struct {
	Database    string
	User        string
	Password    string
	Host        string
	Port        string
	AppName     string
	Timeout     int
	SSLMode     SSLMode
	SSLCert     string
	SSLKey      string
	SSLRootCert string
}

// Connection returns the connection information for a database
func (db *Database) Connection() Connection {
	return *db.conn
}

// ConnectionString will build a connection string given database Connection settings
func (db *Database) ConnectionString() string {
	if db.connString != "" {
		return db.connString
	}
	db.connString = db.conn.String()
	return db.connString
}

// String will build a connection string given database Connection settings
func (conn *Connection) String() string {
	var s string
	if conn.Database != "" {
		s += fmt.Sprintf("dbname=%s ", conn.Database)
	}
	if conn.User != "" {
		s += fmt.Sprintf("user=%s ", conn.User)
	}
	if conn.Password != "" {
		s += fmt.Sprintf("password=%s ", conn.Password)
	}
	if conn.Host != "" {
		s += fmt.Sprintf("host=%s ", conn.Host)
	}
	if conn.Port != "" {
		s += fmt.Sprintf("port=%s ", conn.Port)
	}
	if conn.AppName != "" {
		s += fmt.Sprintf("fallback_application_name=%s ", conn.AppName)
	}
	if conn.SSLMode != "" {
		s += fmt.Sprintf("sslmode=%s ", conn.SSLMode)
	}
	if conn.SSLCert != "" {
		s += fmt.Sprintf("sslcert=%s ", conn.SSLCert)
	}
	if conn.SSLKey != "" {
		s += fmt.Sprintf("sslkey=%s ", conn.SSLKey)
	}
	if conn.SSLRootCert != "" {
		s += fmt.Sprintf("sslrootcert=%s ", conn.SSLRootCert)
	}
	s += fmt.Sprintf("connect_timeout=%v ", conn.Timeout)
	return s
}
