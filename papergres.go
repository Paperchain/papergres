package papergres

import (
	"database/sql"
	"errors"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // install postgres driver
)

var (
	errNoDriver        = errors.New("No database driver loaded")
	errMultipleDrivers = errors.New("More than 1 data driver loaded")
	errUnableToOpenDB  = errors.New("Unable to open sql database")

	sqlDriver string

	// sql DBs are meant to stay open indefinitely so we cache them here
	// by connection string. The DB internally manages the connection pool.
	openDBs map[string]*sqlx.DB
)

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
