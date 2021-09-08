package papergres

import (
	"database/sql"
	"errors"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // install postgres driver
)

var (
	errNoDriver        = errors.New("no database driver loaded")
	errMultipleDrivers = errors.New("more than 1 data driver loaded")
	errUnableToOpenDB  = errors.New("unable to open sql database")

	sqlDriver string

	// sql DBs are meant to stay open indefinitely so we cache them here
	// by connection string. The DB internally manages the connection pool.
	openDBs map[string]*sqlx.DB
)

// the first thing to get called
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

// open returns a new open connection to DB and adds it to connection pool.
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

// getDriver returns a registered driver to connect to db
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
