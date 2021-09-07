package papergres

import (
	"database/sql"
	"fmt"
)

// Database contains all required database attributes
type Database struct {
	// conn is all information needed to connect to the database.
	conn *Connection

	// connString referes to the DSN string for the current DB.
	connString string
}

// Connection returns the connection information for a database
func (db *Database) Connection() Connection {
	return *db.conn
}

// ConnectionString returns the DSN(Data Source Name) connection string for the
// current DB connection.
func (db *Database) ConnectionString() string {
	if db.connString != "" {
		return db.connString
	}
	db.connString = db.conn.String()
	return db.connString
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
	db.conn = &conn
	db.connString = ""
	return db.Query(sql).ExecNonQuery()
}

// GenerateInsert generates an insert query for the given object
func (db *Database) GenerateInsert(obj interface{}) *Query {
	return db.Schema("public").GenerateInsert(obj)
}

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

// Ping tests the database connection
func (db *Database) Ping() error {
	return open(db.ConnectionString()).Ping()
}

// Query creates a base new query object that can be used for all database operations
func (db *Database) Query(sql string, args ...interface{}) *Query {
	return &Query{
		SQL:      sql,
		Database: db,
		Args:     args,
	}
}

// Stats returns DBStats. Right now this only returns OpenConnections
func (db *Database) Stats() sql.DBStats {
	return open(db.ConnectionString()).Stats()
}

// Schema allows for certain operations that require a specific schema
func (db *Database) Schema(name string) *Schema {
	return &Schema{
		Name:     name,
		Database: db,
	}
}
