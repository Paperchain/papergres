# Papergres
Papergres is an ultra lightweight micro-ORM written in golang for the postgres database server. The library provides easy ways to execute queries, insert data and sql logging. 

This is a wrapper around the general purpose extensions library [sqlx](https://github.com/jmoiron/sqlx) by [jmoiron](https://github.com/jmoiron). The core postgres driver used is [pq](https://github.com/lib/pq). 

Papergres is built and used at [Paperchain](https://paperchain.io). Special thanks to [Weston Dorsey(https://github.com/wdorsey), for sparking up the project. 

## Install
`go get -u github.com/Paperchain/papergres`

## API Documentation 
Full API documentation can be found on [godoc](https://godoc.org/github.com/Paperchain/papergres)

## Contributors
We're open to and looking for open-source contributors to help make this library more robust

## Usage
The simplest way to execute a query returning a single object is
```go
// Set up your connection object
conn := NewConnection("postgres://postgres:postgres@localhost:5432/paperchain", "papergres_tests", SSLDisable)
db := conn.NewDatabase()

// Write a query in a single line
var book Book
res := db.Query("SELECT * FROM paper.book WHERE book_id = $1 LIMIT 1;", 777).ExecSingle(&book)
```

To retrieve a list of rows and hydrate it into a list of object
```go
var books []book
res := db.Query("SELECT * FROM paper.book WHERE book_id > 10 LIMIT 1;", 777).ExecAll(&books)
```

To insert a record into a table
```go
// Create a struct and specify database column names via the "db" struct tag
type Book struct {
	BookId    PrimaryKey `db:"book_id"`
	Title     string     `db:"title"`
	Author    string     `db:"author"`
	CreatedAt time.Time  `db:"created_at"`
	CreatedBy string     `db:"created_by"`
}

// Instantiate your struct
book := &Book{
		Title:     "The Martian",
		Author:    "Andy Weir",
		CreatedAt: time.Now(),
		CreatedBy: "TestInsert",
}

// Perform an insert using papergres
res := db.Insert(book)
if res.Err != nil {
		log.Fatalln(res.Err.Error())
}
    
// Retrieve the inserted ID from the primary key
bookid := res.LastInsertId.ID

// To insert into a specific schema other than the public
schema := db.Schema("my_schema")
res := schema.Insert(book)

// To insert multiple
res, err := schema.InsertAll(books)
```

## Logging
The library provides an logging interface that needs to implemented


```go
// Instantiate your logged in your application's bootstrap
Log = &testLogger{}

// The logger interface that needs to be implemented. This is available in papergres.go
type Logger interface {
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
}

// For example, you can set your logger with something like this
type testLogger struct{}

func (t *testLogger) Info(args ...interface{}) {
	if debug {
		fmt.Println(args...)
	}
}
func (t *testLogger) Infof(format string, args ...interface{}) {
	if debug {
		fmt.Printf(format, args...)
	}
}
func (t *testLogger) Debug(args ...interface{}) {
	if debug {
		fmt.Println(args...)
	}
}
func (t *testLogger) Debugf(format string, args ...interface{}) {
	if debug {
		fmt.Printf(format, args...)
	}
}


```

Example of the sql logging 
`
== POSTGRES QUERY ==
        Query:
        INSERT INTO paper.character (
        book_id,
        name,
        description,
        created_at,
        created_by
)
VALUES (
        $1,
        $2,
        $3,
        $4,
        $5
)
RETURNING character_id as LastInsertId;
        Args:
        $1: 6
        $2: Mitch Henderson
        $3: Sean Bean doesn't die in this movie
        $4: 2017-12-14 11:06:46.6077695 +0000 GMT
        $5: TestInsert
        Connection:
        connect_timeout=0
        dbname=paperchain
        fallback_application_name=papergres_tests
        host=localhost
        password=postgres
        port=5432
        sslmode=disable
        user=postgres

== RESULT ==
LastInsertId:  21
RowsAffected:  No RowsAffected returned
RowsReturned:  0
ExecutionTime: 496.2µs
Error: <nil>
== ADDITIONAL INFO ==
Repeat Index: 4 / 4


== POSTGRES QUERY ==
        Query:
        SELECT * FROM paper.character WHERE book_id = $1;
        Args:
        $1: 6
        Connection:
        connect_timeout=0
        dbname=paperchain
        fallback_application_name=papergres_tests
        host=localhost
        password=postgres
        port=5432
        sslmode=disable
        user=postgres

== RESULT ==
LastInsertId:  0
RowsAffected:  0
RowsReturned:  4
ExecutionTime: 5.549ms
Error: <nil>
`