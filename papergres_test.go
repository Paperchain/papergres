package papergres

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// set to true to enable all sql debug logging.
// always commit with this set to false.
var debug = true

type Book struct {
	BookID    PrimaryKey `db:"book_id"`
	Title     string     `db:"title"`
	Author    string     `db:"author"`
	CreatedAt time.Time  `db:"created_at"`
	CreatedBy string     `db:"created_by"`
}

type Character struct {
	CharactedID PrimaryKey `db:"character_id"`
	BookID      PrimaryKey `db:"book_id"`
	Name        string     `db:"name"`
	Description string     `db:"description"`
	CreatedAt   time.Time  `db:"created_at"`
	CreatedBy   string     `db:"created_by"`
}

func TestCanPing(t *testing.T) {
	conn := NewConnection(testDbURL, "papergres-tests")
	err := conn.NewDatabase().Ping()
	assert.Nil(t, err)
}

func TestSetupTeardown(t *testing.T) {
	Log = &testLogger{}
	err := setup()
	if err != nil {
		t.Fail()
	}

	err = teardown()
	if err != nil {
		t.Fail()
	}
}

var (
	book = &Book{
		Title:     "The Martian",
		Author:    "Andy Weir",
		CreatedAt: time.Now(),
		CreatedBy: "TestInsert",
	}

	characters = []Character{
		{
			BookID:      6,
			Name:        "Mark Watney",
			Description: "Our comical hero who is stranded on Mars",
			CreatedAt:   time.Now(),
			CreatedBy:   "TestInsert",
		},
		{
			BookID:      6,
			Name:        "Venkat Kapoor",
			Description: "Sleep deprived MFIC at NASA",
			CreatedAt:   time.Now(),
			CreatedBy:   "TestInsert",
		},
		{
			BookID:      6,
			Name:        "Rich Purnell",
			Description: "A steely-eyed missile man",
			CreatedAt:   time.Now(),
			CreatedBy:   "TestInsert",
		},
		{
			BookID:      6,
			Name:        "Mitch Henderson",
			Description: "Sean Bean doesn't die in this movie",
			CreatedAt:   time.Now(),
			CreatedBy:   "TestInsert",
		},
	}
)

func setup() error {
	Log = &testLogger{}
	teardown()

	db := NewConnection(testDbURL, "papergres-test").NewDatabase()
	r := db.CreateDatabase()
	if r.Err != nil {
		fmt.Printf("failed to create database: %s", r.Err.Error())
		return r.Err
	}
	Log.Infof("Database created: %s", r.String())

	scriptsPath := "scripts" + string(os.PathSeparator) + "create-scripts.sql"
	createScripts, err := ioutil.ReadFile(scriptsPath)
	if err != nil {
		fmt.Printf("failed to read create scripts file: %s", err.Error())
		return err
	}

	r = db.Query(string(createScripts)).ExecNonQuery()
	if r.Err != nil {
		fmt.Printf("failed to run create scripts: %s", r.Err.Error())
		return r.Err
	}

	return nil
}

func teardown() error {
	sql := "DROP SCHEMA IF EXISTS paper CASCADE;"
	db := NewConnection(testDbURL, "papergres-test").NewDatabase()
	r := db.Query(sql).ExecNonQuery()
	if r.Err != nil {
		fmt.Printf("drop error: %s", r.Err.Error())
		return r.Err
	}

	return nil
}

var testDbURL = "postgres://postgres:postgres@localhost:5432/paperchain?sslmode=disable"

func TestCanCreateConnectionObjectFromDatabaseUrl(t *testing.T) {
	dbURL := "postgres://papergresUserName:papergresPassWord@myServer:5432/myDatabase?sslmode=disable"
	conn := NewConnection(dbURL, "papergres_tests")
	assert.Equal(t, "papergresUserName", conn.User, "Not equal")
	assert.Equal(t, "papergresPassWord", conn.Password, "Not equal")
	assert.Equal(t, "myDatabase", conn.Database, "Not equal")
	assert.Equal(t, "myServer", conn.Host, "Not equal")
	assert.Equal(t, "5432", conn.Port, "Not equal")
	assert.Equal(t, SSLDisable, conn.SSLMode, "Not equal")
}

func TestCanCreateValidDatabaseObjectFromConnection(t *testing.T) {
	dbURL := "postgres://papergresUserName:papergresPassWord@myServer:5432/myDatabase?sslmode=disable"
	conn := NewConnection(dbURL, "papergres_tests")
	db := conn.NewDatabase()
	assert.NotNil(t, db, "Nil")
	assert.NotNil(t, db.Connection, "Nil")

	connString := db.ConnectionString()
	assert.True(t, connString != "", "Empty connString")
}

func TestCanCreateValidSchemaObjectFromDatbase(t *testing.T) {
	dbURL := "postgres://papergresUserName:papergresPassWord@myServer:5432/myDatabase?sslmode=disable"
	conn := NewConnection(dbURL, "papergres_tests")
	db := conn.NewDatabase()
	schema := db.Schema("testSchema")
	assert.NotNil(t, schema, "Nil")
	assert.Equal(t, "testSchema", schema.Name, "Schema name not same")
}

func TestCanGenerateValidInsertSql(t *testing.T) {
	// setup()
	book := &Book{
		Title:     "The Martian",
		Author:    "Andy Weir",
		CreatedAt: time.Now(),
		CreatedBy: "TestInsert",
	}
	sql := insertSQL(book, "paper", false)
	fmt.Println(sql)
}

func TestCanInsertAll(t *testing.T) {
	setup()
	length := 1000

	books := make([]Book, length)
	for i := 0; i < length; i++ {
		books[i] = Book{
			Author:    fmt.Sprintf("author-%d", i),
			Title:     fmt.Sprintf("title-%d", i),
			CreatedAt: time.Now(),
			CreatedBy: "papergres-test",
		}
	}

	conn := NewConnection(testDbURL, "papergres_tests")
	db := conn.NewDatabase()

	r, err := db.Schema("paper").InsertAll(books)
	assert.Nil(t, err, "err InsertAll")
	assert.Equal(t, length, len(r), "result length")
	// assert.Equal(t, len, int(r.RowsAffected.Count), "result length")
}

func TestInsert(t *testing.T) {
	setup()
	var defaultTime time.Time

	conn := NewConnection(testDbURL, "papergres_tests")
	db := conn.NewDatabase()

	// test out single insert
	// q := defdb().InsertQuery(book)
	q := db.Schema("paper").GenerateInsert(book)
	assert.NotEmpty(t, q.SQL, "empty SQL")

	// res := schema().Insert(book)
	res := db.Schema("paper").Insert(book)
	if res.Err != nil {
		log.Fatalln(res.Err.Error())
	}
	bookid := res.LastInsertId.ID
	assert.Nil(t, res.Err, "insert error")
	assert.Equal(t, PrimaryKey(6), bookid, "wrong LastInsertId")

	var martian Book
	sql := "SELECT * FROM paper.book WHERE book_id = $1"
	// res = defdb().Query(sql, bookid).ExecSingle(&martian)
	res = db.Query(sql, bookid).ExecSingle(&martian)
	assert.Nil(t, res.Err, "book select")
	assert.True(t, martian.CreatedAt != defaultTime, "book created at")
	assert.Equal(t, "The Martian", martian.Title, "book title")
	assert.Equal(t, "Andy Weir", martian.Author, "book author")
	assert.Equal(t, "TestInsert", martian.CreatedBy, "book created by")
	assert.Equal(t, PrimaryKey(6), bookid, "bookid")

	// r, err := schema().InsertAll(characters)
	r, err := db.Schema("paper").InsertAll(characters)
	assert.Nil(t, err, "err InsertAll")
	assert.Equal(t, len(characters), len(r), "result length")
	// assert.Equal(t, len(characters), int(r.RowsAffected.Count), "result length")

	for i, id := range r {
		characters[i].CharactedID = id.LastInsertId.ID
	}

	sql = "SELECT * FROM paper.character WHERE book_id = $1;"
	var martianChars []Character
	db.Query(sql, 6).ExecAll(&martianChars)
	assert.Equal(t, len(characters), len(martianChars), "martianChars incorrect len")
	for _, c := range martianChars {
		found := false
		for _, og := range characters {
			if c.Name == og.Name {
				found = true
				assert.Equal(t, og.BookID, c.BookID, "book id incorrect", c.CharactedID)
				assert.Equal(t, og.Name, c.Name, "Name incorrect", c.CharactedID)
				assert.Equal(t, og.Description, c.Description, "Description incorrect", c.CharactedID)
				assert.Equal(t, og.CreatedBy, c.CreatedBy, "CreatedBy incorrect", c.CharactedID)
			}
		}
		if !found {
			t.Errorf("character not found %s", c.Name)
		}
	}
}

func TestCanUpdate(t *testing.T) {
	setup()

	conn := NewConnection(testDbURL, "papergres_tests")
	db := conn.NewDatabase()

	res := db.Schema("paper").Insert(book)
	if res.Err != nil {
		log.Fatalln(res.Err.Error())
	}
	bookid := res.LastInsertId.ID

	updateSQL := "UPDATE paper.book SET Title = $1 WHERE book_id = $2"
	b := db.Query(updateSQL, "The New Martian", bookid).ExecNonQuery()
	if b.Err != nil {
		log.Fatalln(res.Err.Error())
	}
	assert.True(t, b.RowsAffected.Count == 1, "Update failed!")

	var martian Book
	selectSQL := "SELECT * FROM paper.book WHERE book_id = $1"
	qRes := db.Query(selectSQL, bookid).ExecSingle(&martian)
	if qRes.Err != nil {
		log.Fatalln(res.Err.Error())
	}

	assert.Equal(t, "The New Martian", martian.Title, "Update failed!")
}

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
