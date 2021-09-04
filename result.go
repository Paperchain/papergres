package papergres

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Result holds the results of an executed query
type Result struct {
	LastInsertId  LastInsertId
	RowsAffected  RowsAffected
	RowsReturned  int
	ExecutionTime time.Duration
	Err           error
}

// PrimaryKey is the type used for primary keys
type PrimaryKey interface{}

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

// meta is a default placeholder for basic query execution results
type meta struct {
	LastInsertId PrimaryKey
	RowsAffected int64
}

// newMeta creates az new meta object with default values
func newMeta() meta {
	// Over here we're setting default value of LastInsertId aka PrimaryKey as
	// 0 (zero) which is of type int64 despite PrimaryKey being an interface{}.
	// This is due to a design decision in the underlying in database/sql
	// and jmoiron/sqlx package which always returns LastInsertIds as int64s.
	return meta{0, -1}
}

// NewResult returns an empty Result
func NewResult() *Result {
	result := &Result{
		LastInsertId: LastInsertId{},
		RowsAffected: RowsAffected{},
	}
	return result
}

// String method returns query execution results in a pretty format
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

// setMeta populates query execution results and errors
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
