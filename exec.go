package papergres

import (
	"time"

	"github.com/jmoiron/sqlx"
)

// function type for executional Command
type execCmd func(*Result) error

// function type for a sqlx database command
type dbCmd func(*sqlx.DB, *Result) error

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
func execCommand(q *Query, cmd execCmd, logArgs ...interface{}) *Result {
	r := NewResult()

	defer logQuery(q, r, time.Now(), logArgs...)

	r.Err = cmd(r)
	return r
}

// execDB creates the database for a command before passing it on to the execCommand function
func execDB(q *Query, dbcmd dbCmd) *Result {
	return execCommand(q, func(r *Result) error {
		return dbcmd(open(q.Database.ConnectionString()), r)
	})
}

// exec sql that expects no results or expects LastInsertId and/or RowsAffected, which
// is still basically a nonquery scripts. This is mostly inserts.
func exec(q *Query, nonQuery bool) *Result {
	cmd := func(db *sqlx.DB, r *Result) error {
		meta := newMeta()

		// nonquery is the easy path
		if nonQuery {
			res, err := db.Exec(q.SQL, q.Args...)
			if err != nil {
				return err
			}
			meta.LastInsertId, err = res.LastInsertId()
			if err != nil {
				return err
			}
			meta.RowsAffected, err = res.RowsAffected()
			if err != nil {
				return err
			}
			r.setMeta(meta)
			return nil
		}

		// use get which will blow up if more than 1 row is returned.
		// if using exec, you don't expect to return rows so this
		// should blow up to indicate a bad script or that the user should
		// be using Single() or Select()

		err := db.Get(&meta, q.SQL, q.Args...)
		if err != nil {
			return err
		}

		r.setMeta(meta)
		return nil
	}

	return execDB(q, cmd)
}
