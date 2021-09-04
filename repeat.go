package papergres

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Repeat holds the iteration count and params function
// for executing a query N times.
type Repeat struct {
	Query    *Query
	ParamsFn SelectParamsFn
	N        int
}

// Exec executes the repeat query command. Internally this will prepare the
// statement with the database and then create go routines to execute each
// statement.
func (r *Repeat) Exec() ([]*Result, error) {
	// Open connection to db
	db := open(r.Query.Database.ConnectionString())
	stmt, err := db.Preparex(r.Query.SQL)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Set up and exec work in go routines managed by a semaphore
	results := make([]*Result, r.N)

	// Create a slice to capture errors for each iteration.
	errs := []error{}

	// Set up a wait group to ensure proper synchronization
	wg := sync.WaitGroup{}

	for i := 0; i < r.N; i++ {
		wg.Add(1)

		func(i int) {
			// We can replace the above call with a go routine here
			// go func(i int){}
			// but that can cause an error - "too many clients open"
			// so to throttle go routine creation we can experiment and try to
			// spin go routines in batches of n<10.
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

			// fire away
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

// mergeErrs returns a compilation of all the errors for each execution of
// repeat. Each error is on it's own separate line.
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
