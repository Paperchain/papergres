package papergres

import (
	"fmt"
	"time"
)

// Log is the way to log the scripting activity happening from the library to the database
var Log Logger

// Logger is the required interface for the papergres logger
type Logger interface {
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
}

// logDebug writes a 'DEBUG' level log
func logDebug(args ...interface{}) {
	// Return early if the logger is not initialized
	if Log == nil {
		return
	}
	Log.Debug(args)
}

// logQuery debugs out a query and a result.
func logQuery(q *Query, res *Result, start time.Time, logArgs ...interface{}) {
	res.ExecutionTime = time.Since(start)

	// Prepare output
	l := fmt.Sprintf("\n== POSTGRES QUERY ==%s\n== RESULT ==%s",
		q.String(), res.String())

	if len(logArgs) >= 1 {
		l += "\n== ADDITIONAL INFO ==\n"
		for _, a := range logArgs {
			l += fmt.Sprintf("%v\n", a)
		}
	}

	logDebug(l)
}
