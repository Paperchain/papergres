package papergres

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
)

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

// NewConnection creates and returns the Connection object to the postgres server
func NewConnection(databaseURL string, appName string) Connection {
	u, err := url.Parse(databaseURL)
	if err != nil {
		panic(err)
	}

	host, port, _ := net.SplitHostPort(u.Host)
	p, _ := u.User.Password()
	q, _ := url.ParseQuery(u.RawQuery)
	path := u.Path
	if strings.Index(path, "/") == 0 {
		path = path[1:]
	}

	// Set sslmode
	sslMode := setSSLMode(q)

	// Build the Connection object
	conn := Connection{
		User:     u.User.Username(),
		Password: p,
		Host:     host,
		Port:     port,
		Database: path,
		AppName:  appName,
		SSLMode:  sslMode,
	}

	return conn
}

// String method builds a DSN(Data Source Name) connection string based on the
// given database connection settings and returns it.
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

// NewDatabase creates a new Database object
func (conn Connection) NewDatabase() *Database {
	return &Database{
		conn: &conn,
	}
}

// function to set SSL mode for connection based on url.Values
func setSSLMode(q url.Values) SSLMode {
	sslMode := SSLDisable

	if len(q["sslmode"]) > 0 && q["sslmode"][0] != "" {
		sslMode = SSLMode(q["sslmode"][0])
	}

	return sslMode
}

// prettifyConnString prints out all the props from connection string in a neat
// way.
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
