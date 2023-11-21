package crdbx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
)

// Driver implements database/sql/driver.Driver interface
type Driver struct {
	driver.Driver
}

// conn implements database/sql/driver.Driver interface
type conn struct {
	driver.Conn
}

// Mutex to protect driver registration.
var m sync.Mutex

// Register registers the driver.
func Register() error {
	// Retrieve the driver implementation we need to wrap.
	db, err := sql.Open("pgx", "")
	if err != nil {
		return err
	}
	drv := db.Driver()
	if err = db.Close(); err != nil {
		return err
	}

	// Register the driver name.
	m.Lock()
	defer m.Unlock()
	sql.Register("crdx", drv)

	// Return.
	return nil
}

// ExecContext implement database/sql/driver.ExecerContext interface.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var result driver.Result
	if err := crdb.Execute(func() error {
		var err error
		result, err = c.Conn.(driver.ExecerContext).ExecContext(crdb.WithMaxRetries(ctx, 5), query, args)
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// QueryContext implement database/sql/driver.QueryerContext interface
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var rows driver.Rows
	if err := crdb.Execute(func() error {
		var err error
		rows, err = c.Conn.(driver.QueryerContext).QueryContext(crdb.WithMaxRetries(ctx, 5), query, args)
		return err
	}); err != nil {
		return nil, err
	}
	return rows, nil
}
