package crdbx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	_ "github.com/jackc/pgx/v5/stdlib" // force psql driver import
)

// Driver implements database/sql/driver.Driver interface
type Driver struct {
	parent driver.Driver
}

// conn implements database/sql/driver.Driver interface
type conn struct {
	parent driver.Conn
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.Prepare(query)
}

func (c *conn) Close() error {
	return c.Close()
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.Begin()
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
	if err := db.Close(); err != nil {
		return err
	}

	// Register and wrap the driver.
	m.Lock()
	defer m.Unlock()
	sql.Register("crdbx", &Driver{parent: db.Driver()})
	return nil
}

// Open implement database/sql/driver.Driver interface.
func (d *Driver) Open(name string) (driver.Conn, error) {
	fmt.Println("ha pasao")
	c, err := d.parent.Open(name)
	if err != nil {
		return nil, err
	}

	return &conn{parent: c}, nil
}

// ExecContext implement database/sql/driver.ExecerContext interface.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	fmt.Println("ha pasao")
	var result driver.Result
	if err := crdb.Execute(func() error {
		var err error
		result, err = c.parent.(driver.ExecerContext).ExecContext(crdb.WithMaxRetries(ctx, 5), query, args)
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// QueryContext implement database/sql/driver.QueryerContext interface
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	fmt.Println("ha pasao")
	var rows driver.Rows
	if err := crdb.Execute(func() error {
		var err error
		rows, err = c.parent.(driver.QueryerContext).QueryContext(crdb.WithMaxRetries(ctx, 5), query, args)
		return err
	}); err != nil {
		return nil, err
	}
	return rows, nil
}
