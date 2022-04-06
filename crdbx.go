package crdbx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/cockroachdb/cockroach-go/v2/crdb"
	pgx "github.com/jackc/pgx/v4/stdlib" // force psql driver import
)

// register crdbx driver on library import
func init() {
	sql.Register("crdbx", &Driver{pgxDriver: &pgx.Driver{}})
}

// Conn implement database/sql/driver.Conn interface
type Conn struct {
	pgxConn pgx.Conn
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.pgxConn.Prepare(query)
}

func (c *Conn) Close() error {
	return c.pgxConn.Close()
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.pgxConn.Begin()
}

// TODO(d.lopez) consider implementing Connector and DriverContext interfaces, so we can make use of OpenDB method
//   instead of
//   encoding all configuration into an string passed to sql.Open, more details here:
//   https://golang.org/doc/go1.10#database/sql/driver

// Driver implement database/sql/driver.Driver interface
type Driver struct {
	pgxDriver *pgx.Driver
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	conn, err := d.pgxDriver.Open(name)
	if err != nil {
		return nil, err
	}
	return &Conn{pgxConn: *conn.(*pgx.Conn)}, nil
}

// ExecContext implement database/sql/driver.ExecerContext interface
func (c *Conn) ExecContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Result, error) {
	var result driver.Result
	if err := crdb.Execute(func() error {
		var err error
		result, err = c.pgxConn.ExecContext(ctx, query, argsV)
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// QueryContext implement database/sql/driver.QueryerContext interface
func (c *Conn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	var rows driver.Rows
	if err := crdb.Execute(func() error {
		var err error
		rows, err = c.pgxConn.QueryContext(ctx, query, argsV)
		return err
	}); err != nil {
		return nil, err
	}
	return rows, nil
}
