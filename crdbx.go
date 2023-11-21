package crdbx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
)

// config is the configuration of crdbx.
type config struct {
	maxRetries *int
}

// newConfig returns a config with all Options set.
func newConfig(options ...Option) *config {
	cfg := &config{}
	for _, opt := range options {
		opt(cfg)
	}
	return cfg
}

// crdbConn is a wrapper of driver.Conn.
type crdbConn struct {
	driver.Conn
	cfg *config
}

// newConn returns a new crdbConn.
func newConn(conn driver.Conn, cfg *config) *crdbConn {
	return &crdbConn{
		Conn: conn,
		cfg:  cfg,
	}
}

// ExecContext implement database/sql/driver.ExecerContext interface.
func (c *crdbConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("crdbx: the driver to be wrapped must implement ExecerContext")
	}
	if c.cfg.maxRetries != nil {
		ctx = crdb.WithMaxRetries(ctx, *c.cfg.maxRetries)
	}
	var result driver.Result
	if err := crdb.Execute(func() error {
		var err error
		result, err = execer.ExecContext(ctx, query, args)
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// QueryContext implement database/sql/driver.QueryerContext interface.
func (c *crdbConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	querier, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, errors.New("crdbx: the driver to be wrapped must implement QueryerContext")
	}
	if c.cfg.maxRetries != nil {
		ctx = crdb.WithMaxRetries(ctx, *c.cfg.maxRetries)
	}
	var rows driver.Rows
	if err := crdb.Execute(func() error {
		var err error
		rows, err = querier.QueryContext(ctx, query, args)
		return err
	}); err != nil {
		return nil, err
	}

	return rows, nil
}

func (c *crdbConn) CheckNamedValue(namedValue *driver.NamedValue) error {
	namedValueChecker, ok := c.Conn.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}

	// Just check the named value for []string
	switch namedValue.Value.(type) {
	case []string:
		return namedValueChecker.CheckNamedValue(namedValue)
	}

	// If not skip
	return driver.ErrSkip
}

// crdbxDriver is a wrapper of driver.Driver.
type crdbxDriver struct {
	driver driver.Driver
	cfg    *config
}

func newDriver(dri driver.Driver, cfg *config) driver.Driver {
	if _, ok := dri.(driver.DriverContext); ok {
		return newCrdbxDriver(dri, cfg)
	}
	// Only implements driver.Driver
	return struct{ driver.Driver }{newCrdbxDriver(dri, cfg)}
}

// newcrdbxDriver returns a new crdbxDriver.
func newCrdbxDriver(drv driver.Driver, cfg *config) *crdbxDriver {
	return &crdbxDriver{driver: drv, cfg: cfg}
}

// Open implements driver.Driver interface.
func (c *crdbxDriver) Open(name string) (driver.Conn, error) {
	conn, err := c.driver.Open(name)
	if err != nil {
		return nil, err
	}
	return newConn(conn, c.cfg), nil
}

// OpenConnector implements driver.DriverContext interface.
func (c *crdbxDriver) OpenConnector(name string) (driver.Connector, error) {
	drv, ok := c.driver.(driver.DriverContext)
	if !ok {
		return nil, errors.New("crdbx: the driver to be wrapped must implement DriverContext")
	}
	connector, err := drv.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return newConnector(connector, c), err
}

// crdbConnector is a wrapper of driver.Connector.
type crdbConnector struct {
	driver.Connector
	driver *crdbxDriver
	cfg    *config
}

// newConnector returns a new crdbConnector.
func newConnector(connector driver.Connector, drv *crdbxDriver) *crdbConnector {
	return &crdbConnector{
		Connector: connector,
		driver:    drv,
		cfg:       drv.cfg,
	}
}

// Connect implements driver.Connector interface.
func (c *crdbConnector) Connect(ctx context.Context) (connection driver.Conn, err error) {
	connection, err = c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return newConn(connection, c.cfg), nil
}

// Driver implements driver.Connector interface.
func (c *crdbConnector) Driver() driver.Driver {
	return c.driver
}

// Option sets options such as the max number of retries.
type Option func(*config)

// WithMaxRetries creates an option to allow overriding the default number of retries.
func WithMaxRetries(maxRetries int) Option {
	return Option(func(cfg *config) {
		cfg.maxRetries = &maxRetries
	})
}

// dsnConnector is a wrapper of driver.Connector.
type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

// Connect implements driver.Connector interface.
func (t dsnConnector) Connect(_ context.Context) (driver.Conn, error) {
	return t.driver.Open(t.dsn)
}

// Driver implements driver.Connector interface.
func (t dsnConnector) Driver() driver.Driver {
	return t.driver
}

// Open is a wrapper over sql.Open with OTel instrumentation.
func Open(driverName, dataSourceName string, options ...Option) (*sql.DB, error) {
	// Retrieve the driver implementation we need to wrap with instrumentation
	db, err := sql.Open(driverName, "")
	if err != nil {
		return nil, err
	}
	d := db.Driver()
	if err = db.Close(); err != nil {
		return nil, err
	}

	// Generate a new crdbx driver and open a connector.
	drv := newCrdbxDriver(d, newConfig(options...))
	if _, ok := d.(driver.DriverContext); ok {
		connector, err := drv.OpenConnector(dataSourceName)
		if err != nil {
			return nil, err
		}
		return sql.OpenDB(connector), nil
	}

	// If the driver doesn't implement the driver context interface, we use the dsnConnector.
	return sql.OpenDB(dsnConnector{dsn: dataSourceName, driver: drv}), nil
}

var registerLock sync.Mutex

// Register initializes and registers crdbx wrapped database driver.
func Register(driverName string, options ...Option) (string, error) {
	// Retrieve the driver implementation we need to wrap with instrumentation
	db, err := sql.Open(driverName, "")
	if err != nil {
		return "", err
	}
	dri := db.Driver()
	if err = db.Close(); err != nil {
		return "", err
	}

	registerLock.Lock()
	defer registerLock.Unlock()

	sql.Register("crdbx", newDriver(dri, newConfig(options...)))

	return "", nil
}
