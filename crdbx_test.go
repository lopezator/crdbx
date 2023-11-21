package crdbx_test

import (
	"context"
	"database/sql"
	"flag"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib" // force psql driver import
	"github.com/lopezator/crdbx"
)

// Initialize test variables.
var (
	databaseURL *string
	ctx         context.Context
	db          *sql.DB
)

// TestMain initializes the context, parses the flag, if any, and if not, gets the data from the environment,
// opens the database, runs tests and exits.
func TestMain(m *testing.M) {
	databaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "Database URL")
	flag.Parse()
	ctx = context.Background()
	var err error
	db, err = crdbx.Open("pgx", *databaseURL, crdbx.WithMaxRetries(5))
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// TestExecContext tests the exec context method.
func TestExecContext(t *testing.T) {
	// This update is sterile, is just to test that the exec context method works.
	if _, err := db.ExecContext(ctx, "UPDATE users SET id = 'foo' WHERE ID = 'foo';"); err != nil {
		t.Fatal(err)
	}
}

// TestQueryContext tests the query context method.
func TestQueryContext(t *testing.T) {
	// nolint:gocritic // This select is sterile, is just to test that the query context method works.
	if _, err := db.QueryContext(ctx, "SELECT id FROM users WHERE id = 'foo'"); err != nil {
		t.Fatal(err)
	}
}
