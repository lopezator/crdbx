package crdbx_test

import (
	"context"
	"database/sql"
	"flag"
	"os"
	"testing"

	"github.com/lopezator/crdbx"
)

var (
	databaseURL *string
	ctx         context.Context
)

func TestMain(m *testing.M) {
	// Initialize the context
	ctx = context.Background()

	// Parse the flag, if any, and if not, get the data from the environment.
	databaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "Database URL")
	flag.Parse()

	// Register the crdbx driver.
	if err := crdbx.Register(); err != nil {
		panic(err)
	}

	// Run tests and exit.
	os.Exit(m.Run())
}

func TestExecContext(t *testing.T) {
	// Open connection to the database.
	db, err := sql.Open("crdbx", *databaseURL)
	if err != nil {
		t.Fatal(err)
	}

	// Test the exec context method.
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPSERT INTO test (id) VALUES (1);"); err != nil {
		t.Fatal(err)
	}
}

func TestQueryContext(t *testing.T) {
	// Open connection to the database.
	db, err := sql.Open("crdbx", *databaseURL)
	if err != nil {
		t.Fatal(err)
	}

	// Test the query context method.
	if _, err := db.QueryContext(ctx, "SELECT * FROM test"); err != nil {
		t.Fatal(err)
	}
}
