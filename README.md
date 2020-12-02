# crdbx - CockroachDB Driver

`crdbx` is a golang database driver that wraps https://github.com/jackc/pgx `database/sql` implementation (stdlib),
extending it by providing a retry-mechanism for `ExecContext` and `QueryContext` methods suitable to `CockroachDB`.

For the latter, we use https://github.com/cockroachdb/cockroach-go `Execute` function that provides retry mechanism 
of single statements.

More info about why we need to retry mechanism at all in CockroachDB:

https://www.cockroachlabs.com/docs/stable/error-handling-and-troubleshooting.html#transaction-retry-errors