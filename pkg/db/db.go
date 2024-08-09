package db

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

// Handler is a function that is executed within a transaction.
type Handler func(ctx context.Context) error

// Client is a client interface for interacting with the database.
type Client interface {
	DB() DB
	Close() error
}

// TxManager is a transaction manager that executes a user-specified handler within a transaction.
type TxManager interface {
	ReadCommitted(ctx context.Context, f Handler) error
}

// Query is a wrapper for a query that stores the query name and the query itself.
// The query name is used for logging and potentially for other purposes, such as tracing.
type Query struct {
	Name     string
	QueryRaw string
}

// Transactor is an interface for working with transactions.
type Transactor interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// SQLExecer combines the NamedExecer and QueryExecer interfaces.
type SQLExecer interface {
	NamedExecer
	QueryExecer
}

// NamedExecer is an interface for executing named queries using struct tags.
type NamedExecer interface {
	ScanOneContext(ctx context.Context, dest interface{}, q Query, args ...interface{}) error
	ScanAllContext(ctx context.Context, dest interface{}, q Query, args ...interface{}) error
}

// QueryExecer is an interface for executing regular queries.
type QueryExecer interface {
	ExecContext(ctx context.Context, q Query, args ...interface{}) (pgconn.CommandTag, error)
	QueryContext(ctx context.Context, q Query, args ...interface{}) (pgx.Rows, error)
	QueryRowContext(ctx context.Context, q Query, args ...interface{}) pgx.Row
	SendBatchContext(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Pinger is an interface for checking the database connection.
type Pinger interface {
	Ping(ctx context.Context) error
}

// DB is an interface for interacting with the database.
type DB interface {
	SQLExecer
	Pinger
	Transactor
	Close()
}
