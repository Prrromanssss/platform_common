package pg

import (
	"context"
	"fmt"
	"log"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/Prrromanssss/platform_common/pkg/db"
)

type key string

const (
	TxKey key = "tx" // TxKey is the context key for database transactions.
)

type pg struct {
	dbc *pgxpool.Pool
}

// NewDB creates a new pg instance.
func NewDB(dbc *pgxpool.Pool) db.DB {
	return &pg{
		dbc: dbc,
	}
}

// ScanOneContext executes a query and scans a single row into dest.
func (p *pg) ScanOneContext(ctx context.Context, dest interface{}, q db.Query, args ...interface{}) error {
	logQuery(ctx, q, args...)
	row, err := p.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	return pgxscan.ScanOne(dest, row)
}

// ScanAllContext executes a query and scans all rows into dest.
func (p *pg) ScanAllContext(ctx context.Context, dest interface{}, q db.Query, args ...interface{}) error {
	logQuery(ctx, q, args...)
	rows, err := p.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	return pgxscan.ScanAll(dest, rows)
}

// ExecContext executes a query without returning any rows.
func (p *pg) ExecContext(ctx context.Context, q db.Query, args ...interface{}) (pgconn.CommandTag, error) {
	logQuery(ctx, q, args...)
	tx, ok := ctx.Value(TxKey).(pgx.Tx)
	if ok {
		return tx.Exec(ctx, q.QueryRaw, args...)
	}
	return p.dbc.Exec(ctx, q.QueryRaw, args...)
}

// QueryContext executes a query and returns the resulting rows.
func (p *pg) QueryContext(ctx context.Context, q db.Query, args ...interface{}) (pgx.Rows, error) {
	logQuery(ctx, q, args...)
	tx, ok := ctx.Value(TxKey).(pgx.Tx)
	if ok {
		return tx.Query(ctx, q.QueryRaw, args...)
	}
	return p.dbc.Query(ctx, q.QueryRaw, args...)
}

// QueryRowContext executes a query and returns a single row.
func (p *pg) QueryRowContext(ctx context.Context, q db.Query, args ...interface{}) pgx.Row {
	logQuery(ctx, q, args...)
	tx, ok := ctx.Value(TxKey).(pgx.Tx)
	if ok {
		return tx.QueryRow(ctx, q.QueryRaw, args...)
	}
	return p.dbc.QueryRow(ctx, q.QueryRaw, args...)
}

// BeginTx starts a new transaction with the given options.
func (p *pg) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return p.dbc.BeginTx(ctx, txOptions)
}

// Ping checks the database connection.
func (p *pg) Ping(ctx context.Context) error {
	return p.dbc.Ping(ctx)
}

// Close closes the database connection pool.
func (p *pg) Close() {
	p.dbc.Close()
}

// MakeContextTx returns a new context with the given transaction.
func MakeContextTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, TxKey, tx)
}

// logQuery logs the query and its arguments.
func logQuery(ctx context.Context, q db.Query, args ...interface{}) {
	log.Println(
		ctx,
		fmt.Sprintf("sql: %s", q.Name),
		fmt.Sprintf("query: %s", q),
		fmt.Sprintf("args: %+v", args...),
	)
}
