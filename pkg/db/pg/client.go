package pg

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"

	"github.com/Prrromanssss/platform_common/pkg/db"
)

type pgClient struct {
	masterDBC db.DB
}

// New initializes and returns a new pgClient instance.
func New(ctx context.Context, dsn string) (db.Client, error) {
	dbc, err := pgxpool.Connect(ctx, dsn)
	if err != nil {
		return nil, errors.Errorf("failed to connect to db: %v", err)
	}

	return &pgClient{
		masterDBC: &pg{dbc: dbc},
	}, nil
}

// DB returns the master database connection.
func (c *pgClient) DB() db.DB {
	return c.masterDBC
}

// Close closes the database connection.
func (c *pgClient) Close() error {
	if c.masterDBC != nil {
		c.masterDBC.Close()
	}

	return nil
}
