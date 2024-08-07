package transaction

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"

	"github.com/Prrromanssss/platform_common/pkg/db"
	"github.com/Prrromanssss/platform_common/pkg/db/pg"
)

type manager struct {
	db db.Transactor
}

// NewTransactionManager creates a new transaction manager that implements the db.TxManager interface.
func NewTransactionManager(db db.Transactor) db.TxManager {
	return &manager{
		db: db,
	}
}

// transaction is the main function that executes the user-specified handler within a transaction.
func (m *manager) transaction(ctx context.Context, opts pgx.TxOptions, fn db.Handler) (err error) {
	// If this is a nested transaction, skip initiating a new transaction and execute the handler.
	tx, ok := ctx.Value(pg.TxKey).(pgx.Tx)
	if ok {
		return fn(ctx)
	}

	// Start a new transaction.
	tx, err = m.db.BeginTx(ctx, opts)
	if err != nil {
		return errors.Wrap(err, "can't begin transaction")
	}

	// Store the transaction in the context.
	ctx = pg.MakeContextTx(ctx, tx)

	// Set up a deferred function for rolling back or committing the transaction.
	defer func() {
		// Recover from panic
		if r := recover(); r != nil {
			err = errors.Errorf("panic recovered: %v", r)
		}

		// Rollback the transaction if an error occurred
		if err != nil {
			if errRollback := tx.Rollback(ctx); errRollback != nil {
				err = errors.Wrapf(err, "errRollback: %v", errRollback)
			}

			return
		}

		// Commit the transaction if no errors occurred
		if err == nil {
			err = tx.Commit(ctx)
			if err != nil {
				err = errors.Wrap(err, "tx commit failed")
			}
		}
	}()

	// Execute the code inside the transaction.
	// If the function fails, return an error, and the deferred function will roll back
	// otherwise, the transaction is committed.
	if err = fn(ctx); err != nil {
		err = errors.Wrap(err, "failed executing code inside transaction")
	}

	return err
}

func (m *manager) ReadCommitted(ctx context.Context, f db.Handler) error {
	txOpts := pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
	return m.transaction(ctx, txOpts, f)
}
