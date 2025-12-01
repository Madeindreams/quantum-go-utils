package database

import (
	"context"
	"database/sql"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

type QuantumAuthDatabase interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (QuantumAuthDatabaseExecResult, error)
	QueryRow(ctx context.Context, sql string, arguments ...interface{}) (QuantumAuthDatabaseRow, error)
	Query(ctx context.Context, sql string, arguments ...interface{}) (QuantumAuthDatabaseRows, error)
	GetTransaction(ctx context.Context) (QuantumAuthDatabaseTransaction, error)
	Close() error
	Ping(ctx context.Context) error
	MigrateWithIOFS(ctx context.Context, source source.Driver) error
}

type QuantumAuthDatabaseRow interface {
	Scan(dest ...interface{}) error
}

type QuantumAuthDatabaseRows interface {
	Err() error
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
}

type QuantumAuthDatabaseExecResult interface {
	RowsAffected() (int64, error)
}

type QuantumAuthDatabaseTransaction interface {
	Rollback(ctx context.Context) error
	Commit(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...interface{}) (QuantumAuthDatabaseExecResult, error)
}

var ErrNoRows = errors.New("no rows in result set")

func ConditionallyConvertToErrNoRows(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoRows
	} else if errors.Is(err, sql.ErrNoRows) {
		return ErrNoRows
	}
	return err
}
