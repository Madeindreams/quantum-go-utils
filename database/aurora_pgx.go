package database

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/quantumauth-io/quantum-go-utils/retry"
	"go.elastic.co/apm/module/apmpgx/v2"
)

// AuroraPGXDatabase implements QuantumAuthDatabase using pgxpool against Aurora PostgreSQL.
type AuroraPGXDatabase struct {
	dbPool   *pgxpool.Pool
	settings DatabaseSettings
}

// NewAuroraPGXDatabase creates a NAT/Fargate-friendly pool and verifies connectivity.
func NewAuroraPGXDatabase(ctx context.Context, dbSettings DatabaseSettings) (QuantumAuthDatabase, error) {
	connStr, err := getConnectionString(dbSettings)
	if err != nil {
		return nil, err
	}

	retryCfg := retry.DefaultConfig()
	retryCfg.MaxDelayBeforeRetrying = 1 * time.Second
	retryCfg.MaxNumRetries = defaultMaxRetry

	result, err := retry.Retry(ctx, retryCfg,
		func(context.Context) ([]interface{}, error) {
			// IMPORTANT: connStr should already be URL-safe (or already a DSN).
			// If your getConnectionString returns "user:pass@host:port/db?params",
			// then we prefix with "postgres://" like your original code.
			cfg, err := pgxpool.ParseConfig(connStr)
			if err != nil {
				return nil, errors.Wrap(err, "error parsing pgxpool config")
			}

			// Pool sizing
			minPool := dbSettings.MinPoolSize
			if minPool == 0 {
				minPool = defaultMinDBPoolSize
			}
			maxPool := dbSettings.MaxPoolSize
			if maxPool == 0 {
				maxPool = defaultMaxDBPoolSize
			}
			cfg.MinConns = int32(minPool)
			cfg.MaxConns = int32(maxPool)

			// Fargate/NAT-friendly churn
			cfg.MaxConnLifetime = 60 * time.Second
			cfg.MaxConnIdleTime = 30 * time.Second
			cfg.HealthCheckPeriod = 15 * time.Second

			// Ensure connections don't hang forever
			cfg.ConnConfig.ConnectTimeout = 5 * time.Second

			// APM instrumentation: do it ONCE on the config
			apmpgx.Instrument(cfg.ConnConfig)

			// If you require TLS and want to be explicit.
			// Aurora typically works fine with sslmode=require in the DSN,
			// but this prevents accidental plaintext if your DSN builder is lax.
			if looksLikeSSLEnabled(connStr) && cfg.ConnConfig.TLSConfig == nil {
				cfg.ConnConfig.TLSConfig = &tls.Config{
					MinVersion: tls.VersionTLS12,
				}
			}

			dbPool, err := pgxpool.ConnectConfig(ctx, cfg)
			if err != nil {
				return nil, errors.Wrap(err, "error opening database")
			}

			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := dbPool.Ping(pingCtx); err != nil {
				dbPool.Close()
				return nil, errors.Wrap(err, "failed to ping database")
			}

			return []interface{}{&AuroraPGXDatabase{
				dbPool:   dbPool,
				settings: dbSettings,
			}}, nil
		},
		isRetryableAurora,
		"Database Connection (Aurora)",
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to instantiate db after retries")
	}

	return result[0].(*AuroraPGXDatabase), nil
}

func (db *AuroraPGXDatabase) GetSettings() DatabaseSettings {
	return db.settings
}

func (db *AuroraPGXDatabase) MigrateWithIOFS(ctx context.Context, src source.Driver) error {
	return migrateWithIOFS(ctx, src, db.settings)
}

func (db *AuroraPGXDatabase) GetTransaction(ctx context.Context) (QuantumAuthDatabaseTransaction, error) {
	// Aurora/Postgres-friendly default.
	// Use Serializable only where you truly need it.
	opts := pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	}

	retryCfg := retry.DefaultConfig()
	retryCfg.MaxDelayBeforeRetrying = 1 * time.Second
	retryCfg.MaxNumRetries = defaultMaxRetry

	result, err := retry.Retry(ctx, retryCfg,
		func(context.Context) ([]interface{}, error) {
			txn, err := db.dbPool.BeginTx(ctx, opts)
			if err != nil {
				return nil, errors.Wrap(err, "failed to begin transaction")
			}
			return []interface{}{&pgxTransaction{tx: txn}}, nil
		},
		isRetryableAurora,
		"Get DB Transaction (Aurora)",
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction after retries")
	}
	return result[0].(*pgxTransaction), nil
}

func (db *AuroraPGXDatabase) Exec(ctx context.Context, sql string, arguments ...interface{}) (QuantumAuthDatabaseExecResult, error) {
	retryCfg := retry.DefaultConfig()
	retryCfg.MaxDelayBeforeRetrying = 1 * time.Second
	retryCfg.MaxNumRetries = defaultMaxRetry

	result, err := retry.Retry(ctx, retryCfg,
		func(context.Context) ([]interface{}, error) {
			conn, err := db.dbPool.Acquire(ctx)
			if err != nil {
				return nil, err
			}
			defer conn.Release()

			cmd, err := conn.Exec(ctx, sql, arguments...)
			if err != nil {
				return nil, err
			}
			return []interface{}{&pgxDatabaseExecResult{cmdTag: cmd}}, nil
		},
		isRetryableAurora,
		"Database Exec (Aurora)",
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute %s after retries", sql)
	}
	return result[0].(*pgxDatabaseExecResult), nil
}

func (db *AuroraPGXDatabase) QueryRow(ctx context.Context, sql string, arguments ...interface{}) (QuantumAuthDatabaseRow, error) {
	// QueryRow doesn't execute until Scan, but returning it is fine.
	// We don’t retry here; the retry would need to wrap Scan which is caller-owned.
	// If you want retries for QueryRow, do them at repository layer where Scan occurs.
	conn, err := db.dbPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	// IMPORTANT: we cannot defer Release here, because QueryRow may be scanned later.
	// So we must not Acquire a pooled conn for QueryRow.
	// Instead use pool.QueryRow directly (it manages connection usage internally).
	conn.Release()

	return db.dbPool.QueryRow(ctx, sql, arguments...), nil
}

func (db *AuroraPGXDatabase) Query(ctx context.Context, sql string, arguments ...interface{}) (QuantumAuthDatabaseRows, error) {
	retryCfg := retry.DefaultConfig()
	retryCfg.MaxDelayBeforeRetrying = 1 * time.Second
	retryCfg.MaxNumRetries = defaultMaxRetry

	result, err := retry.Retry(ctx, retryCfg,
		func(context.Context) ([]interface{}, error) {
			rows, err := db.dbPool.Query(ctx, sql, arguments...)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to query %s", sql)
			}
			return []interface{}{&pgxDatabaseRows{rows: rows}}, nil
		},
		isRetryableAurora,
		"Database Query (Aurora)",
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to query %s after retries", sql)
	}
	return result[0].(*pgxDatabaseRows), nil
}

func (db *AuroraPGXDatabase) Close() error {
	db.dbPool.Close()
	return nil
}

func (db *AuroraPGXDatabase) Ping(ctx context.Context) error {
	return pingDB(ctx, db.dbPool.Ping)
}

// --- wrappers to satisfy your interfaces ---

type pgxTransaction struct {
	tx pgx.Tx
}

type pgxDatabaseExecResult struct {
	cmdTag pgconn.CommandTag
}

type pgxDatabaseRows struct {
	rows pgx.Rows
}

func (r *pgxDatabaseRows) Close() error { r.rows.Close(); return nil }
func (r *pgxDatabaseRows) Err() error   { return r.rows.Err() }
func (r *pgxDatabaseRows) Next() bool   { return r.rows.Next() }
func (r *pgxDatabaseRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *pgxDatabaseExecResult) RowsAffected() (int64, error) {
	return r.cmdTag.RowsAffected(), nil
}

// Transaction methods
func (t *pgxTransaction) Exec(ctx context.Context, sql string, arguments ...interface{}) (QuantumAuthDatabaseExecResult, error) {
	retryCfg := retry.DefaultConfig()
	retryCfg.MaxDelayBeforeRetrying = 1 * time.Second
	retryCfg.MaxNumRetries = defaultMaxRetry

	result, err := retry.Retry(ctx, retryCfg,
		func(context.Context) ([]interface{}, error) {
			cmd, err := t.tx.Exec(ctx, sql, arguments...)
			if err != nil {
				return nil, err
			}
			return []interface{}{&pgxDatabaseExecResult{cmdTag: cmd}}, nil
		},
		isRetryableAurora,
		"Database Tx Exec (Aurora)",
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute tx statement %s after retries", sql)
	}
	return result[0].(*pgxDatabaseExecResult), nil
}

func (t *pgxTransaction) Commit(ctx context.Context) error {
	retryCfg := retry.DefaultConfig()
	retryCfg.MaxDelayBeforeRetrying = 1 * time.Second
	retryCfg.MaxNumRetries = defaultMaxRetry

	_, err := retry.Retry(ctx, retryCfg,
		func(context.Context) ([]interface{}, error) {
			if err := t.tx.Commit(ctx); err != nil {
				return nil, err
			}
			return nil, nil
		},
		isRetryableAurora,
		"Database Tx Commit (Aurora)",
	)
	if err != nil {
		return errors.Wrap(err, "failed to commit transaction after retries")
	}
	return nil
}

func (t *pgxTransaction) Rollback(ctx context.Context) error {
	retryCfg := retry.DefaultConfig()
	retryCfg.MaxDelayBeforeRetrying = 1 * time.Second
	retryCfg.MaxNumRetries = defaultMaxRetry

	_, err := retry.Retry(ctx, retryCfg,
		func(context.Context) ([]interface{}, error) {
			if err := t.tx.Rollback(ctx); err != nil {
				return nil, err
			}
			return nil, nil
		},
		isRetryableAurora,
		"Database Tx Rollback (Aurora)",
	)
	if err != nil {
		return errors.Wrap(err, "failed to rollback transaction after retries")
	}
	return nil
}

// --- Aurora retry classifier ---
//
// NOTE: Retrying writes can duplicate effects if the statement isn't idempotent.
// Best practice: for "create" operations use request-id / unique keys / ON CONFLICT patterns.
func isRetryableAurora(err error) bool {
	if err == nil {
		return false
	}

	// Network-ish transient errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		// timeout or temporary network errors
		if netErr.Timeout() {
			return true
		}
		// net.Error doesn't always expose Temporary() anymore consistently;
		// still treat as retryable if it's a net.Error.
		return true
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "server closed the connection") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "eof") {
		return true
	}

	// PostgreSQL SQLSTATE handling
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		// serialization_failure (if you ever run SERIALIZABLE transactions)
		case "40001":
			return true
		// deadlock_detected
		case "40P01":
			return true
		// connection_exception-ish / admin shutdown (during failover)
		case "57P01", "57P02", "57P03":
			return true
		// too_many_connections could be transient if pool bursts
		case "53300":
			return true
		}
	}

	return false
}

func looksLikeSSLEnabled(connStr string) bool {
	s := strings.ToLower(connStr)
	return strings.Contains(s, "sslmode=require") ||
		strings.Contains(s, "sslmode=verify-ca") ||
		strings.Contains(s, "sslmode=verify-full")
}

// --- IMPORTANT NOTE ABOUT QueryRow ---
//
// pgxpool.Pool.QueryRow returns a Row backed by an internal connection usage.
// This is safe for your interface (caller Scan immediately as usual).
//
// If you have a pattern where you return Row and scan later in another goroutine,
// that pattern is unsafe regardless of pool/driver.
