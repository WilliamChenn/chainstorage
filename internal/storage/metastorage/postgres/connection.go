package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/coinbase/chainstorage/internal/config"
	_ "github.com/lib/pq"
	"golang.org/x/xerrors"
)

func newDBConnection(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	// Build PostgreSQL connection string
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)

	// Open database connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MinConnections)
	db.SetConnMaxLifetime(cfg.MaxLifetime)
	db.SetConnMaxIdleTime(cfg.MaxIdleTime)

	if err := db.PingContext(ctx); err != nil {
		return nil, xerrors.Errorf("failed to ping database: %w", err)
	}
	if err := applySchema(ctx, db); err != nil {
		return nil, xerrors.Errorf("failed to apply schema: %w", err)
	}

	return db, nil
}
