package postgres

import (
	"context"
	"database/sql"

	"golang.org/x/xerrors"
)

const schema = `
CREATE TABLE IF NOT EXISTS blocks (
    height BIGINT NOT NULL,
    tag INTEGER NOT NULL,
    hash VARCHAR(66) NOT NULL,
    parent_hash VARCHAR(66) NOT NULL,
    parent_height BIGINT NOT NULL,
    object_key_main VARCHAR(255),
    timestamp BIGINT NOT NULL,
    skipped BOOLEAN NOT NULL DEFAULT FALSE,
    is_canonical BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (height, hash)
);

CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height);
CREATE INDEX IF NOT EXISTS idx_blocks_tag ON blocks(tag);
CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp);
CREATE INDEX IF NOT EXISTS idx_blocks_skipped ON blocks(skipped);
CREATE INDEX IF NOT EXISTS idx_blocks_is_canonical ON blocks(is_canonical);

CREATE TABLE IF NOT EXISTS events (
    event_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    tag INTEGER NOT NULL,
    event_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_events_block_height ON events(block_height);
CREATE INDEX IF NOT EXISTS idx_events_tag ON events(tag);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_hash VARCHAR(66) NOT NULL,
    block_height BIGINT NOT NULL,
    tag INTEGER NOT NULL,
    transaction_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_hash, tag)
);
`

func applySchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, schema)
	if err != nil {
		return xerrors.Errorf("failed to apply database schema: %w", err)
	}
	return nil
}
