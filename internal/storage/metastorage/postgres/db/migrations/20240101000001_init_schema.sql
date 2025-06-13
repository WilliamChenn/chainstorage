-- +goose Up
-- Create block_metadata table (append-only storage for every block ever observed)
CREATE TABLE block_metadata (
    id BIGSERIAL PRIMARY KEY, -- for canonical_blocks and event fk reference
    height BIGINT NOT NULL,
    tag INT NOT NULL,
    hash VARCHAR(66) NOT NULL UNIQUE, -- can hold a "0x"+64-hex string
    parent_hash VARCHAR(66),
    object_key_main VARCHAR(255),
    timestamp TIMESTAMPTZ NOT NULL,
    skipped BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (tag, hash)
);

CREATE INDEX idx_block_meta_tag_height ON block_metadata(tag, height);

-- Create canonical_blocks table (track the "winning" block at each height)
CREATE TABLE canonical_blocks (
    height BIGINT NOT NULL,
    block_metadata_id BIGINT NOT NULL,
    tag INT NOT NULL,
    sequence_number BIGINT NOT NULL DEFAULT 0,
    -- Constraints
    PRIMARY KEY (height, tag, sequence_number),
    UNIQUE (height, tag, block_metadata_id), -- Prevent same block from being canonical multiple times
    FOREIGN KEY (block_metadata_id) REFERENCES block_metadata(id) ON DELETE RESTRICT
);

-- Supports: GetLatestBlock() - finds max sequence_number for a tag
CREATE INDEX idx_canonical_sequence ON canonical_blocks (tag, sequence_number DESC);
-- Supports: JOINs between canonical_blocks and block_metadata tables
CREATE INDEX idx_canonical_block_metadata_fk ON canonical_blocks (block_metadata_id);

-- Create block_events table (append-only stream of all blockchain state changes)
CREATE TYPE event_type_enum AS ENUM ('BLOCK_ADDED', 'BLOCK_REMOVED', 'UNKNOWN');

CREATE TABLE block_events (
    event_tag INT NOT NULL DEFAULT 0, -- version
    event_sequence BIGINT NOT NULL, -- monotonically-increasing per tag
    event_type event_type_enum NOT NULL,
    block_metadata_id BIGINT NOT NULL, -- fk referencing block_metadata
    height BIGINT NOT NULL,
    -- Constraints
    PRIMARY KEY (event_tag, event_sequence),
    FOREIGN KEY (block_metadata_id) REFERENCES block_metadata(id) ON DELETE CASCADE
);

-- Supports: GetEventsByBlockHeight(), GetFirstEventIdByBlockHeight()
CREATE INDEX idx_events_height_tag ON block_events(height, event_tag);
-- Supports: JOINs to get full block details from events
CREATE INDEX idx_events_block_meta ON block_events(block_metadata_id);

-- Create transactions table (for transaction hash to block mapping)
CREATE TABLE transactions (
    transaction_hash VARCHAR(66) NOT NULL,
    block_height BIGINT NOT NULL,
    tag INT NOT NULL,
    transaction_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_hash, tag)
); 

-- +goose Down
-- Drop tables in reverse order due to foreign key constraints
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS block_events;
DROP TABLE IF EXISTS canonical_blocks;
DROP TABLE IF EXISTS block_metadata;

-- Drop custom types
DROP TYPE IF EXISTS event_type_enum; 