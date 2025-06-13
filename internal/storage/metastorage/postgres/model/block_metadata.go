package model

import (
	"database/sql"

	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// getParentHeight calculates parent height from current height
func getParentHeight(height uint64) uint64 {
	if height == 0 {
		return 0
	}
	return height - 1
}

// BlockMetadataFromRow converts a postgres row into a BlockMetadata proto
// Used for direct block_metadata table queries
// Schema: id, height, tag, hash, parent_hash, object_key_main, timestamp, skipped
func BlockMetadataFromRow(row *sql.Row) (*api.BlockMetadata, error) {
	var block api.BlockMetadata
	var timestamp int64
	var id int64 // We get this but don't need it in the result
	err := row.Scan(
		&id,
		&block.Height,
		&block.Tag,
		&block.Hash,
		&block.ParentHash,
		&block.ObjectKeyMain,
		&timestamp,
		&block.Skipped,
	)
	if err != nil {
		return nil, err
	}
	block.Timestamp = utils.ToTimestamp(timestamp)
	block.ParentHeight = getParentHeight(block.Height)
	return &block, nil
}

// BlockMetadataFromCanonicalRow converts a postgres row from canonical join into a BlockMetadata proto
// Used for queries that join canonical_blocks with block_metadata
// Schema: bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.object_key_main, bm.timestamp, bm.skipped, cb.sequence_number
func BlockMetadataFromCanonicalRow(row *sql.Row) (*api.BlockMetadata, error) {
	var block api.BlockMetadata
	var timestamp int64
	var id int64             // block_metadata.id
	var sequenceNumber int64 // canonical_blocks.sequence_number
	err := row.Scan(
		&id,
		&block.Height,
		&block.Tag,
		&block.Hash,
		&block.ParentHash,
		&block.ObjectKeyMain,
		&timestamp,
		&block.Skipped,
		&sequenceNumber,
	)
	if err != nil {
		return nil, err
	}
	block.Timestamp = utils.ToTimestamp(timestamp)
	block.ParentHeight = getParentHeight(block.Height)
	return &block, nil
}

// BlockMetadataFromRows converts multiple postgres rows into BlockMetadata protos
// Used for direct block_metadata table queries
func BlockMetadataFromRows(rows *sql.Rows) ([]*api.BlockMetadata, error) {
	var blocks []*api.BlockMetadata
	for rows.Next() {
		var block api.BlockMetadata
		var timestamp int64
		var id int64
		err := rows.Scan(
			&id,
			&block.Height,
			&block.Tag,
			&block.Hash,
			&block.ParentHash,
			&block.ObjectKeyMain,
			&timestamp,
			&block.Skipped,
		)
		if err != nil {
			return nil, err
		}
		block.Timestamp = utils.ToTimestamp(timestamp)
		block.ParentHeight = getParentHeight(block.Height)
		blocks = append(blocks, &block)
	}
	return blocks, nil
}

// BlockMetadataFromCanonicalRows converts multiple postgres rows from canonical joins into BlockMetadata protos
// Used for queries that join canonical_blocks with block_metadata
func BlockMetadataFromCanonicalRows(rows *sql.Rows) ([]*api.BlockMetadata, error) {
	var blocks []*api.BlockMetadata
	for rows.Next() {
		var block api.BlockMetadata
		var timestamp int64
		var id int64
		var sequenceNumber int64
		err := rows.Scan(
			&id,
			&block.Height,
			&block.Tag,
			&block.Hash,
			&block.ParentHash,
			&block.ObjectKeyMain,
			&timestamp,
			&block.Skipped,
			&sequenceNumber,
		)
		if err != nil {
			return nil, err
		}
		block.Timestamp = utils.ToTimestamp(timestamp)
		block.ParentHeight = getParentHeight(block.Height)
		blocks = append(blocks, &block)
	}
	return blocks, nil
}
