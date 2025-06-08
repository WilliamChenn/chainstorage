package model

import (
	"database/sql"

	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// BlockMetadataFromRow converts a postgres row into a BlockMetadata proto
func BlockMetadataFromRow(row *sql.Row) (*api.BlockMetadata, error) {
	var block api.BlockMetadata
	var timestamp int64
	var isCanonical bool
	err := row.Scan(
		&block.Height,
		&block.Tag,
		&block.Hash,
		&block.ParentHash,
		&block.ParentHeight,
		&block.ObjectKeyMain,
		&timestamp,
		&block.Skipped,
		&isCanonical,
	)
	if err != nil {
		return nil, err
	}
	block.Timestamp = utils.ToTimestamp(timestamp)
	return &block, nil
}

// convert rows to blocks
func BlockMetadataFromRows(rows *sql.Rows) ([]*api.BlockMetadata, error) {
	var blocks []*api.BlockMetadata
	var timestamp int64
	for rows.Next() {
		var block api.BlockMetadata
		var isCanonical bool
		err := rows.Scan(
			&block.Height,
			&block.Tag,
			&block.Hash,
			&block.ParentHash,
			&block.ParentHeight,
			&block.ObjectKeyMain,
			&timestamp,
			&block.Skipped,
			&isCanonical,
		)
		if err != nil {
			return nil, err
		}
		block.Timestamp = utils.ToTimestamp(timestamp)
		blocks = append(blocks, &block)
	}
	return blocks, nil
}
