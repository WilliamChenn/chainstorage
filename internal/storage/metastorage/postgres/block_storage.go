package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres/model"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/xerrors"
)

type (
	blockStorageImpl struct {
		db                               *sql.DB
		blockStartHeight                 uint64
		instrumentPersistBlockMetas      instrument.Instrument
		instrumentGetLatestBlock         instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlockByHash         instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlockByHeight       instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlocksByHeightRange instrument.InstrumentWithResult[[]*api.BlockMetadata]
		instrumentGetBlocksByHeights     instrument.InstrumentWithResult[[]*api.BlockMetadata]
	}
)

func newBlockStorage(db *sql.DB, params Params) (internal.BlockStorage, error) {
	metrics := params.Metrics.SubScope("block_storage").Tagged(map[string]string{
		"storage_type": "postgres",
	})
	accessor := blockStorageImpl{
		db:                               db,
		blockStartHeight:                 params.Config.Chain.BlockStartHeight,
		instrumentPersistBlockMetas:      instrument.New(metrics, "persist_block_metas"),
		instrumentGetLatestBlock:         instrument.NewWithResult[*api.BlockMetadata](metrics, "get_latest_block"),
		instrumentGetBlockByHash:         instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_hash"),
		instrumentGetBlockByHeight:       instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_height"),
		instrumentGetBlocksByHeightRange: instrument.NewWithResult[[]*api.BlockMetadata](metrics, "get_blocks_by_height_range"),
		instrumentGetBlocksByHeights:     instrument.NewWithResult[[]*api.BlockMetadata](metrics, "get_blocks_by_heights"),
	}
	return &accessor, nil
}

func (b *blockStorageImpl) PersistBlockMetas(
	ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
	return b.instrumentPersistBlockMetas.Instrument(ctx, func(ctx context.Context) error {
		// `updateWatermark` is ignored in Postgres implementation because we can always find the latest
		// block by querying the maximum height in canonical_blocks for a tag.
		//For each new block we simply
		// insert or update the canonical_blocks row at its height.
		if len(blocks) == 0 {
			return nil
		}
		// Sort blocks by height for chain validation
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Height < blocks[j].Height
		})
		if err := parser.ValidateChain(blocks, lastBlock); err != nil {
			return xerrors.Errorf("failed to validate chain: %w", err)
		}
		tx, err := b.db.BeginTx(ctx, nil)
		if err != nil {
			return xerrors.Errorf("failed to begin transaction: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback()
			}
		}()
		//insert all blocks into block_metadata table (append-only)
		//insert only non-skipped blocks into canonical_blocks table
		blockMetadataQuery := `
			INSERT INTO block_metadata (height, tag, hash, parent_hash, object_key_main, timestamp, skipped) 
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (tag, hash) DO UPDATE SET
				height = EXCLUDED.height,
				parent_hash = EXCLUDED.parent_hash,
				object_key_main = EXCLUDED.object_key_main,
				timestamp = EXCLUDED.timestamp,
				skipped = EXCLUDED.skipped
			RETURNING id`
		canonicalQuery := `
			INSERT INTO canonical_blocks (height, block_metadata_id, tag)
			VALUES ($1, $2, $3)
			ON CONFLICT (height, tag) DO UPDATE
			SET block_metadata_id = EXCLUDED.block_metadata_id`
		for _, block := range blocks {
			tsProto := block.GetTimestamp()
			var goTime time.Time
			if tsProto == nil { //special case for genesis block
				goTime = time.Unix(0, 0)
			} else {
				goTime, err = ptypes.Timestamp(tsProto) //convert timestamp to time.Time
				if err != nil {
					return xerrors.Errorf("invalid block timestamp at height %d: %w", block.Height, err)
				}
			}
			var blockId int64
			err = tx.QueryRowContext(ctx, blockMetadataQuery,
				block.Height,
				block.Tag,
				block.Hash,
				block.ParentHash,
				block.ObjectKeyMain,
				goTime,
				block.Skipped,
			).Scan(&blockId)
			if err != nil {
				return xerrors.Errorf("failed to insert block metadata for height %d: %w", block.Height, err)
			}

			// Only insert non-skipped blocks into canonical_blocks
			if !block.Skipped {
				_, err = tx.ExecContext(ctx, canonicalQuery,
					block.Height,
					blockId,
					block.Tag,
				)
				if err != nil {
					return xerrors.Errorf("failed to insert canonical block for height %d: %w", block.Height, err)
				}
			}
		}
		// Commit transaction
		err = tx.Commit()
		if err != nil {
			return xerrors.Errorf("failed to commit transaction: %w", err)
		}
		committed = true
		return nil
	})
}

func (b *blockStorageImpl) GetLatestBlock(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
	return b.instrumentGetLatestBlock.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		// Get the latest canonical block by highest height
		query := `
			SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1
			ORDER BY cb.height DESC
			LIMIT 1`
		row := b.db.QueryRowContext(ctx, query, tag)
		block, err := model.BlockMetadataFromCanonicalRow(b.db, row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("no latest block found: %w", errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get latest block: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlockByHash(ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error) {
	return b.instrumentGetBlockByHash.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		if err := b.validateHeight(height); err != nil {
			return nil, err
		}
		var row *sql.Row
		if blockHash == "" {
			// First try to get the canonical block at this height
			canonicalQuery := `
				SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
				FROM canonical_blocks cb
				JOIN block_metadata bm ON cb.block_metadata_id = bm.id
				WHERE cb.tag = $1 AND cb.height = $2
				ORDER BY cb.height DESC`
			row = b.db.QueryRowContext(ctx, canonicalQuery, tag, height)
			block, err := model.BlockMetadataFromCanonicalRow(b.db, row)
			if err == nil {
				return block, nil
			}
			if err != sql.ErrNoRows {
				return nil, xerrors.Errorf("failed to get canonical block by hash: %w", err)
			}

			// If no canonical block found, try to get a skipped block at this height
			skippedQuery := `
				SELECT id, height, tag, hash, parent_hash, object_key_main, 
			       EXTRACT(EPOCH FROM timestamp)::BIGINT, skipped
				FROM block_metadata 
				WHERE tag = $1 AND height = $2 AND skipped = true
				LIMIT 1`
			row = b.db.QueryRowContext(ctx, skippedQuery, tag, height)
		} else {
			// Query block_metadata directly by hash (may not be canonical)
			query := `
			SELECT id, height, tag, hash, parent_hash, object_key_main, 
			       EXTRACT(EPOCH FROM timestamp)::BIGINT, skipped
			FROM block_metadata 
			WHERE tag = $1 AND height = $2 AND hash = $3`
			row = b.db.QueryRowContext(ctx, query, tag, height, blockHash)
		}

		block, err := model.BlockMetadataFromRow(b.db, row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("block not found: %w", errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get block by hash: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlockByHeight(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
	return b.instrumentGetBlockByHeight.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		if err := b.validateHeight(height); err != nil {
			return nil, err
		}
		// First try to get the canonical block at this height
		canonicalQuery := `
			SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height = $2
			LIMIT 1`
		row := b.db.QueryRowContext(ctx, canonicalQuery, tag, height)
		block, err := model.BlockMetadataFromCanonicalRow(b.db, row)
		if err == nil {
			return block, nil
		}
		if err != sql.ErrNoRows {
			return nil, xerrors.Errorf("failed to get canonical block by height: %w", err)
		}

		// If no canonical block found, try to get a skipped block at this height
		skippedQuery := `
			SELECT id, height, tag, hash, parent_hash, object_key_main, 
			       EXTRACT(EPOCH FROM timestamp)::BIGINT, skipped
			FROM block_metadata 
			WHERE tag = $1 AND height = $2 AND skipped = true
			LIMIT 1`
		row = b.db.QueryRowContext(ctx, skippedQuery, tag, height)
		block, err = model.BlockMetadataFromRow(b.db, row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("block at height %d not found: %w", height, errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get skipped block by height: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*api.BlockMetadata, error) {
	return b.instrumentGetBlocksByHeightRange.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		if startHeight >= endHeight {
			return nil, errors.ErrOutOfRange
		}
		if err := b.validateHeight(startHeight); err != nil {
			return nil, err
		}

		// Get canonical blocks in the height range
		canonicalQuery := `
			SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height >= $2 AND cb.height < $3
			ORDER BY cb.height ASC`
		rows, err := b.db.QueryContext(ctx, canonicalQuery, tag, startHeight, endHeight)
		if err != nil {
			return nil, xerrors.Errorf("failed to query canonical blocks by height range: %w", err)
		}
		defer rows.Close()

		canonicalBlocks, err := model.BlockMetadataFromCanonicalRows(b.db, rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan canonical block rows: %w", err)
		}

		// Get skipped blocks in the height range (not in canonical_blocks)
		skippedQuery := `
			SELECT id, height, tag, hash, parent_hash, object_key_main, 
			       EXTRACT(EPOCH FROM timestamp)::BIGINT, skipped
			FROM block_metadata bm
			WHERE bm.tag = $1 AND bm.height >= $2 AND bm.height < $3 AND bm.skipped = true
			  AND NOT EXISTS (
				  SELECT 1 FROM canonical_blocks cb 
				  WHERE cb.block_metadata_id = bm.id AND cb.tag = $1
			  )
			ORDER BY bm.height ASC`
		rows, err = b.db.QueryContext(ctx, skippedQuery, tag, startHeight, endHeight)
		if err != nil {
			return nil, xerrors.Errorf("failed to query skipped blocks by height range: %w", err)
		}
		defer rows.Close()

		skippedBlocks, err := model.BlockMetadataFromRows(b.db, rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan skipped block rows: %w", err)
		}

		// Merge canonical and skipped blocks, sort by height
		allBlocks := make([]*api.BlockMetadata, 0, len(canonicalBlocks)+len(skippedBlocks))
		allBlocks = append(allBlocks, canonicalBlocks...)
		allBlocks = append(allBlocks, skippedBlocks...)

		// Sort by height to ensure proper ordering
		sort.Slice(allBlocks, func(i, j int) bool {
			return allBlocks[i].Height < allBlocks[j].Height
		})

		// Check if we have all blocks in the range (no gaps)
		expectedCount := int(endHeight - startHeight)
		if len(allBlocks) != expectedCount {
			return nil, xerrors.Errorf("missing blocks in range [%d, %d): expected %d, got %d: %w",
				startHeight, endHeight, expectedCount, len(allBlocks), errors.ErrItemNotFound)
		}

		// Verify no gaps in heights
		for i, block := range allBlocks {
			expectedHeight := startHeight + uint64(i)
			if block.Height != expectedHeight {
				return nil, xerrors.Errorf("gap in block heights: expected %d, got %d: %w",
					expectedHeight, block.Height, errors.ErrItemNotFound)
			}
		}

		return allBlocks, nil
	})
}

func (b *blockStorageImpl) GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*api.BlockMetadata, error) {
	return b.instrumentGetBlocksByHeights.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		for _, height := range heights {
			if err := b.validateHeight(height); err != nil {
				return nil, err
			}
		}
		if len(heights) == 0 {
			return []*api.BlockMetadata{}, nil
		}
		// Build dynamic query with placeholders for IN clause for canonical blocks
		placeholders := make([]string, len(heights))
		args := make([]interface{}, len(heights)+1)
		args[0] = tag // First argument is tag
		for i, height := range heights {
			placeholders[i] = fmt.Sprintf("$%d", i+2) // Start from $2 since $1 is tag
			args[i+1] = height
		}
		canonicalQuery := fmt.Sprintf(`
			SELECT bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.object_key_main, 
			       EXTRACT(EPOCH FROM bm.timestamp)::BIGINT, bm.skipped
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height IN (%s)
			ORDER BY cb.height ASC`,
			strings.Join(placeholders, ", "))

		rows, err := b.db.QueryContext(ctx, canonicalQuery, args...)
		if err != nil {
			return nil, xerrors.Errorf("failed to query canonical blocks by heights: %w", err)
		}
		defer rows.Close()

		canonicalBlocks, err := model.BlockMetadataFromCanonicalRows(b.db, rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan canonical block rows: %w", err)
		}

		// Build query for skipped blocks at the requested heights
		skippedQuery := fmt.Sprintf(`
			SELECT id, height, tag, hash, parent_hash, object_key_main, 
			       EXTRACT(EPOCH FROM timestamp)::BIGINT, skipped
			FROM block_metadata bm
			WHERE bm.tag = $1 AND bm.height IN (%s) AND bm.skipped = true
			  AND NOT EXISTS (
				  SELECT 1 FROM canonical_blocks cb 
				  WHERE cb.block_metadata_id = bm.id AND cb.tag = $1
			  )
			ORDER BY bm.height ASC`,
			strings.Join(placeholders, ", "))

		rows, err = b.db.QueryContext(ctx, skippedQuery, args...)
		if err != nil {
			return nil, xerrors.Errorf("failed to query skipped blocks by heights: %w", err)
		}
		defer rows.Close()

		skippedBlocks, err := model.BlockMetadataFromRows(b.db, rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan skipped block rows: %w", err)
		}

		// Merge canonical and skipped blocks
		allBlocks := make([]*api.BlockMetadata, 0, len(canonicalBlocks)+len(skippedBlocks))
		allBlocks = append(allBlocks, canonicalBlocks...)
		allBlocks = append(allBlocks, skippedBlocks...)

		// Verify we got all requested blocks and return them in the same order as requested
		blockMap := make(map[uint64]*api.BlockMetadata)
		for _, block := range allBlocks {
			blockMap[block.Height] = block
		}

		orderedBlocks := make([]*api.BlockMetadata, len(heights))
		for i, height := range heights {
			block, exists := blockMap[height]
			if !exists {
				return nil, xerrors.Errorf("block at height %d not found: %w", height, errors.ErrItemNotFound)
			}
			orderedBlocks[i] = block
		}

		return orderedBlocks, nil
	})
}

func (b *blockStorageImpl) validateHeight(height uint64) error {
	if height < b.blockStartHeight {
		return xerrors.Errorf("height(%d) should be no less than blockStartHeight(%d): %w",
			height, b.blockStartHeight, errors.ErrInvalidHeight)
	}
	return nil
}
