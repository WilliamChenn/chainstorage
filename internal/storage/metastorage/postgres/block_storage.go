package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres/model"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
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
	//TODO: watermark update
	return b.instrumentPersistBlockMetas.Instrument(ctx, func(ctx context.Context) error {
		if len(blocks) == 0 {
			return nil
		} else {
			if len(blocks) == 0 {
				return nil
			}
			sort.Slice(blocks, func(i, j int) bool { return blocks[i].Height < blocks[j].Height }) //sort blocks by height
			if err := parser.ValidateChain(blocks, lastBlock); err != nil {                        //validate chain
				return xerrors.Errorf("failed to validate chain: %w", err)
			}
			transaction, err := b.db.BeginTx(ctx, nil) //start transaction
			if err != nil {
				return xerrors.Errorf("transaction failed to start: %w", err)
			}
			committed := false
			defer func() {
				if !committed {
					transaction.Rollback()
				}
			}()

			query := `INSERT INTO blocks (tag, hash, parent_hash, height, object_key_main, parent_height, timestamp, skipped, is_canonical) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (height, hash) DO UPDATE SET
			tag = EXCLUDED.tag,
			parent_hash = EXCLUDED.parent_hash,
			object_key_main = EXCLUDED.object_key_main,
			parent_height = EXCLUDED.parent_height,
			timestamp = EXCLUDED.timestamp,
			skipped = EXCLUDED.skipped,
			is_canonical = EXCLUDED.is_canonical`

			statement, err := transaction.PrepareContext(ctx, query)
			if err != nil {
				return xerrors.Errorf("failed to prepare statement: %w", err)
			}
			defer statement.Close()

			for _, block := range blocks {
				_, err := statement.ExecContext(ctx,
					block.Tag,
					block.Hash,
					block.ParentHash,
					block.Height,
					block.ObjectKeyMain,
					block.ParentHeight,
					block.GetTimestamp().GetSeconds(),
					block.Skipped,
					true)
				if err != nil {
					return xerrors.Errorf("failed to execute statement: %w", err)
				}
			}
			err = transaction.Commit()
			if err != nil {
				return xerrors.Errorf("failed to commit transaction: %w", err)
			}
			committed = true
			fmt.Printf("DEBUG: Successfully committed %d blocks to database\n", len(blocks))
			return nil
		}
	})
}

func (b *blockStorageImpl) GetLatestBlock(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
	return b.instrumentGetLatestBlock.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		query := "SELECT height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped, is_canonical FROM blocks WHERE tag = $1 ORDER BY height DESC LIMIT 1"
		row := b.db.QueryRowContext(ctx, query, tag)
		block, err := model.BlockMetadataFromRow(row)
		if err != nil {
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
		query := "SELECT height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped, is_canonical FROM blocks WHERE tag = $1 AND height = $2 AND hash = $3"
		row := b.db.QueryRowContext(ctx, query, tag, height, blockHash)
		block, err := model.BlockMetadataFromRow(row)
		if err != nil {
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
		query := "SELECT height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped, is_canonical FROM blocks WHERE tag = $1 AND height = $2"
		row := b.db.QueryRowContext(ctx, query, tag, height)
		block, err := model.BlockMetadataFromRow(row)
		if err != nil {
			return nil, xerrors.Errorf("failed to get block by height: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*api.BlockMetadata, error) {
	return b.instrumentGetBlocksByHeightRange.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		if err := b.validateHeight(startHeight); err != nil {
			return nil, err
		}
		query := "SELECT height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped, is_canonical FROM blocks WHERE tag = $1 AND height >= $2 AND height < $3 ORDER BY height"
		rows, err := b.db.QueryContext(ctx, query, tag, startHeight, endHeight)
		if err != nil {
			return nil, xerrors.Errorf("failed to query blocks by height range: %w", err)
		}
		defer rows.Close()

		blocks, err := model.BlockMetadataFromRows(rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan block row: %w", err)
		}
		return blocks, nil
	})
}

func (b *blockStorageImpl) GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*api.BlockMetadata, error) {
	return b.instrumentGetBlocksByHeights.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		for _, height := range heights {
			if err := b.validateHeight(height); err != nil {
				return nil, err
			}
		}

		query := "SELECT height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped, is_canonical FROM blocks WHERE tag = $1 AND height IN ($2)"
		rows, err := b.db.QueryContext(ctx, query, tag, heights)
		if err != nil {
			return nil, xerrors.Errorf("failed to query blocks by heights: %w", err)
		}
		defer rows.Close()
		blocks, err := model.BlockMetadataFromRows(rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan block row: %w", err)
		}
		return blocks, nil
	})
}

func (b *blockStorageImpl) validateHeight (height uint64) error {
	if height < b.blockStartHeight {
		return xerrors.Errorf("height(%d) should be no less than blockStartHeight(%d): %w",
			height, b.blockStartHeight, errors.ErrInvalidHeight)
	}
	return nil
}