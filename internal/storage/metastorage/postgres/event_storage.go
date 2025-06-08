package postgres

import (
	"context"
	"database/sql"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
)

type (
	eventStorageImpl struct {
		db *sql.DB
	}
)

func newEventStorage(db *sql.DB, params Params) (internal.EventStorage, error) {
	accessor := &eventStorageImpl{
		db: db,
	}
	return accessor, nil
}

func (e *eventStorageImpl) AddEvents(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
	// TODO: Implement event insertion
	return nil
}

func (e *eventStorageImpl) AddEventEntries(ctx context.Context, eventTag uint32, eventEntries []*model.EventEntry) error {
	// TODO: Implement event entry insertion
	return nil
}

func (e *eventStorageImpl) GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*model.EventEntry, error) {
	// TODO: Implement get event by ID
	return nil, nil
}

func (e *eventStorageImpl) GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error) {
	// TODO: Implement get events after ID
	return nil, nil
}

func (e *eventStorageImpl) GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
	// TODO: Implement get events by ID range
	return nil, nil
}

func (e *eventStorageImpl) GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error) {
	// TODO: Implement get max event ID
	return 0, nil
}

func (e *eventStorageImpl) SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error {
	// TODO: Implement set max event ID
	return nil
}

func (e *eventStorageImpl) GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error) {
	// TODO: Implement get first event ID by block height
	return 0, nil
}

func (e *eventStorageImpl) GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error) {
	// TODO: Implement get events by block height
	return nil, nil
}
