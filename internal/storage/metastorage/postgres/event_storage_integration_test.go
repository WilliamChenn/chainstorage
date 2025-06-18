package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type eventStorageTestSuite struct {
	suite.Suite
	accessor internal.MetaStorage
	config   *config.Config
	tag      uint32
	eventTag uint32
	db       *sql.DB
}

func (s *eventStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())
	var accessor internal.MetaStorage
	cfg, err := config.New()
	require.NoError(err)

	app := testapp.New(
		s.T(),
		fx.Provide(NewMetaStorage),
		testapp.WithIntegration(),
		testapp.WithConfig(s.config),
		fx.Populate(&accessor),
	)
	defer app.Close()
	s.accessor = accessor
	s.tag = 1
	s.eventTag = 0

	// Get database connection for cleanup
	db, err := newDBConnection(context.Background(), &cfg.AWS.Postgres)
	require.NoError(err)
	s.db = db
}

func (s *eventStorageTestSuite) TearDownTest() {
	if s.db != nil {
		ctx := context.Background()
		s.T().Log("Clearing database tables after test")
		// Clear all tables in reverse order due to foreign key constraints
		tables := []string{"transactions", "block_events", "canonical_blocks", "block_metadata"}
		for _, table := range tables {
			_, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", table))
			if err != nil {
				s.T().Logf("Failed to clear table %s: %v", table, err)
			}
		}
	}
}

func (s *eventStorageTestSuite) TearDownSuite() {
	if s.db != nil {
		s.db.Close()
	}
}
func (s *eventStorageTestSuite) addEvents(eventTag uint32, startHeight uint64, numEvents uint64, tag uint32) {
	// First, add block metadata for the events
	blockMetas := testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(numEvents), tag)
	ctx := context.TODO()
	err := s.accessor.PersistBlockMetas(ctx, true, blockMetas, nil)
	if err != nil {
		panic(err)
	}

	// Then add events
	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, startHeight, startHeight+numEvents, tag)
	err = s.accessor.AddEvents(ctx, eventTag, blockEvents)
	if err != nil {
		panic(err)
	}
}
func (s *eventStorageTestSuite) verifyEvents(eventTag uint32, numEvents uint64, tag uint32) {
	require := testutil.Require(s.T())
	ctx := context.TODO()

	watermark, err := s.accessor.GetMaxEventId(ctx, eventTag)
	if err != nil {
		panic(err)
	}
	require.Equal(watermark-model.EventIdStartValue, int64(numEvents-1))

	// fetch range with missing item
	_, err = s.accessor.GetEventsByEventIdRange(ctx, eventTag, model.EventIdStartValue, model.EventIdStartValue+int64(numEvents+100))
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))

	// fetch valid range
	fetchedEvents, err := s.accessor.GetEventsByEventIdRange(ctx, eventTag, model.EventIdStartValue, model.EventIdStartValue+int64(numEvents))
	if err != nil {
		panic(err)
	}
	require.NotNil(fetchedEvents)
	require.Equal(uint64(len(fetchedEvents)), numEvents)

	numFollowingEventsToFetch := uint64(10)
	for i, event := range fetchedEvents {
		require.Equal(int64(i)+model.EventIdStartValue, event.EventId)
		require.Equal(uint64(i), event.BlockHeight)
		require.Equal(api.BlockchainEvent_BLOCK_ADDED, event.EventType)
		require.Equal(tag, event.Tag)
		require.Equal(eventTag, event.EventTag)

		expectedNumEvents := numFollowingEventsToFetch
		if uint64(event.EventId)+numFollowingEventsToFetch >= numEvents {
			expectedNumEvents = numEvents - 1 - uint64(event.EventId-model.EventIdStartValue)
		}
		followingEvents, err := s.accessor.GetEventsAfterEventId(ctx, eventTag, event.EventId, numFollowingEventsToFetch)
		if err != nil {
			panic(err)
		}
		require.Equal(uint64(len(followingEvents)), expectedNumEvents)
		for j, followingEvent := range followingEvents {
			require.Equal(int64(i+j+1)+model.EventIdStartValue, followingEvent.EventId)
			require.Equal(uint64(i+j+1), followingEvent.BlockHeight)
			require.Equal(api.BlockchainEvent_BLOCK_ADDED, followingEvent.EventType)
			require.Equal(eventTag, followingEvent.EventTag)
		}
	}
}

func (s *eventStorageTestSuite) TestSetMaxEventId() {
	require := testutil.Require(s.T())
	ctx := context.TODO()
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, s.tag)
	watermark, err := s.accessor.GetMaxEventId(ctx, s.eventTag)
	require.NoError(err)
	require.Equal(model.EventIdStartValue+int64(numEvents-1), watermark)

	// reset it to a new value
	newEventId := int64(5)
	err = s.accessor.SetMaxEventId(ctx, s.eventTag, newEventId)
	require.NoError(err)
	watermark, err = s.accessor.GetMaxEventId(ctx, s.eventTag)
	require.NoError(err)
	require.Equal(watermark, newEventId)

	// reset it to invalid value
	invalidEventId := int64(-1)
	err = s.accessor.SetMaxEventId(ctx, s.eventTag, invalidEventId)
	require.Error(err)

	// reset it to value bigger than current max
	invalidEventId = newEventId + 10
	err = s.accessor.SetMaxEventId(ctx, s.eventTag, invalidEventId)
	require.Error(err)

	// reset it to EventIdDeleted
	err = s.accessor.SetMaxEventId(ctx, s.eventTag, model.EventIdDeleted)
	require.NoError(err)
	_, err = s.accessor.GetMaxEventId(ctx, s.eventTag)
	require.Error(err)
	require.Equal(errors.ErrNoEventHistory, err)
}

func (s *eventStorageTestSuite) TestSetMaxEventIdNonDefaultEventTag() {
	require := testutil.Require(s.T())
	ctx := context.TODO()
	numEvents := uint64(100)
	eventTag := uint32(1)
	s.addEvents(eventTag, 0, numEvents, s.tag)
	watermark, err := s.accessor.GetMaxEventId(ctx, eventTag)
	require.NoError(err)
	require.Equal(model.EventIdStartValue+int64(numEvents-1), watermark)

	// reset it to a new value
	newEventId := int64(5)
	err = s.accessor.SetMaxEventId(ctx, eventTag, newEventId)
	require.NoError(err)
	watermark, err = s.accessor.GetMaxEventId(ctx, eventTag)
	require.NoError(err)
	require.Equal(watermark, newEventId)

	// reset it to invalid value
	invalidEventId := int64(-1)
	err = s.accessor.SetMaxEventId(ctx, eventTag, invalidEventId)
	require.Error(err)

	// reset it to value bigger than current max
	invalidEventId = newEventId + 10
	err = s.accessor.SetMaxEventId(ctx, eventTag, invalidEventId)
	require.Error(err)

	// reset it to EventIdDeleted
	err = s.accessor.SetMaxEventId(ctx, eventTag, model.EventIdDeleted)
	require.NoError(err)
	_, err = s.accessor.GetMaxEventId(ctx, eventTag)
	require.Error(err)
	require.Equal(errors.ErrNoEventHistory, err)
}

////////////////////////////////////////////////////////////

func (s *eventStorageTestSuite) TestAddEvents() {
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, s.tag)
	s.verifyEvents(s.eventTag, numEvents, s.tag)
}

func (s *eventStorageTestSuite) TestAddEventsNonDefaultEventTag() {
	numEvents := uint64(100)
	s.addEvents(uint32(1), 0, numEvents, s.tag)
	s.verifyEvents(uint32(1), numEvents, s.tag)
}

func (s *eventStorageTestSuite) TestAddEventsDefaultTag() {
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, 0)
	s.verifyEvents(s.eventTag, numEvents, model.DefaultBlockTag)
}

func (s *eventStorageTestSuite) TestAddEventsNonDefaultTag() {
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, 2)
	s.verifyEvents(s.eventTag, numEvents, 2)
}

func (s *eventStorageTestSuite) TestAddEventsMultipleTimes() {
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, s.tag)
	s.addEvents(s.eventTag, numEvents, numEvents, s.tag)
	numEvents = numEvents * 2
	s.verifyEvents(s.eventTag, numEvents, s.tag)
}

func (s *eventStorageTestSuite) TestAddEventsMultipleTimesNonDefaultEventTag() {
	numEvents := uint64(100)
	eventTag := uint32(1)
	s.addEvents(eventTag, 0, numEvents, s.tag)
	s.addEvents(eventTag, numEvents, numEvents, s.tag)
	numEvents = numEvents * 2
	s.verifyEvents(eventTag, numEvents, s.tag)
}

////////////////////////////////////////////////////////////

func (s *eventStorageTestSuite) TestAddEventsDiscontinuousChain_NotSkipped() {
	require := testutil.Require(s.T())
	numEvents := uint64(100)
	
	// First, add block metadata for the initial events
	blockMetas := testutil.MakeBlockMetadatasFromStartHeight(0, int(numEvents), s.tag)
	ctx := context.TODO()
	err := s.accessor.PersistBlockMetas(ctx, true, blockMetas, nil)
	require.NoError(err)

	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, 0, numEvents, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	if err != nil {
		panic(err)
	}

	// Add block metadata for the additional events that will be tested
	additionalBlockMetas := testutil.MakeBlockMetadatasFromStartHeight(numEvents, 10, s.tag)
	err = s.accessor.PersistBlockMetas(ctx, true, additionalBlockMetas, nil)
	require.NoError(err)

	// have add event for height numEvents-1 again, invalid
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents-1, numEvents+4, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.Error(err)

	// missing event for height numEvents, invalid
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+2, numEvents+7, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.Error(err)

	// hash mismatch, invalid
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+2, numEvents+7, s.tag, testutil.WithBlockHashFormat("HashMismatch0x%s"))
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.Error(err)

	// continuous, should be able to add them
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents, numEvents+7, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
}

func TestIntegrationEventStorageTestSuite(t *testing.T) {
	require := testutil.Require(t)
	// Test with eth-mainnet for stream version
	cfg, err := config.New()
	require.NoError(err)
	suite.Run(t, &eventStorageTestSuite{config: cfg})
}
