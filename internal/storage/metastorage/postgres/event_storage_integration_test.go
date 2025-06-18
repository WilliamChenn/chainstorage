package postgres

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
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

func TestIntegrationEventStorageTestSuite(t *testing.T) {
	require := testutil.Require(t)
	// Test with eth-mainnet for stream version
	cfg, err := config.New()
	require.NoError(err)
	suite.Run(t, &eventStorageTestSuite{config: cfg})
}

