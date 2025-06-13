package model

import (
	"database/sql"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func BlockEventFromRow(row *sql.Row) error {
	return nil
}

// ParseEventType converts a string representation of event type to the protobuf enum
func ParseEventType(eventTypeStr string) api.BlockchainEvent_Type {
	switch eventTypeStr {
	case "BLOCK_ADDED":
		return api.BlockchainEvent_BLOCK_ADDED
	case "BLOCK_REMOVED":
		return api.BlockchainEvent_BLOCK_REMOVED
	default:
		return api.BlockchainEvent_UNKNOWN
	}
}

// EventTypeToString converts the protobuf enum to string representation
func EventTypeToString(eventType api.BlockchainEvent_Type) string {
	switch eventType {
	case api.BlockchainEvent_BLOCK_ADDED:
		return "BLOCK_ADDED"
	case api.BlockchainEvent_BLOCK_REMOVED:
		return "BLOCK_REMOVED"
	default:
		return "UNKNOWN"
	}
}
