package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/broker"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/partition"
	"github.com/sudo-ss1/streamq/internal/storage"
)

func TestErrorCode(t *testing.T) {
	tests := []struct {
		err  error
		want streamqv1.ErrorCode
	}{
		{nil, streamqv1.ErrorCode_ERROR_CODE_NONE},
		{metadata.ErrUnknownTopic, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION},
		{broker.ErrUnknownPartition, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION},
		{storage.ErrClosed, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION},
		{metadata.ErrTopicExists, streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS},
		{metadata.ErrInvalidReplicationFactor, streamqv1.ErrorCode_ERROR_CODE_INVALID_REPLICATION_FACTOR},
		{metadata.ErrInvalidTopic, streamqv1.ErrorCode_ERROR_CODE_INVALID_TOPIC},
		{config.ErrInvalidConfig, streamqv1.ErrorCode_ERROR_CODE_INVALID_CONFIG},
		{partition.ErrNotLeader, streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER},
		{partition.ErrFencedLeaderEpoch, streamqv1.ErrorCode_ERROR_CODE_FENCED_LEADER_EPOCH},
		{partition.ErrNotEnoughReplicas, streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS},
		{storage.ErrOffsetOutOfRange, streamqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_RANGE},
		{storage.ErrMessageTooLarge, streamqv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE},
		{storage.ErrCorruptBatch, streamqv1.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE},
		{storage.ErrInvalidTimestamp, streamqv1.ErrorCode_ERROR_CODE_INVALID_TIMESTAMP},
		{metadata.ErrNotController, streamqv1.ErrorCode_ERROR_CODE_NOT_CONTROLLER},
		{context.DeadlineExceeded, streamqv1.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT},
		{storage.ErrLogFailed, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN},
		{context.Canceled, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN},
		{errors.New("boom"), streamqv1.ErrorCode_ERROR_CODE_UNKNOWN},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.err), func(t *testing.T) {
			require.Equal(t, tt.want, errorCode(tt.err))
			if tt.err != nil {
				require.Equal(t, tt.want, errorCode(fmt.Errorf("wrapped: %w", tt.err)))
			}
		})
	}
}
