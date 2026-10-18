package client

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// Sentinel errors, one per server ErrorCode (SPEC §7.3, DECISIONS D13). A ServerError matches the
// sentinel for its code with errors.Is.
var (
	ErrUnknownTopicOrPartition  = errors.New("unknown topic or partition")
	ErrTopicAlreadyExists       = errors.New("topic already exists")
	ErrInvalidReplicationFactor = errors.New("invalid replication factor")
	ErrNotLeader                = errors.New("not leader")
	ErrFencedLeaderEpoch        = errors.New("fenced leader epoch")
	ErrOffsetOutOfRange         = errors.New("offset out of range")
	ErrNotEnoughReplicas        = errors.New("not enough replicas")
	ErrRequestTimedOut          = errors.New("request timed out")
	ErrCorruptMessage           = errors.New("corrupt message")
	ErrMessageTooLarge          = errors.New("message too large")
	ErrOutOfOrderSequence       = errors.New("out of order sequence")
	ErrUnknownProducerID        = errors.New("unknown producer id")
	ErrIllegalGeneration        = errors.New("illegal generation")
	ErrRebalanceInProgress      = errors.New("rebalance in progress")
	ErrUnknownMemberID          = errors.New("unknown member id")
	ErrNotCoordinator           = errors.New("not coordinator")
	ErrNotController            = errors.New("not controller")
	ErrInvalidConfig            = errors.New("invalid config")
	ErrInvalidTopic             = errors.New("invalid topic")
	ErrInvalidTimestamp         = errors.New("invalid timestamp")
	ErrUnknownServerError       = errors.New("unknown server error")

	// ErrClosed reports use of a closed Client, Producer or Consumer.
	ErrClosed = errors.New("client: closed")
)

// codeErrors maps codes to sentinels. It is built per call (no package-level mutable state).
func codeError(code streamqv1.ErrorCode) error {
	switch code {
	case streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION:
		return ErrUnknownTopicOrPartition
	case streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS:
		return ErrTopicAlreadyExists
	case streamqv1.ErrorCode_ERROR_CODE_INVALID_REPLICATION_FACTOR:
		return ErrInvalidReplicationFactor
	case streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER:
		return ErrNotLeader
	case streamqv1.ErrorCode_ERROR_CODE_FENCED_LEADER_EPOCH:
		return ErrFencedLeaderEpoch
	case streamqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_RANGE:
		return ErrOffsetOutOfRange
	case streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS:
		return ErrNotEnoughReplicas
	case streamqv1.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT:
		return ErrRequestTimedOut
	case streamqv1.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE:
		return ErrCorruptMessage
	case streamqv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE:
		return ErrMessageTooLarge
	case streamqv1.ErrorCode_ERROR_CODE_OUT_OF_ORDER_SEQUENCE:
		return ErrOutOfOrderSequence
	case streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_PRODUCER_ID:
		return ErrUnknownProducerID
	case streamqv1.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION:
		return ErrIllegalGeneration
	case streamqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS:
		return ErrRebalanceInProgress
	case streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_MEMBER_ID:
		return ErrUnknownMemberID
	case streamqv1.ErrorCode_ERROR_CODE_NOT_COORDINATOR:
		return ErrNotCoordinator
	case streamqv1.ErrorCode_ERROR_CODE_NOT_CONTROLLER:
		return ErrNotController
	case streamqv1.ErrorCode_ERROR_CODE_INVALID_CONFIG:
		return ErrInvalidConfig
	case streamqv1.ErrorCode_ERROR_CODE_INVALID_TOPIC:
		return ErrInvalidTopic
	case streamqv1.ErrorCode_ERROR_CODE_INVALID_TIMESTAMP:
		return ErrInvalidTimestamp
	default:
		return ErrUnknownServerError
	}
}

// ServerError is a non-NONE ErrorCode returned by a broker.
type ServerError struct {
	Code    streamqv1.ErrorCode
	Message string
}

// Error implements error.
func (e *ServerError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("streamq: %s", e.Code)
	}
	return fmt.Sprintf("streamq: %s: %s", e.Code, e.Message)
}

// Is matches the sentinel for e.Code.
func (e *ServerError) Is(target error) bool { return codeError(e.Code) == target }

// serverError returns nil for ERROR_CODE_NONE, else a *ServerError.
func serverError(code streamqv1.ErrorCode, msg string) error {
	if code == streamqv1.ErrorCode_ERROR_CODE_NONE {
		return nil
	}
	return &ServerError{Code: code, Message: msg}
}

// IsRetriable reports whether an operation that failed with err may succeed if retried
// (after refreshing metadata where NeedsMetadataRefresh says so).
func IsRetriable(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrNotLeader), errors.Is(err, ErrFencedLeaderEpoch),
		errors.Is(err, ErrUnknownTopicOrPartition), errors.Is(err, ErrNotEnoughReplicas),
		errors.Is(err, ErrRequestTimedOut), errors.Is(err, ErrNotController):
		return true
	}
	return isTransportError(err)
}

// NeedsMetadataRefresh reports whether err suggests the cached leadership is stale.
func NeedsMetadataRefresh(err error) bool {
	return errors.Is(err, ErrNotLeader) || errors.Is(err, ErrFencedLeaderEpoch) ||
		errors.Is(err, ErrUnknownTopicOrPartition) || errors.Is(err, ErrNotController) || isTransportError(err)
}

// isTransportError reports a gRPC failure that is not a server ErrorCode and may be transient.
func isTransportError(err error) bool {
	var se *ServerError
	if errors.As(err, &se) {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
}
