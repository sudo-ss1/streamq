package broker

import (
	"context"
	"fmt"
	"sync"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

// IDBlockSource reserves a block of metadata.ProducerIDBlockSize producer IDs and returns its
// first ID (the controller in cluster mode, the local store in single-node mode).
type IDBlockSource func(ctx context.Context) (int64, error)

// ProducerIDs hands out idempotent producer IDs from blocks reserved through a source (SPEC §6,
// DECISIONS D51). A failed reservation is returned to the caller and retried by the next call;
// IDs of a block whose reservation response was lost are simply never used.
type ProducerIDs struct {
	src IDBlockSource

	mu        sync.Mutex // held across a reservation so concurrent callers share one block
	next, end int64
}

// NewProducerIDs returns an allocator over src.
func NewProducerIDs(src IDBlockSource) *ProducerIDs { return &ProducerIDs{src: src} }

// Next returns an unused producer ID.
func (a *ProducerIDs) Next(ctx context.Context) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.next >= a.end {
		first, err := a.src(ctx)
		if err != nil {
			return 0, fmt.Errorf("reserve producer ids: %w", err)
		}
		a.next, a.end = first, first+metadata.ProducerIDBlockSize
	}
	id := a.next
	a.next++
	return id, nil
}

// LocalIDBlockSource reserves blocks from a single-node metadata store for broker self: it
// proposes the allocation and reads the block start back. The local store applies proposals
// synchronously and only this broker allocates for itself under ProducerIDs.mu, so the value read
// is this proposal's (D51).
func LocalIDBlockSource(store metadata.Store, self int32) IDBlockSource {
	return func(ctx context.Context) (int64, error) {
		cmd := allocateProducerIDsCmd(self)
		if err := store.Propose(ctx, cmd); err != nil {
			return 0, err
		}
		first, ok := store.Snapshot().ProducerIDBlocks[self]
		if !ok {
			return 0, fmt.Errorf("producer id block for broker %d missing after allocation", self)
		}
		return first, nil
	}
}

func allocateProducerIDsCmd(broker int32) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_AllocateProducerIds{
		AllocateProducerIds: &streamqv1.AllocateProducerIDsCommand{BrokerId: broker},
	}}
}
