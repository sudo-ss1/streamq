// Package local implements a single-node metadata.Store: in-memory state persisted as JSON in
// <data_dir>/metadata.json (SPEC §2.2, M2–M4).
package local

import (
	"context"
	"errors"
	"log/slog"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

// FileName is the metadata file inside the data dir.
const FileName = "metadata.json"

var errUnimplemented = errors.New("local: unimplemented")

// Store is the single-node metadata store. The local broker is the only registered broker and
// the controller.
//
// Propose applies each command to a deep copy of the state, persists the copy atomically (tmp,
// fsync, rename, dir fsync), and only then swaps it in and queues the changes. A persist failure
// returns an error and leaves the state unchanged. One owned notifier goroutine delivers changes
// to subscribers in order; Close stops it.
type Store struct{}

var _ metadata.Store = (*Store)(nil)

// Open loads <dataDir>/metadata.json (or starts empty), registers self as the only broker and
// controller, and starts the notifier goroutine.
func Open(ctx context.Context, dataDir string, self metadata.Broker, logger *slog.Logger) (*Store, error) {
	_, _, _, _ = ctx, dataDir, self, logger
	return nil, errUnimplemented
}

// Snapshot implements metadata.Store.
func (s *Store) Snapshot() metadata.Snapshot { return metadata.Snapshot{} }

// Propose implements metadata.Store.
func (s *Store) Propose(ctx context.Context, cmd *streamqv1.MetadataCommand) error {
	_, _ = ctx, cmd
	return errUnimplemented
}

// Subscribe implements metadata.Store.
func (s *Store) Subscribe(fn func(context.Context, metadata.Change)) (metadata.Snapshot, func()) {
	_ = fn
	return metadata.Snapshot{}, func() {}
}

// Close implements metadata.Store.
func (s *Store) Close() error { return errUnimplemented }
