// Package storage implements the per-partition on-disk log (SPEC §3): segments with sparse
// offset and time indexes, rolling, retention with delayed deletion, checkpoint files and crash
// recovery.
//
// Concurrency: each Log has a single writer (Append, roll, flush bookkeeping, retention and Close
// run under Log.mu). Readers are lock-free: they load an immutable segment slice and each
// segment's atomically published size, and never read past it. Index lookups hold the index's
// read lock only for the binary search, so an index is never unmapped under a reader.
//
// Lock ordering: Manager.mu → Log.mu → {Manager.pendingMu, index mu (offsetIndex.mu /
// timeIndex.mu)}; pendingMu and the index locks are leaves.
// Log.Flush releases Log.mu before calling fsync.
package storage
