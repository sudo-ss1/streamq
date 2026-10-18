package raftstore

import "github.com/sudo-ss1/streamq/internal/metrics"

// RaftStats implements metrics.RaftSource.
func (s *Store) RaftStats() metrics.RaftStats { return metrics.RaftStats{} }
