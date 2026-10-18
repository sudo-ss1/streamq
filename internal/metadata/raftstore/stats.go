package raftstore

import (
	"strings"
	"time"

	"github.com/hashicorp/raft"

	"github.com/sudo-ss1/streamq/internal/metrics"
)

// RaftStats implements metrics.RaftSource. State is the lower-case Raft state (leader, follower,
// candidate or shutdown). LastContact is the time since this node last heard from the leader: 0
// on the leader, and the time since the store opened when it has never heard from one (so a
// node that cannot reach a leader never looks healthy, D52). It is safe to call on a closed
// store (the state is then "shutdown").
func (s *Store) RaftStats() metrics.RaftStats {
	if s == nil || s.raft == nil {
		return metrics.RaftStats{State: "shutdown"}
	}
	r := s.raft
	state := r.State()
	st := metrics.RaftStats{
		State:        strings.ToLower(state.String()),
		Term:         r.CurrentTerm(),
		CommitIndex:  r.CommitIndex(),
		AppliedIndex: r.AppliedIndex(),
	}
	if state != raft.Leader {
		last := r.LastContact()
		if last.IsZero() {
			last = s.opened
		}
		st.LastContact = max(time.Since(last), 0)
	}
	return st
}
