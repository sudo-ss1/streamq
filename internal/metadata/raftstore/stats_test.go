package raftstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRaftStats(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()
	follower := c.follower()

	tests := []struct {
		name  string
		store *Store
		state string
	}{
		{"leader", leader, "leader"},
		{"follower", follower, "follower"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Eventually(t, func() bool {
				st := tc.store.RaftStats()
				return st.State == tc.state && st.Term > 0 && st.CommitIndex > 0 && st.AppliedIndex > 0
			}, 10*time.Second, 10*time.Millisecond)
			st := tc.store.RaftStats()
			if tc.state == "leader" {
				require.Zero(t, st.LastContact, "last contact is 0 on the leader")
			} else {
				require.Less(t, st.LastContact, 5*time.Second, "follower hears heartbeats")
			}
		})
	}

	var followerID int32
	for _, p := range c.peers {
		if c.store(p.ID) == follower {
			followerID = p.ID
		}
	}
	require.NotZero(t, followerID)
	c.stop(followerID)
	require.Equal(t, "shutdown", follower.RaftStats().State)
}
