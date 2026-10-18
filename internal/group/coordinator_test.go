package group

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

const grp = "g1"

func TestSingleMemberGetsAllPartitions(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 4)

	p := e.startJoin(JoinRequest{GroupID: grp, Topics: []string{"t"}})
	e.waitParked(grp, 1)
	require.Equal(t, StatePreparingRebalance, e.state(grp))
	e.clock.Advance(testDelay - time.Millisecond)
	require.False(t, p.done(), "completed before the initial delay")
	e.clock.Advance(time.Millisecond)

	res := p.result(t)
	require.Equal(t, int32(1), res.GenerationID)
	require.True(t, strings.HasPrefix(res.MemberID, "member-"))
	require.Equal(t, RangeAssignorName, res.Assignor)
	require.Equal(t, Assignment{"t": {0, 1, 2, 3}}, res.Assignment)

	asg, err := e.c.Sync(context.Background(), grp, res.MemberID, 1)
	require.NoError(t, err)
	require.Equal(t, res.Assignment, asg)
	require.NoError(t, e.c.Heartbeat(context.Background(), grp, res.MemberID, 1))
	require.Equal(t, StateStable, e.state(grp))
}

func TestThreeMembersOneGenerationDisjointCover(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 6)

	res := e.joinAll(grp, sub("t"), sub("t"), sub("t"))
	require.Equal(t, int32(1), res[0].GenerationID)
	requireCover(t, res, map[string]int{"t": 6})
	for _, r := range res {
		require.Len(t, r.Assignment["t"], 2)
	}
	d, err := e.c.Describe(context.Background(), grp)
	require.NoError(t, err)
	require.Equal(t, StateStable, d.State)
	require.Equal(t, int32(1), d.Generation)
	require.Len(t, d.Members, 3)
}

func TestFourthMemberTriggersRebalance(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 6)
	res := e.joinAll(grp, sub("t"), sub("t"), sub("t"))

	p4 := e.startJoin(JoinRequest{GroupID: grp, Topics: []string{"t"}, SessionTimeout: testSession})
	e.waitParked(grp, 1)
	require.Equal(t, StatePreparingRebalance, e.state(grp))
	for _, r := range res {
		require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, r.MemberID, 1), ErrRebalanceInProgress)
		_, err := e.c.Sync(context.Background(), grp, r.MemberID, 1)
		require.ErrorIs(t, err, ErrRebalanceInProgress)
	}
	// Existing members rejoin; the last rejoin completes the rebalance without any delay.
	var ps []*pendingJoin
	for i, r := range res {
		ps = append(ps, e.startJoin(JoinRequest{GroupID: grp, MemberID: r.MemberID, Topics: []string{"t"}, SessionTimeout: testSession}))
		if i < len(res)-1 {
			e.waitParked(grp, i+2)
		}
	}
	all := []JoinResult{p4.result(t)}
	for _, p := range ps {
		all = append(all, p.result(t))
	}
	require.Equal(t, int32(2), all[0].GenerationID)
	requireCover(t, all, map[string]int{"t": 6})
	for _, r := range res {
		require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, r.MemberID, 1), ErrIllegalGeneration)
		require.NoError(t, e.c.Heartbeat(context.Background(), grp, r.MemberID, 2))
	}
}

func TestMemberNotRejoiningIsRemovedAtRebalanceTimeout(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 4)
	res := e.joinAll(grp,
		JoinRequest{Topics: []string{"t"}, SessionTimeout: testSession, RebalanceTimeout: 3 * time.Second},
		JoinRequest{Topics: []string{"t"}, SessionTimeout: testSession, RebalanceTimeout: 3 * time.Second})
	a, b := res[0], res[1]

	// a rejoins with a changed subscription, b keeps heartbeating but never rejoins.
	pa := e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t", "u"}, SessionTimeout: testSession, RebalanceTimeout: 3 * time.Second})
	e.waitParked(grp, 1)
	for range 5 {
		e.clock.Advance(500 * time.Millisecond)
		require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, b.MemberID, 1), ErrRebalanceInProgress)
	}
	require.False(t, pa.done())
	e.clock.Advance(500 * time.Millisecond) // 3s: rebalance timeout

	r := pa.result(t)
	require.Equal(t, int32(2), r.GenerationID)
	require.Equal(t, Assignment{"t": {0, 1, 2, 3}}, r.Assignment)
	require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, b.MemberID, 1), ErrUnknownMemberID)
}

func TestSessionExpiryTriggersRebalance(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 4)
	res := e.joinAll(grp, sub("t"), sub("t"))
	a, b := res[0], res[1]

	e.clock.Advance(testSession / 2)
	require.NoError(t, e.c.Heartbeat(context.Background(), grp, a.MemberID, 1))
	e.clock.Advance(testSession / 2) // b expires; a's deadline was pushed out
	require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, b.MemberID, 1), ErrUnknownMemberID)
	require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, a.MemberID, 1), ErrRebalanceInProgress)

	pa := e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t"}, SessionTimeout: testSession})
	r := pa.result(t)
	require.Equal(t, int32(2), r.GenerationID)
	require.Equal(t, Assignment{"t": {0, 1, 2, 3}}, r.Assignment)
}

func TestLeave(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	res := e.joinAll(grp, sub("t"), sub("t"))

	require.NoError(t, e.c.Leave(context.Background(), grp, res[1].MemberID))
	require.ErrorIs(t, e.c.Leave(context.Background(), grp, res[1].MemberID), ErrUnknownMemberID)
	require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, res[0].MemberID, 1), ErrRebalanceInProgress)
	r := e.startJoin(JoinRequest{GroupID: grp, MemberID: res[0].MemberID, Topics: []string{"t"}, SessionTimeout: testSession}).result(t)
	require.Equal(t, int32(2), r.GenerationID)
	require.Equal(t, Assignment{"t": {0, 1}}, r.Assignment)

	require.NoError(t, e.c.Leave(context.Background(), grp, r.MemberID))
	require.Equal(t, StateEmpty, e.state(grp))
	require.ErrorIs(t, e.c.Leave(context.Background(), "nope", r.MemberID), ErrUnknownMemberID)
}

func TestErrorCodes(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	m := e.joinAll(grp, sub("t"))[0]
	ctx := context.Background()
	offs := map[TopicPartition]int64{{"t", 0}: 5}

	tests := []struct {
		name string
		call func() error
		want error
	}{
		{"heartbeat stale generation", func() error { return e.c.Heartbeat(context.Background(), grp, m.MemberID, 0) }, ErrIllegalGeneration},
		{"heartbeat future generation", func() error { return e.c.Heartbeat(context.Background(), grp, m.MemberID, 2) }, ErrIllegalGeneration},
		{"heartbeat unknown member", func() error { return e.c.Heartbeat(context.Background(), grp, "member-x", 1) }, ErrUnknownMemberID},
		{"heartbeat unknown group", func() error { return e.c.Heartbeat(context.Background(), "other", m.MemberID, 1) }, ErrUnknownMemberID},
		{"sync stale generation", func() error { _, err := e.c.Sync(context.Background(), grp, m.MemberID, 0); return err }, ErrIllegalGeneration},
		{"sync unknown member", func() error { _, err := e.c.Sync(context.Background(), grp, "member-x", 1); return err }, ErrUnknownMemberID},
		{"commit stale generation", func() error { _, err := e.c.Commit(ctx, grp, m.MemberID, 0, offs); return err }, ErrIllegalGeneration},
		{"commit unknown member", func() error { _, err := e.c.Commit(ctx, grp, "member-x", 1, offs); return err }, ErrUnknownMemberID},
		{"standalone commit to group with members", func() error { _, err := e.c.Commit(ctx, grp, "", -1, offs); return err }, ErrUnknownMemberID},
		{"commit empty group id", func() error { _, err := e.c.Commit(ctx, "", "", -1, offs); return err }, ErrInvalidGroup},
		{"join empty group id", func() error { _, err := e.c.Join(ctx, JoinRequest{}); return err }, ErrInvalidGroup},
		{"join unknown assignor", func() error {
			_, err := e.c.Join(ctx, JoinRequest{GroupID: grp, Assignors: []string{"sticky"}})
			return err
		}, ErrInvalidGroup},
		{"join no common assignor", func() error {
			_, err := e.c.Join(ctx, JoinRequest{GroupID: grp, Assignors: []string{RoundRobinAssignorName}})
			return err
		}, ErrInvalidGroup},
		{"join unknown member id", func() error {
			_, err := e.c.Join(ctx, JoinRequest{GroupID: grp, MemberID: "member-x"})
			return err
		}, ErrUnknownMemberID},
		{"join unknown member id in unknown group", func() error {
			_, err := e.c.Join(ctx, JoinRequest{GroupID: "other", MemberID: "member-x"})
			return err
		}, ErrUnknownMemberID},
		{"fetch empty group id", func() error { _, err := e.c.FetchOffsets(context.Background(), "", nil); return err }, ErrInvalidGroup},
		{"describe empty group id", func() error { _, err := e.c.Describe(context.Background(), ""); return err }, ErrInvalidGroup},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, tc.call(), tc.want)
		})
	}
	// None of the failed calls disturbed the group.
	require.Equal(t, StateStable, e.state(grp))
	_, err := e.c.Commit(ctx, grp, m.MemberID, 1, offs)
	require.NoError(t, err)
}

// R1: the current generation commits during PreparingRebalance.
func TestCommitDuringPreparingRebalance(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	res := e.joinAll(grp, sub("t"), sub("t"))
	require.NoError(t, e.c.Leave(context.Background(), grp, res[1].MemberID))
	require.Equal(t, StatePreparingRebalance, e.state(grp))

	perr, err := e.c.Commit(context.Background(), grp, res[0].MemberID, 1, map[TopicPartition]int64{{"t", 0}: 7, {"t", 1}: 9})
	require.NoError(t, err)
	require.Empty(t, perr)
	got, err := e.c.FetchOffsets(context.Background(), grp, nil)
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{{"t", 0}: 7, {"t", 1}: 9}, got)
}

func TestStandaloneCommitAndFetch(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 3)
	ctx := context.Background()

	perr, err := e.c.Commit(ctx, grp, "", -1, map[TopicPartition]int64{{"t", 0}: 3, {"t", 2}: 4})
	require.NoError(t, err)
	require.Empty(t, perr)
	got, err := e.c.FetchOffsets(context.Background(), grp, []TopicPartition{{"t", 0}, {"t", 1}, {"x", 0}})
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{{"t", 0}: 3, {"t", 1}: -1, {"x", 0}: -1}, got)

	// Empty offsets: a no-op.
	perr, err = e.c.Commit(ctx, grp, "", -1, nil)
	require.NoError(t, err)
	require.Empty(t, perr)

	// Gen -1 with a member id, or an empty member id with a real generation, is not standalone.
	_, err = e.c.Commit(ctx, grp, "member-x", -1, map[TopicPartition]int64{{"t", 0}: 1})
	require.ErrorIs(t, err, ErrUnknownMemberID)
	_, err = e.c.Commit(ctx, grp, "", 1, map[TopicPartition]int64{{"t", 0}: 1})
	require.ErrorIs(t, err, ErrUnknownMemberID)

	// A negative offset is a request-level error.
	_, err = e.c.Commit(ctx, grp, "", -1, map[TopicPartition]int64{{"t", 0}: -5})
	require.ErrorIs(t, err, metadata.ErrInvalidCommand)

	// Unknown group: nothing committed.
	got, err = e.c.FetchOffsets(context.Background(), "other", nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

// Committed offsets survive a coordinator restart and a store reopen; group membership does not.
func TestCommittedOffsetsSurviveRestart(t *testing.T) {
	cfg := testConfig()
	e := newEnv(t, cfg)
	createTopic(t, e.store, "t", 2)
	m := e.joinAll(grp, sub("t"))[0]
	_, err := e.c.Commit(context.Background(), grp, m.MemberID, 1, map[TopicPartition]int64{{"t", 0}: 11, {"t", 1}: 12})
	require.NoError(t, err)
	e.c.Close()

	// New coordinator, same store.
	c2 := New(cfg, e.store, e.clock, nil)
	t.Cleanup(c2.Close)
	got, err := c2.FetchOffsets(context.Background(), grp, nil)
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{{"t", 0}: 11, {"t", 1}: 12}, got)
	require.ErrorIs(t, c2.Heartbeat(context.Background(), grp, m.MemberID, 1), ErrUnknownMemberID)
	c2.Close()

	// Reopened store from the same directory.
	require.NoError(t, e.store.Close())
	s2 := openStore(t, e.dir)
	c3 := New(cfg, s2, e.clock, nil)
	t.Cleanup(c3.Close)
	got, err = c3.FetchOffsets(context.Background(), grp, []TopicPartition{{"t", 1}})
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{{"t", 1}: 12}, got)
}

func TestTopicDeletionDropsOffsets(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	createTopic(t, e.store, "u", 1)
	_, err := e.c.Commit(context.Background(), grp, "", -1, map[TopicPartition]int64{{"t", 0}: 1, {"u", 0}: 2})
	require.NoError(t, err)
	deleteTopic(t, e.store, "t")
	got, err := e.c.FetchOffsets(context.Background(), grp, []TopicPartition{{"t", 0}, {"u", 0}})
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{{"t", 0}: -1, {"u", 0}: 2}, got)
}

func TestBeginShutdownFailsParkedAndNewCalls(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	p := e.startJoin(JoinRequest{GroupID: grp, Topics: []string{"t"}})
	e.waitParked(grp, 1)

	e.c.BeginShutdown()
	require.ErrorIs(t, p.wait(t).err, ErrCoordinatorClosed)
	_, err := e.c.Join(context.Background(), JoinRequest{GroupID: grp})
	require.ErrorIs(t, err, ErrCoordinatorClosed)
	require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, "m", 0), ErrCoordinatorClosed)
	_, err = e.c.Sync(context.Background(), grp, "m", 0)
	require.ErrorIs(t, err, ErrCoordinatorClosed)
	require.ErrorIs(t, e.c.Leave(context.Background(), grp, "m"), ErrCoordinatorClosed)
	_, err = e.c.Commit(context.Background(), grp, "", -1, nil)
	require.ErrorIs(t, err, ErrCoordinatorClosed)
	_, err = e.c.FetchOffsets(context.Background(), grp, nil)
	require.ErrorIs(t, err, ErrCoordinatorClosed)
	_, err = e.c.List(context.Background())
	require.ErrorIs(t, err, ErrCoordinatorClosed)
	_, err = e.c.Describe(context.Background(), grp)
	require.ErrorIs(t, err, ErrCoordinatorClosed)

	e.c.Close()
	e.c.Close() // idempotent
	e.clock.Advance(time.Hour)
}

func TestCloseFailsParkedJoins(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	res := e.joinAll(grp, sub("t"), sub("t"))
	p := e.startJoin(JoinRequest{GroupID: grp, MemberID: res[0].MemberID, Topics: []string{"t", "x"}})
	e.waitParked(grp, 1)
	e.c.Close()
	require.ErrorIs(t, p.wait(t).err, ErrCoordinatorClosed)
}

// R2: parked members are kept alive while a rebalance waits longer than their session timeout.
func TestParkedMembersSurviveLongRebalance(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	res := e.joinAll(grp, sub("t"), sub("t"))
	a, b := res[0], res[1]

	pa := e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t", "u"}, SessionTimeout: testSession})
	e.waitParked(grp, 1)
	for range 8 { // 4s, four session timeouts; b keeps its own session alive by heartbeating
		e.clock.Advance(testSession / 2)
		require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, b.MemberID, 1), ErrRebalanceInProgress)
	}
	pb := e.startJoin(JoinRequest{GroupID: grp, MemberID: b.MemberID, Topics: []string{"t"}, SessionTimeout: testSession})
	ra, rb := pa.result(t), pb.result(t)
	require.Equal(t, int32(2), ra.GenerationID)
	requireCover(t, []JoinResult{ra, rb}, map[string]int{"t": 2})

	// Deadlines restart at completion: both are alive just under a session later.
	e.clock.Advance(testSession - time.Millisecond)
	require.NoError(t, e.c.Heartbeat(context.Background(), grp, a.MemberID, 2))
	require.NoError(t, e.c.Heartbeat(context.Background(), grp, b.MemberID, 2))
}

// R3: an abandoned Join removes the member it created.
func TestAbandonedNewMemberJoinIsRemoved(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	a := e.joinAll(grp, sub("t"))[0]

	p := e.startJoin(JoinRequest{GroupID: grp, Topics: []string{"t"}, SessionTimeout: testSession})
	e.waitParked(grp, 1)
	_, members := e.parked(grp)
	require.Equal(t, 2, members)
	p.cancel()
	require.ErrorIs(t, p.wait(t).err, context.Canceled)
	_, members = e.parked(grp)
	require.Equal(t, 1, members)

	r := e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t"}, SessionTimeout: testSession}).result(t)
	require.Equal(t, int32(2), r.GenerationID)
	require.Equal(t, Assignment{"t": {0, 1}}, r.Assignment)
}

// R3: a lone new member that abandons its join during the initial delay leaves an empty group.
func TestAbandonedInitialJoinEmptiesGroup(t *testing.T) {
	e := newEnv(t, testConfig())
	p := e.startJoin(JoinRequest{GroupID: grp, Topics: []string{"t"}})
	e.waitParked(grp, 1)
	p.cancel()
	require.ErrorIs(t, p.wait(t).err, context.Canceled)
	require.Equal(t, StateEmpty, e.state(grp))
	ids, err := e.c.List(context.Background())
	require.NoError(t, err)
	require.Empty(t, ids)
	e.clock.Advance(time.Hour) // stale timers are harmless
}

// R3/M5: an existing member's abandoned rejoin un-parks it; its session restarts from then.
func TestAbandonedExistingMemberJoinUnparks(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	res := e.joinAll(grp, sub("t"), sub("t"))
	a, b := res[0], res[1]

	pa := e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t", "u"}, SessionTimeout: testSession})
	e.waitParked(grp, 1)
	e.clock.Advance(3 * testSession / 4)
	require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, b.MemberID, 1), ErrRebalanceInProgress)
	pa.cancel()
	require.ErrorIs(t, pa.wait(t).err, context.Canceled)
	_, members := e.parked(grp)
	require.Equal(t, 2, members)

	e.clock.Advance(testSession - time.Millisecond)
	require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, b.MemberID, 1), ErrRebalanceInProgress)
	e.clock.Advance(time.Millisecond) // a's deadline: cancel time + session
	_, members = e.parked(grp)
	require.Equal(t, 1, members)
	require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, a.MemberID, 1), ErrUnknownMemberID)
}

// R6: a topic created after the group stabilised is assigned through a rebalance.
func TestTopicCreatedAfterStableIsAssigned(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	a := e.joinAll(grp, sub("t", "u"))[0]
	require.Equal(t, Assignment{"t": {0, 1}}, a.Assignment)

	createTopic(t, e.store, "other", 1) // not subscribed: no rebalance
	createTopic(t, e.store, "u", 3)
	require.Eventually(t, func() bool {
		return errors.Is(e.c.Heartbeat(context.Background(), grp, a.MemberID, 1), ErrRebalanceInProgress)
	}, 5*time.Second, time.Millisecond)
	r := e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t", "u"}, SessionTimeout: testSession}).result(t)
	require.Equal(t, int32(2), r.GenerationID)
	require.Equal(t, Assignment{"t": {0, 1}, "u": {0, 1, 2}}, r.Assignment)

	deleteTopic(t, e.store, "u")
	require.Eventually(t, func() bool {
		return errors.Is(e.c.Heartbeat(context.Background(), grp, a.MemberID, 2), ErrRebalanceInProgress)
	}, 5*time.Second, time.Millisecond)
	r = e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t", "u"}, SessionTimeout: testSession}).result(t)
	require.Equal(t, Assignment{"t": {0, 1}}, r.Assignment)
}

// R6: a commit naming a deleted topic still persists the other partitions.
func TestCommitWithDeletedTopicPersistsOthers(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	createTopic(t, e.store, "u", 2)
	m := e.joinAll(grp, sub("t"))[0] // only subscribed to t, so no rebalance on deleting u
	deleteTopic(t, e.store, "u")

	perr, err := e.c.Commit(context.Background(), grp, m.MemberID, 1, map[TopicPartition]int64{
		{"t", 0}: 5, {"u", 0}: 6, {"t", 9}: 7, {"t", -1}: 8,
	})
	require.NoError(t, err)
	require.Len(t, perr, 3)
	for _, tp := range []TopicPartition{{"u", 0}, {"t", 9}, {"t", -1}} {
		require.ErrorIs(t, perr[tp], metadata.ErrUnknownTopic, tp)
	}
	got, err := e.c.FetchOffsets(context.Background(), grp, nil)
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{{"t", 0}: 5}, got)

	// All partitions unknown: nothing to propose, still per-partition errors only.
	perr, err = e.c.Commit(context.Background(), grp, m.MemberID, 1, map[TopicPartition]int64{{"u", 1}: 1})
	require.NoError(t, err)
	require.ErrorIs(t, perr[TopicPartition{"u", 1}], metadata.ErrUnknownTopic)
}

// M4: the filter + propose is retried once when a topic disappears in between.
func TestCommitRetriesOnUnknownTopic(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 1)
	createTopic(t, e.store, "u", 1)
	fs := &flakyStore{Store: e.store}
	fs.beforePropose = func(n int) {
		if n == 1 {
			deleteTopic(t, e.store, "u") // the topic vanishes after the first filter
		}
	}
	c := New(testConfig(), fs, e.clock, nil)
	t.Cleanup(c.Close)

	perr, err := c.Commit(context.Background(), grp, "", -1, map[TopicPartition]int64{{"t", 0}: 1, {"u", 0}: 2})
	require.NoError(t, err)
	require.ErrorIs(t, perr[TopicPartition{"u", 0}], metadata.ErrUnknownTopic)
	require.Equal(t, 2, fs.proposals())
	got, err := c.FetchOffsets(context.Background(), grp, nil)
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{{"t", 0}: 1}, got)
}

// R7: a member that is not parked still expires during PreparingRebalance, and its removal
// completes the rebalance.
func TestNonParkedMemberExpiresDuringRebalance(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	res := e.joinAll(grp, sub("t"), sub("t"))
	a, b := res[0], res[1]

	pa := e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t", "u"}, SessionTimeout: testSession})
	e.waitParked(grp, 1)
	e.clock.Advance(testSession - time.Millisecond)
	require.False(t, pa.done())
	e.clock.Advance(time.Millisecond) // b's session expires (well before the 5s rebalance timeout)
	r := pa.result(t)
	require.Equal(t, int32(2), r.GenerationID)
	require.Equal(t, Assignment{"t": {0, 1}}, r.Assignment)
	require.ErrorIs(t, e.c.Heartbeat(context.Background(), grp, b.MemberID, 1), ErrUnknownMemberID)
}

// M6: the initial delay restarts per new joiner, but PreparingRebalance is capped at the
// rebalance timeout.
func TestInitialDelayCappedByRebalanceTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.InitialRebalanceDelay = 400 * time.Millisecond
	e := newEnv(t, cfg)
	createTopic(t, e.store, "t", 8)
	req := JoinRequest{GroupID: grp, Topics: []string{"t"}, SessionTimeout: testSession, RebalanceTimeout: time.Second}

	var ps []*pendingJoin
	for i := range 4 { // joiners at 0, 300, 600, 900ms each restart the 400ms delay
		ps = append(ps, e.startJoin(req))
		e.waitParked(grp, i+1)
		if i < 3 {
			e.clock.Advance(300 * time.Millisecond)
		}
	}
	e.clock.Advance(99 * time.Millisecond)
	require.False(t, ps[0].done(), "completed before the cap")
	e.clock.Advance(time.Millisecond) // t = 1s: the cap, although the last delay runs to 1.3s
	var res []JoinResult
	for _, p := range ps {
		res = append(res, p.result(t))
	}
	require.Equal(t, int32(1), res[0].GenerationID)
	requireCover(t, res, map[string]int{"t": 8})
}

// The initial delay restarts on each new joiner (below the cap).
func TestInitialDelayRestartsPerJoiner(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	p1 := e.startJoin(JoinRequest{GroupID: grp, Topics: []string{"t"}})
	e.waitParked(grp, 1)
	e.clock.Advance(testDelay - time.Millisecond)
	p2 := e.startJoin(JoinRequest{GroupID: grp, Topics: []string{"t"}})
	e.waitParked(grp, 2)
	e.clock.Advance(testDelay - time.Millisecond)
	require.False(t, p1.done())
	e.clock.Advance(time.Millisecond)
	requireCover(t, []JoinResult{p1.result(t), p2.result(t)}, map[string]int{"t": 2})
}

// R16: rejoin shortcut, rebalance on a changed subscription, assignor choice.
func TestRejoinAndAssignorChoice(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 4)
	res := e.joinAll(grp,
		JoinRequest{Topics: []string{"t"}, SessionTimeout: testSession, Assignors: []string{RoundRobinAssignorName, RangeAssignorName}},
		JoinRequest{Topics: []string{"t"}, SessionTimeout: testSession, Assignors: []string{RangeAssignorName, RoundRobinAssignorName}})
	a := res[0]
	// The earliest joiner prefers roundrobin and both support it.
	require.Equal(t, RoundRobinAssignorName, a.Assignor)

	// Same subscription and assignors: current result, no rebalance.
	r, err := e.c.Join(context.Background(), JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t", "t"},
		SessionTimeout: testSession, Assignors: []string{RoundRobinAssignorName, RangeAssignorName}})
	require.NoError(t, err)
	require.Equal(t, a, r)
	require.Equal(t, StateStable, e.state(grp))

	// A third member supporting only range is accepted (range is common) and switches the assignor.
	p := e.startJoin(JoinRequest{GroupID: grp, Topics: []string{"t"}, SessionTimeout: testSession, Assignors: []string{RangeAssignorName}})
	e.waitParked(grp, 1)
	pa := e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t"}, SessionTimeout: testSession, Assignors: []string{RoundRobinAssignorName, RangeAssignorName}})
	e.waitParked(grp, 2)
	pb := e.startJoin(JoinRequest{GroupID: grp, MemberID: res[1].MemberID, Topics: []string{"t"}, SessionTimeout: testSession, Assignors: []string{RangeAssignorName, RoundRobinAssignorName}})
	all := []JoinResult{p.result(t), pa.result(t), pb.result(t)}
	require.Equal(t, RangeAssignorName, all[0].Assignor)
	requireCover(t, all, map[string]int{"t": 4})

	// A member with a changed subscription triggers a rebalance even in Stable.
	pa = e.startJoin(JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t", "v"}, SessionTimeout: testSession, Assignors: []string{RangeAssignorName}})
	e.waitParked(grp, 1)
	require.Equal(t, StatePreparingRebalance, e.state(grp))
	pa.cancel()
	require.ErrorIs(t, pa.wait(t).err, context.Canceled)
}

func TestNoCommonAssignorRejected(t *testing.T) {
	e := newEnv(t, testConfig())
	e.joinAll(grp, JoinRequest{Topics: []string{"t"}, SessionTimeout: testSession, Assignors: []string{RoundRobinAssignorName}})
	_, err := e.c.Join(context.Background(), JoinRequest{GroupID: grp, Assignors: []string{RangeAssignorName}})
	require.ErrorIs(t, err, ErrInvalidGroup)
	_, members := e.parked(grp)
	require.Equal(t, 1, members)
}

func TestTimeoutDefaultsAndClamp(t *testing.T) {
	e := newEnv(t, testConfig())
	res := e.joinAll(grp,
		JoinRequest{SessionTimeout: 0},
		JoinRequest{SessionTimeout: time.Hour, RebalanceTimeout: time.Minute},
		JoinRequest{SessionTimeout: 2 * time.Second, RebalanceTimeout: -1})
	e.c.mu.Lock()
	defer e.c.mu.Unlock()
	g := e.c.groups[grp]
	type to struct{ s, r time.Duration }
	got := []to{}
	for _, r := range res {
		m := g.members[r.MemberID]
		got = append(got, to{m.session, m.rebalance})
	}
	require.Equal(t, []to{{10 * time.Second, testRebalance}, {10 * time.Second, time.Minute}, {2 * time.Second, testRebalance}}, got)
}

func TestNotCoordinator(t *testing.T) {
	e := newEnv(t, testConfig())
	cfg := testConfig()
	cfg.BrokerID = 2
	c := New(cfg, e.store, e.clock, nil)
	t.Cleanup(c.Close)
	_, err := c.Join(context.Background(), JoinRequest{GroupID: grp})
	require.ErrorIs(t, err, ErrNotCoordinator)
	require.ErrorIs(t, c.Heartbeat(context.Background(), grp, "m", 1), ErrNotCoordinator)
	_, err = c.Sync(context.Background(), grp, "m", 1)
	require.ErrorIs(t, err, ErrNotCoordinator)
	require.ErrorIs(t, c.Leave(context.Background(), grp, "m"), ErrNotCoordinator)
	_, err = c.Commit(context.Background(), grp, "", -1, nil)
	require.ErrorIs(t, err, ErrNotCoordinator)
	_, err = c.FetchOffsets(context.Background(), grp, nil)
	require.ErrorIs(t, err, ErrNotCoordinator)
	_, err = c.List(context.Background())
	require.ErrorIs(t, err, ErrNotCoordinator)
	_, err = c.Describe(context.Background(), grp)
	require.ErrorIs(t, err, ErrNotCoordinator)
}

// R15: List and Describe cover in-memory groups and groups with committed offsets only.
func TestListAndDescribe(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	_, err := e.c.Commit(context.Background(), "offsets-only", "", -1, map[TopicPartition]int64{{"t", 1}: 4})
	require.NoError(t, err)
	m := e.joinAll("live", sub("t"))[0]

	ids, err := e.c.List(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"live", "offsets-only"}, ids)

	d, err := e.c.Describe(context.Background(), "offsets-only")
	require.NoError(t, err)
	require.Equal(t, GroupDescription{GroupID: "offsets-only", State: StateEmpty,
		Committed: map[TopicPartition]int64{{"t", 1}: 4}}, d)

	d, err = e.c.Describe(context.Background(), "live")
	require.NoError(t, err)
	require.Equal(t, GroupDescription{
		GroupID: "live", State: StateStable, Generation: 1, Assignor: RangeAssignorName,
		Members:   []MemberDescription{{MemberID: m.MemberID, Topics: []string{"t"}, Assignment: Assignment{"t": {0, 1}}}},
		Committed: map[TopicPartition]int64{},
	}, d)

	d, err = e.c.Describe(context.Background(), "unknown")
	require.NoError(t, err)
	require.Equal(t, StateEmpty, d.State)
	require.Empty(t, d.Members)
}

// A newer Join from the same member supersedes its parked one.
func TestDuplicateJoinSupersedes(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	res := e.joinAll(grp, sub("t"), sub("t"))
	a, b := res[0], res[1]
	req := JoinRequest{GroupID: grp, MemberID: a.MemberID, Topics: []string{"t"}, SessionTimeout: testSession}

	req.Topics = []string{"t", "u"}
	p1 := e.startJoin(req)
	e.waitParked(grp, 1)
	p2 := e.startJoin(req)
	require.ErrorIs(t, p1.wait(t).err, ErrRebalanceInProgress)
	e.waitParked(grp, 1)
	pb := e.startJoin(JoinRequest{GroupID: grp, MemberID: b.MemberID, Topics: []string{"t"}, SessionTimeout: testSession})
	requireCover(t, []JoinResult{p2.result(t), pb.result(t)}, map[string]int{"t": 2})
}

// Leave of a parked member answers its Join with UNKNOWN_MEMBER_ID.
func TestLeaveWhileParked(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	res := e.joinAll(grp, sub("t"), sub("t"))
	p := e.startJoin(JoinRequest{GroupID: grp, MemberID: res[0].MemberID, Topics: []string{"t", "u"}, SessionTimeout: testSession})
	e.waitParked(grp, 1)
	require.NoError(t, e.c.Leave(context.Background(), grp, res[0].MemberID))
	require.ErrorIs(t, p.wait(t).err, ErrUnknownMemberID)
}

// M7: Close while a store callback is in flight does not deadlock: Close calls unsubscribe
// (which waits for the callback) without holding mu, and the callback takes mu.
func TestCloseDuringInFlightStoreCallback(t *testing.T) {
	e := newEnv(t, testConfig())
	ss := &stubSubscribeStore{Store: e.store, unsubCalled: make(chan struct{})}
	c := New(testConfig(), ss, e.clock, nil)

	started, release := make(chan struct{}), make(chan struct{})
	deliverDone := make(chan struct{})
	go func() {
		defer close(deliverDone)
		ss.deliver(func() { close(started); <-release }, metadata.Change{Kind: metadata.TopicCreated, Topic: "t"})
	}()
	<-started
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		c.Close()
	}()
	<-ss.unsubCalled // Close is now waiting for the in-flight callback
	close(release)
	require.Eventually(t, func() bool {
		select {
		case <-closeDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, time.Millisecond, "Close deadlocked with an in-flight store callback")
	<-deliverDone
}

// stubSubscribeStore captures the subscriber so tests control delivery. Its unsubscribe func
// signals unsubCalled and then waits for an in-flight delivery, like the real stores.
type stubSubscribeStore struct {
	metadata.Store
	unsubCalled chan struct{}

	deliverMu sync.Mutex // held while a callback runs
	fn        func(context.Context, metadata.Change)
	once      sync.Once
}

func (s *stubSubscribeStore) Subscribe(fn func(context.Context, metadata.Change)) (metadata.Snapshot, func()) {
	s.deliverMu.Lock()
	s.fn = fn
	s.deliverMu.Unlock()
	return s.Snapshot(), func() {
		s.once.Do(func() {
			close(s.unsubCalled)
			s.deliverMu.Lock()
			s.fn = nil
			s.deliverMu.Unlock()
		})
	}
}

// deliver runs pre and then the subscriber's callback, as one in-flight delivery.
func (s *stubSubscribeStore) deliver(pre func(), ch metadata.Change) {
	s.deliverMu.Lock()
	defer s.deliverMu.Unlock()
	pre()
	if s.fn != nil {
		s.fn(context.Background(), ch)
	}
}

// flakyStore counts proposals and runs a hook before each.
type flakyStore struct {
	metadata.Store
	mu            sync.Mutex
	n             int
	beforePropose func(n int)
}

func (s *flakyStore) Propose(ctx context.Context, cmd *streamqv1.MetadataCommand) error {
	s.mu.Lock()
	s.n++
	n := s.n
	s.mu.Unlock()
	if s.beforePropose != nil {
		s.beforePropose(n)
	}
	return s.Store.Propose(ctx, cmd)
}

// ProposeAsLeader is the path the coordinator uses for commits (leader-only, D33).
func (s *flakyStore) ProposeAsLeader(ctx context.Context, cmd *streamqv1.MetadataCommand) error {
	return s.Propose(ctx, cmd)
}

func (s *flakyStore) proposals() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// epochStore reports a configurable controller and controller epoch on top of a real store.
type epochStore struct {
	metadata.Store
	mu         sync.Mutex
	controller int32
	epoch      uint64
	barriers   int
}

// Barrier records that the coordinator waited for its FSM to catch up (M5 review G).
func (s *epochStore) Barrier(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.barriers++
	return nil
}

func (s *epochStore) set(controller int32, epoch uint64) {
	s.mu.Lock()
	s.controller, s.epoch = controller, epoch
	s.mu.Unlock()
}

func (s *epochStore) Snapshot() metadata.Snapshot {
	snap := s.Store.Snapshot()
	s.mu.Lock()
	snap.ControllerID, snap.ControllerEpoch = s.controller, s.epoch
	s.mu.Unlock()
	return snap
}

// R4: a coordinator that loses and regains leadership starts with no groups, and its commits
// carry the controller epoch its groups belong to.
func TestCoordinatorClearsGroupsOnControllerEpochChange(t *testing.T) {
	e := newEnv(t, testConfig())
	createTopic(t, e.store, "t", 2)
	es := &epochStore{Store: e.store, controller: testConfig().BrokerID, epoch: 3}
	c := New(testConfig(), es, e.clock, nil)
	t.Cleanup(c.Close)
	e2 := &env{t: t, c: c, store: e.store, clock: e.clock}
	m := e2.joinAll(grp, sub("t"))[0]
	d, err := c.Describe(context.Background(), grp)
	require.NoError(t, err)
	require.Len(t, d.Members, 1)

	es.set(99, 3) // leadership moved away
	require.ErrorIs(t, c.Heartbeat(context.Background(), grp, m.MemberID, m.GenerationID), ErrNotCoordinator)
	es.set(testConfig().BrokerID, 5) // regained in a later term
	require.ErrorIs(t, c.Heartbeat(context.Background(), grp, m.MemberID, m.GenerationID), ErrUnknownMemberID, "old membership is gone")
	d, err = c.Describe(context.Background(), grp)
	require.NoError(t, err)
	require.Empty(t, d.Members)
	es.mu.Lock()
	require.Equal(t, 2, es.barriers, "one load barrier per newly observed controller epoch (3, then 5)")
	es.mu.Unlock()
}
