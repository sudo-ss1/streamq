package main

import (
	"context"
	"net"
	"strconv"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// router sends partition requests to each partition's leader (from one Metadata call), so
// produce and consume work on multi-broker clusters. Partitions whose leader or address is unknown
// go to the bootstrap broker, which answers with an error code the caller reports.
type router struct {
	a        *app
	boot     streamqv1.StreamQServiceClient
	leaderOf map[int32]int32
	addrs    map[int32]string
	clients  map[int32]streamqv1.StreamQServiceClient
	closers  []func()
}

// newRouter fetches topic metadata (failing on a topic-level error) and the broker addresses.
// The caller must close the router.
func (a *app) newRouter(ctx context.Context, boot streamqv1.StreamQServiceClient, topic string) (*router, *streamqv1.TopicMetadata, error) {
	md, brokers, err := a.topicMetadata(ctx, boot, topic)
	if err != nil {
		return nil, nil, err
	}
	r := &router{a: a, boot: boot, leaderOf: map[int32]int32{}, addrs: map[int32]string{},
		clients: map[int32]streamqv1.StreamQServiceClient{}}
	for _, b := range brokers {
		r.addrs[b.GetId()] = net.JoinHostPort(b.GetHost(), strconv.Itoa(int(b.GetPort())))
	}
	for _, p := range md.GetPartitions() {
		r.leaderOf[p.GetPartition()] = p.GetLeaderId()
	}
	return r, md, nil
}

// close releases the leader connections.
func (r *router) close() {
	for _, c := range r.closers {
		c()
	}
}

// clientFor returns the client for partition p's leader (the bootstrap if unknown).
func (r *router) clientFor(ctx context.Context, p int32) streamqv1.StreamQServiceClient {
	leader, ok := r.leaderOf[p]
	if !ok || leader < 0 {
		return r.boot
	}
	if c, ok := r.clients[leader]; ok {
		return c
	}
	addr, ok := r.addrs[leader]
	if !ok {
		return r.boot
	}
	conn, err := r.a.opts.dial(ctx, addr)
	if err != nil {
		return r.boot
	}
	c := streamqv1.NewStreamQServiceClient(conn)
	r.clients[leader] = c
	r.closers = append(r.closers, func() { _ = conn.Close() })
	return c
}

// group splits partitions by the client that serves them, keeping each group's input order.
func (r *router) group(ctx context.Context, parts []int32) ([]streamqv1.StreamQServiceClient, map[streamqv1.StreamQServiceClient][]int32) {
	var order []streamqv1.StreamQServiceClient
	out := map[streamqv1.StreamQServiceClient][]int32{}
	for _, p := range parts {
		c := r.clientFor(ctx, p)
		if _, ok := out[c]; !ok {
			order = append(order, c)
		}
		out[c] = append(out[c], p)
	}
	return order, out
}
