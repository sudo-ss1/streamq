package loadgen

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/sudo-ss1/streamq/pkg/client"
)

// timestampBytes is the size of the intended send time embedded in values with -consume.
const timestampBytes = 8

// sender is the part of client.Producer the workload uses (a fake in tests).
type sender interface {
	Produce(ctx context.Context, rec client.Record, cb func(client.RecordMetadata, error)) error
}

// payloads builds deterministic record keys and values (M10 plan R15: no math/rand).
type payloads struct {
	value     []byte   // shared, never mutated after construction
	keys      [][]byte // nil = unkeyed
	stamp     bool     // embed the intended send time (end-to-end latency)
	producers int
}

// newPayloads returns the payload source for producer id.
func newPayloads(id, size, keyCardinality, producers int, stamp bool) *payloads {
	p := &payloads{value: make([]byte, size), stamp: stamp, producers: producers}
	for j := range p.value {
		p.value[j] = byte((id*31 + j) % 251)
	}
	if keyCardinality > 0 {
		p.keys = make([][]byte, keyCardinality)
		for k := range p.keys {
			p.keys[k] = fmt.Appendf(nil, "key-%08d", k)
		}
	}
	return p
}

// next returns the key and value of the seq-th record of producer id.
func (p *payloads) next(id int, seq uint64, intended time.Time) (key, value []byte) {
	if p.keys != nil {
		key = p.keys[(seq*uint64(p.producers)+uint64(id))%uint64(len(p.keys))]
	}
	if !p.stamp {
		return key, p.value // the producer only reads values, so sharing is safe
	}
	value = make([]byte, len(p.value))
	copy(value, p.value)
	binary.BigEndian.PutUint64(value, uint64(intended.UnixNano()))
	return key, value
}

// stampOf decodes the intended send time embedded by next.
func stampOf(value []byte) (time.Time, bool) {
	if len(value) < timestampBytes {
		return time.Time{}, false
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(value))), true
}

// worker drives one producer (M10 plan R1). It stops when the next intended send time reaches
// the window end or ctx ends.
type worker struct {
	id     int
	topic  string
	p      sender
	clk    Clock
	bucket *TokenBucket // nil = as fast as possible
	rec    *Recorder
	pay    *payloads
	end    time.Time // stop producing at this intended time
}

// run produces until the window ends or ctx is cancelled.
func (w *worker) run(ctx context.Context) {
	for seq := uint64(0); ctx.Err() == nil; seq++ {
		var intended time.Time
		if w.bucket != nil {
			t, err := w.bucket.Take(ctx)
			if err != nil {
				return
			}
			intended = t
		}
		start := w.clk.Now()
		if w.bucket == nil {
			intended = start
		}
		if !intended.Before(w.end) {
			return
		}
		key, value := w.pay.next(w.id, seq, intended)
		tok := w.rec.Sent(intended, start, int64(len(value)))
		err := w.p.Produce(ctx, client.Record{Topic: w.topic, Key: key, Value: value},
			func(_ client.RecordMetadata, err error) { w.rec.Done(tok, w.clk.Now(), err) })
		if err != nil {
			w.rec.Done(tok, w.clk.Now(), err) // not accepted: cb will not run
		}
	}
}
