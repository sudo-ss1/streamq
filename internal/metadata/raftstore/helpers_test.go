package raftstore

import (
	"bytes"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

type rlog struct {
	index uint64
	term  uint64
	data  []byte
}

func (r rlog) log() *raft.Log {
	return &raft.Log{Index: r.index, Term: r.term, Data: r.data, Type: raft.LogCommand}
}

func protoMarshal(cmd *streamqv1.MetadataCommand) ([]byte, error) { return proto.Marshal(cmd) }

// memSink is an in-memory raft.SnapshotSink.
type memSink struct{ bytes.Buffer }

func (*memSink) ID() string    { return "mem" }
func (*memSink) Cancel() error { return nil }
func (*memSink) Close() error  { return nil }
