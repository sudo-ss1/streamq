package record

// BuilderOptions sets the producer fields of a batch. The zero value is not a valid
// non-idempotent batch; use NoProducerID and NoSequence for that.
type BuilderOptions struct {
	ProducerID    int64
	ProducerEpoch int16
	BaseSequence  int32
}

// Builder accumulates records and encodes them as one batch with baseOffset 0 and
// leaderEpoch 0 (the leader assigns both on append).
type Builder struct {
	opts BuilderOptions
}

// NewBuilder returns an empty Builder.
func NewBuilder(opts BuilderOptions) *Builder { return &Builder{opts: opts} }

// Append adds one record. timestamp is unix ms. nil key/value encode as null.
func (b *Builder) Append(timestamp int64, key, value []byte, headers []Header) {
	_, _, _, _ = timestamp, key, value, headers
}

// Len returns the number of records appended so far.
func (b *Builder) Len() int { return 0 }

// Size returns the encoded size the batch would have if built now.
func (b *Builder) Size() int { return HeaderSize }

// Build encodes the batch. It fails if no records were appended.
func (b *Builder) Build() (Batch, error) { return nil, errNotImplemented }
