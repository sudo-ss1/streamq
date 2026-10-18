// Package record encodes and decodes streamq record batches (SPEC §3.5).
//
// A batch is the unit of storage and transfer: its bytes on disk are exactly its bytes on the
// wire. All integers are big-endian; per-record varints are zig-zag encoded. The CRC-32C covers
// the bytes from the attributes field to the end of the batch, so the leader can stamp the base
// offset and leader epoch without recomputing it.
package record
