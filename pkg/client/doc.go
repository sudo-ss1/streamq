// Package client is the Go client library for streamq: a Client (connections and a metadata
// cache), a batching, retrying Producer, a standalone Consumer, and an Admin API.
//
// Metadata is cached per topic and refreshed lazily when it is older than MetadataMaxAge or
// when a broker answers NOT_LEADER, FENCED_LEADER_EPOCH or UNKNOWN_TOPIC_OR_PARTITION (FR12).
// Retriable failures are retried with exponential backoff and full jitter.
package client
