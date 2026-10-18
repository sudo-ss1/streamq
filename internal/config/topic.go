package config

import (
	"fmt"
	"sort"

	"github.com/sudo-ss1/streamq/internal/storage"
)

// TopicConfig is the resolved configuration of one topic.
type TopicConfig struct {
	Log               storage.LogConfig
	MinInsyncReplicas int32
}

// topicLeaves returns the overridable topic_defaults fields of td, keyed by yaml key. Partitions
// and replication_factor are create-time arguments, not topic configs.
func topicLeaves(td *TopicDefaults) map[string]leaf {
	out := make(map[string]leaf)
	for _, l := range leaves(td) {
		k := l.dotted()
		if k == "partitions" || k == "replication_factor" {
			continue
		}
		out[k] = l
	}
	return out
}

// TopicConfigKeys returns the keys accepted as topic config overrides (the SPEC §10
// topic_defaults keys other than partitions and replication_factor).
func TopicConfigKeys() []string {
	var td TopicDefaults
	ls := topicLeaves(&td)
	keys := make([]string, 0, len(ls))
	for k := range ls {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ResolveTopicConfig applies per-topic overrides (keys from TopicConfigKeys, values as decimal
// strings or flush policy names) over the defaults and validates the result. Unknown keys or bad
// values return an error wrapping ErrInvalidConfig. min_insync_replicas is not checked against
// the replication factor (DECISIONS D16).
func ResolveTopicConfig(defaults TopicDefaults, overrides map[string]string) (TopicConfig, error) {
	td := defaults
	if err := applyOverrides(topicLeaves(&td), overrides, "topic config"); err != nil {
		return TopicConfig{}, err
	}
	if td.MinInsyncReplicas < 1 {
		return TopicConfig{}, fmt.Errorf("%w: min_insync_replicas %d must be >= 1", ErrInvalidConfig, td.MinInsyncReplicas)
	}
	lc := logConfig(td)
	if err := lc.Validate(); err != nil {
		return TopicConfig{}, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return TopicConfig{Log: lc, MinInsyncReplicas: td.MinInsyncReplicas}, nil
}
