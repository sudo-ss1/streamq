package config

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// envPrefix prefixes every environment variable the broker reads.
const envPrefix = "STREAMQ_"

// leaf is one settable scalar config key found by walking yaml tags.
type leaf struct {
	path []string      // yaml key path, e.g. ["topic_defaults", "partitions"]
	v    reflect.Value // settable field value
}

// dotted returns the flag key, e.g. "topic_defaults.partitions".
func (l leaf) dotted() string { return strings.Join(l.path, ".") }

// envKey returns the environment key, e.g. "STREAMQ_TOPIC_DEFAULTS_PARTITIONS".
func (l leaf) envKey() string { return envPrefix + strings.ToUpper(strings.Join(l.path, "_")) }

// leaves walks the struct pointed to by ptr and returns every scalar field (int32, int64 or
// string) keyed by yaml tag. Slices (e.g. cluster) are skipped: they are file-only.
func leaves(ptr any) []leaf {
	var out []leaf
	walk(reflect.ValueOf(ptr).Elem(), nil, &out)
	return out
}

func walk(v reflect.Value, prefix []string, out *[]leaf) {
	t := v.Type()
	for i := range t.NumField() {
		sf := t.Field(i)
		name, _, _ := strings.Cut(sf.Tag.Get("yaml"), ",")
		if name == "" || name == "-" || !sf.IsExported() {
			continue
		}
		path := append(append([]string(nil), prefix...), name)
		fv := v.Field(i)
		switch fv.Kind() {
		case reflect.Struct:
			walk(fv, path, out)
		case reflect.Int32, reflect.Int64, reflect.String:
			*out = append(*out, leaf{path: path, v: fv})
		default:
			// Slices (cluster) and other kinds are file-only.
		}
	}
}

// set parses raw into the leaf's field.
func (l leaf) set(raw string) error {
	switch l.v.Kind() {
	case reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, l.v.Type().Bits())
		if err != nil {
			return fmt.Errorf("%s: invalid integer %q: %w", l.dotted(), raw, err)
		}
		l.v.SetInt(n)
	case reflect.String:
		l.v.SetString(raw)
	default:
		return fmt.Errorf("%s: unsupported kind %s", l.dotted(), l.v.Kind())
	}
	return nil
}

// FlagKeys returns the sorted dotted yaml paths of every scalar config key, one per command-line
// flag (e.g. "broker_id", "topic_defaults.partitions").
func FlagKeys() []string {
	var c Config
	ls := leaves(&c)
	keys := make([]string, 0, len(ls))
	for _, l := range ls {
		keys = append(keys, l.dotted())
	}
	sort.Strings(keys)
	return keys
}

// applyEnv sets every scalar key that has a STREAMQ_<UPPER_SNAKE_KEY> entry in env. Later entries
// win. STREAMQ_* variables that match no key are ignored (they may belong to tooling).
func applyEnv(cfg *Config, env []string) error {
	vals := make(map[string]string)
	for _, kv := range env {
		k, v, ok := strings.Cut(kv, "=")
		if ok && strings.HasPrefix(k, envPrefix) {
			vals[k] = v
		}
	}
	var errs []error
	for _, l := range leaves(cfg) {
		raw, ok := vals[l.envKey()]
		if !ok {
			continue
		}
		if err := l.set(raw); err != nil {
			errs = append(errs, fmt.Errorf("env %s: %w", l.envKey(), err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
	}
	return nil
}

// applyFlags sets the scalar keys named by dotted yaml path in flags. Unknown keys are an error.
func applyFlags(cfg *Config, flags map[string]string) error {
	byKey := make(map[string]leaf)
	for _, l := range leaves(cfg) {
		byKey[l.dotted()] = l
	}
	return applyOverrides(byKey, flags, "flag")
}

// applyOverrides sets each override (sorted by key, so errors are deterministic) on the matching
// leaf, collecting every unknown key and bad value into one error wrapping ErrInvalidConfig.
func applyOverrides(byKey map[string]leaf, overrides map[string]string, what string) error {
	keys := make([]string, 0, len(overrides))
	for k := range overrides {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var errs []error
	for _, k := range keys {
		l, ok := byKey[k]
		if !ok {
			errs = append(errs, fmt.Errorf("unknown %s key %q", what, k))
			continue
		}
		if err := l.set(overrides[k]); err != nil {
			errs = append(errs, fmt.Errorf("%s %w", what, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
	}
	return nil
}
