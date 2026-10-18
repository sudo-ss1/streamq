package loadgen

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// gitTimeout bounds each git command of the environment capture.
const gitTimeout = 5 * time.Second

// Environment describes where the run happened (M10 plan R7, R17).
type Environment struct {
	CPU         string `json:"cpu"`
	Cores       int    `json:"cores"`
	RAM         string `json:"ram"`
	OS          string `json:"os"`
	Kernel      string `json:"kernel"`
	Go          string `json:"go"`
	Commit      string `json:"commit"`
	Dirty       bool   `json:"dirty"`
	Filesystem  string `json:"filesystem"`
	FlushPolicy string `json:"flush_policy"`
	PeakRSS     string `json:"peak_rss"`
}

// captureEnvironment gathers the environment. dataDir is the broker data dir ("" when external).
func captureEnvironment(ctx context.Context, dataDir, flushPolicy string) Environment {
	env := Environment{
		CPU:         cpuModel(),
		Cores:       runtime.NumCPU(),
		RAM:         procField("/proc/meminfo", "MemTotal"),
		OS:          runtime.GOOS + "/" + runtime.GOARCH,
		Kernel:      readTrim("/proc/sys/kernel/osrelease"),
		Go:          runtime.Version(),
		Commit:      "unknown",
		Filesystem:  "unknown",
		FlushPolicy: flushPolicy,
	}
	if out, err := git(ctx, "rev-parse", "HEAD"); err == nil {
		env.Commit = strings.TrimSpace(out)
		if st, err := git(ctx, "status", "--porcelain"); err == nil {
			env.Dirty = strings.TrimSpace(st) != ""
		}
	}
	if dataDir != "" {
		if mounts, err := os.ReadFile("/proc/mounts"); err == nil {
			env.Filesystem = filesystemOf(string(mounts), dataDir)
		}
	}
	return env
}

// git runs a fixed git command in the working directory.
func git(ctx context.Context, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, gitTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", args...).Output() //nolint:gosec // fixed git subcommands chosen by this file
	return string(out), err
}

// peakRSS returns VmHWM from /proc/self/status, or "unknown".
func peakRSS() string { return procField("/proc/self/status", "VmHWM") }

// cpuModel returns the first "model name" in /proc/cpuinfo, or "unknown".
func cpuModel() string { return procField("/proc/cpuinfo", "model name") }

// procField returns the value of the first "key: value" line of a /proc file, or "unknown".
func procField(path, key string) string {
	data, err := os.ReadFile(path) //nolint:gosec // fixed /proc paths chosen by this file
	if err != nil {
		return "unknown"
	}
	return parseField(string(data), key)
}

// parseField returns the value of the first "key: value" line, or "unknown".
func parseField(data, key string) string {
	sc := bufio.NewScanner(strings.NewReader(data))
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), ":")
		if ok && strings.TrimSpace(k) == key {
			return strings.Join(strings.Fields(v), " ")
		}
	}
	return "unknown"
}

// readTrim returns a small file's trimmed content, or "unknown".
func readTrim(path string) string {
	data, err := os.ReadFile(path) //nolint:gosec // fixed /proc paths chosen by this file
	if err != nil {
		return "unknown"
	}
	return string(bytes.TrimSpace(data))
}

// filesystemOf returns "fstype (device on mountpoint)" for the mount holding dir, using the
// longest matching mount point of a /proc/mounts listing.
func filesystemOf(mounts, dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "unknown"
	}
	if r, err := filepath.EvalSymlinks(abs); err == nil {
		abs = r
	}
	best, bestLen := "unknown", -1
	sc := bufio.NewScanner(strings.NewReader(mounts))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 3 {
			continue
		}
		mp := unescapeMount(f[1])
		if !within(abs, mp) || len(mp) <= bestLen {
			continue
		}
		best, bestLen = f[2]+" ("+f[0]+" on "+mp+")", len(mp)
	}
	return best
}

// within reports whether path is mount point mp or below it.
func within(path, mp string) bool {
	if mp == "/" {
		return strings.HasPrefix(path, "/")
	}
	return path == mp || strings.HasPrefix(path, mp+"/")
}

// unescapeMount decodes the octal escapes (\040 for space, ...) of /proc/mounts fields.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
