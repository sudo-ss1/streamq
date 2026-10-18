package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMmapFileOpen(t *testing.T) {
	tests := []struct {
		name     string
		existing []byte // nil: no file
		size     int
		wantOrig int64
		wantSize int64
	}{
		{"new file", nil, 64, 0, 64},
		{"smaller file is extended", []byte("abcd"), 64, 4, 64},
		{"larger file is kept", make([]byte, 128), 64, 128, 128},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "f")
			if tc.existing != nil {
				require.NoError(t, os.WriteFile(path, tc.existing, fileMode))
			}
			m, orig, err := openMmapFile(path, tc.size)
			require.NoError(t, err)
			require.Equal(t, tc.wantOrig, orig)
			require.Len(t, m.data, tc.size)
			fi, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, tc.wantSize, fi.Size())
			if tc.existing == nil {
				require.Equal(t, os.FileMode(fileMode), fi.Mode().Perm()&os.FileMode(fileMode))
			} else {
				require.Equal(t, tc.existing[:min(len(tc.existing), tc.size)], m.data[:min(len(tc.existing), tc.size)])
			}
			require.NoError(t, m.close())
		})
	}
}

func TestMmapFileWriteTruncateSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	m, _, err := openMmapFile(path, 4096)
	require.NoError(t, err)
	copy(m.data, "hello")
	require.NoError(t, m.sync())

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, got, 4096)
	require.Equal(t, "hello", string(got[:5]))

	// truncate changes the file size but keeps the mapping usable below the new EOF.
	require.NoError(t, m.truncate(5))
	require.Len(t, m.data, 4096)
	require.Equal(t, "hello", string(m.data[:5]))
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))

	require.NoError(t, m.truncate(4096))
	m.data[10] = 'x'
	require.NoError(t, m.sync())
	require.NoError(t, m.close())

	got, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, got, 4096)
	require.Equal(t, byte('x'), got[10])
	require.Equal(t, byte(0), got[6], "re-extended region reads as zero")
}

func TestMmapFileErrors(t *testing.T) {
	dir := t.TempDir()
	_, _, err := openMmapFile(filepath.Join(dir, "f"), 0)
	require.Error(t, err)
	_, _, err = openMmapFile(filepath.Join(dir, "missing", "f"), 64)
	require.Error(t, err)
}
