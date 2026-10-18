package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// TestContractDependencies pins the packages the M8 work packages use, so go.mod/go.sum stay
// frozen after the contract commit (plan R11). Package A replaces it with real tests.
func TestContractDependencies(t *testing.T) {
	m := New()
	require.NoError(t, m.Registry().Register(collectors.NewBuildInfoCollector()))
	n, err := testutil.GatherAndCount(m.Registry())
	require.NoError(t, err)
	require.Positive(t, n)
	p := expfmt.NewTextParser(model.UTF8Validation)
	_, err = p.TextToMetricFamilies(strings.NewReader("x 1\n"))
	require.NoError(t, err)
}
