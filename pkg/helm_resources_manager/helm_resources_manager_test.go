package helm_resources_manager

import (
	"context"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestManager(t *testing.T) *helmResourcesManager {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return &helmResourcesManager{
		ctx:      ctx,
		cancel:   cancel,
		monitors: make(map[string]*ResourcesMonitor),
		logger:   log.NewNop(),
	}
}

func revisionMonitor(ctx context.Context, revision string) *ResourcesMonitor {
	return NewResourcesMonitor(ctx, &ResourceMonitorConfig{
		ModuleName:      "m",
		ReleaseRevision: revision,
		HelmStatusGetter: func(string) (string, string, error) {
			return revision, "deployed", nil
		},
		Logger: log.NewNop(),
	})
}

// TestSwapMonitorRefusesDowngrade pins the guard that keeps a stale
// monitor-publish transaction from overwriting the newer release monitor.
func TestSwapMonitorRefusesDowngrade(t *testing.T) {
	hm := newTestManager(t)

	require.NoError(t, hm.SwapMonitor("m", revisionMonitor(hm.ctx, "5")))
	assert.Equal(t, "5", hm.MonitorRevision("m"))

	// Multi-digit revisions must compare numerically, not lexicographically.
	err := hm.SwapMonitor("m", revisionMonitor(hm.ctx, "10"))
	require.NoError(t, err)
	assert.Equal(t, "10", hm.MonitorRevision("m"))

	err = hm.SwapMonitor("m", revisionMonitor(hm.ctx, "9"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMonitorRevisionStale)
	assert.Equal(t, "10", hm.MonitorRevision("m"), "newer monitor must stay in service")

	// Replacing with the same revision (re-publish) is allowed.
	require.NoError(t, hm.SwapMonitor("m", revisionMonitor(hm.ctx, "10")))
	assert.Equal(t, "10", hm.MonitorRevision("m"))
}

// TestSwapMonitorFirstRegistration registers when no monitor existed.
func TestSwapMonitorFirstRegistration(t *testing.T) {
	hm := newTestManager(t)

	assert.Equal(t, "", hm.MonitorRevision("missing"))
	require.NoError(t, hm.SwapMonitor("m", revisionMonitor(hm.ctx, "1")))
	assert.Equal(t, "1", hm.MonitorRevision("m"))
}

// TestRestoreMonitorNeverDowngrades checks the anti-downgrade behavior of the
// restore path used after an interrupted replacement.
func TestRestoreMonitor(t *testing.T) {
	hm := newTestManager(t)

	require.NoError(t, hm.SwapMonitor("m", revisionMonitor(hm.ctx, "3")))

	// An older snapshot must not replace the current monitor.
	require.NoError(t, hm.RestoreMonitor("m", MonitorSnapshot{
		ModuleName:      "m",
		ReleaseRevision: "2",
	}, nil))
	assert.Equal(t, "3", hm.MonitorRevision("m"))

	// With no monitor registered, an older snapshot is restored as-is.
	hm.StopMonitor("m")
	require.NoError(t, hm.RestoreMonitor("m", MonitorSnapshot{
		ModuleName:      "m",
		ReleaseRevision: "2",
	}, func(string) (string, string, error) { return "2", "deployed", nil }))
	assert.Equal(t, "2", hm.MonitorRevision("m"))
}

func TestRevisionNewer(t *testing.T) {
	assert.False(t, revisionNewer("", "1"))
	assert.True(t, revisionNewer("1", ""))
	assert.True(t, revisionNewer("10", "9"))
	assert.False(t, revisionNewer("9", "10"))
	assert.False(t, revisionNewer("2", "2"))
}
