package helm_resources_manager

import (
	"context"
	"errors"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newConfirmMonitor(t *testing.T, targetRevision string, getter func(releaseName string) (revision string, status string, err error)) *ResourcesMonitor {
	t.Helper()

	return NewResourcesMonitor(context.Background(), &ResourceMonitorConfig{
		ModuleName:       "some-module",
		ReleaseRevision:  targetRevision,
		DefaultNamespace: "default",
		HelmStatusGetter: getter,
		Logger:           log.NewNop(),
	})
}

// TestResourcesMonitor_ConfirmRevisionMismatch: a monitor built for one
// revision must refuse to observe a release that moved to another revision.
func TestResourcesMonitor_ConfirmRevisionMismatch(t *testing.T) {
	mon := newConfirmMonitor(t, "2", func(string) (string, string, error) {
		return "3", "deployed", nil
	})

	err := mon.Confirm(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMonitorRevisionMismatch)
}

// TestResourcesMonitor_ConfirmNonDeployedStatus: a non-deployed release is
// not confirmed even when the revision matches.
func TestResourcesMonitor_ConfirmNonDeployedStatus(t *testing.T) {
	mon := newConfirmMonitor(t, "2", func(string) (string, string, error) {
		return "2", "failed", nil
	})

	err := mon.Confirm(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "want")
}

// TestResourcesMonitor_ConfirmStatusError surfaces helm status errors.
func TestResourcesMonitor_ConfirmStatusError(t *testing.T) {
	mon := newConfirmMonitor(t, "2", func(string) (string, string, error) {
		return "", "", errors.New("apiserver unavailable")
	})

	err := mon.Confirm(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get helm release status")
}

// TestResourcesMonitor_ConfirmWithoutGetter fails explicitly instead of panicking.
func TestResourcesMonitor_ConfirmWithoutGetter(t *testing.T) {
	mon := NewResourcesMonitor(context.Background(), &ResourceMonitorConfig{
		ModuleName: "some-module",
		Logger:     log.NewNop(),
	})

	err := mon.Confirm(context.Background())
	require.Error(t, err)
}

// TestResourcesMonitor_ConfirmEmptyManifests: a chart with no resources is
// confirmed immediately on revision/status match without touching the cache.
func TestResourcesMonitor_ConfirmEmptyManifests(t *testing.T) {
	mon := newConfirmMonitor(t, "2", func(string) (string, string, error) {
		return "2", "deployed", nil
	})

	require.NoError(t, mon.Confirm(context.Background()))
	assert.Equal(t, "2", mon.ReleaseRevision())
}

// TestResourcesMonitor_SnapshotRoundTrip records the data required to rebuild
// the previous monitor.
func TestResourcesMonitor_SnapshotRoundTrip(t *testing.T) {
	mon := newConfirmMonitor(t, "7", func(string) (string, string, error) {
		return "7", "deployed", nil
	})

	snapshot := mon.Snapshot()
	assert.Equal(t, "some-module", snapshot.ModuleName)
	assert.Equal(t, "7", snapshot.ReleaseRevision)
	assert.Equal(t, "default", snapshot.DefaultNamespace)

	restored := NewResourcesMonitor(context.Background(), &ResourceMonitorConfig{
		ModuleName:       snapshot.ModuleName,
		ReleaseRevision:  snapshot.ReleaseRevision,
		DefaultNamespace: snapshot.DefaultNamespace,
		Manifests:        snapshot.Manifests,
		HelmStatusGetter: mon.helmStatusGetter,
		Logger:           log.NewNop(),
	})

	assert.Equal(t, "7", restored.ReleaseRevision())
	require.NoError(t, restored.Confirm(context.Background()))
}
