package helm_resources_manager

import (
	"context"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/flant/addon-operator/pkg/helm_resources_manager/types"
)

func newTestManager(t *testing.T) *helmResourcesManager {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return &helmResourcesManager{
		ctx:         ctx,
		cancel:      cancel,
		eventCh:     make(chan types.ReleaseStatusEvent, 4),
		monitors:    make(map[string]*ResourcesMonitor),
		pauseCounts: make(map[string]int),
		logger:      log.NewLogger(),
	}
}

func addTestMonitor(hm *helmResourcesManager, moduleName string) *ResourcesMonitor {
	rm := NewResourcesMonitor(hm.ctx, &ResourceMonitorConfig{
		ModuleName: moduleName,
		Logger:     log.NewLogger(),
	})

	hm.l.Lock()
	hm.monitors[moduleName] = rm
	hm.l.Unlock()

	return rm
}

func TestPauseMonitorReferenceCounting(t *testing.T) {
	hm := newTestManager(t)
	rm := addTestMonitor(hm, "alpha")

	assert.False(t, rm.IsPaused())

	hm.PauseMonitor("alpha")
	hm.PauseMonitor("alpha")
	require.True(t, hm.IsModuleMonitorPaused("alpha"))
	assert.True(t, rm.IsPaused())

	// One release must not resume the monitor while another owner is active.
	hm.ResumeMonitor("alpha")
	assert.True(t, rm.IsPaused(), "nested execution must keep the monitor paused")
	assert.True(t, hm.IsModuleMonitorPaused("alpha"))

	hm.ResumeMonitor("alpha")
	assert.False(t, rm.IsPaused(), "last release resumes the monitor")
	assert.False(t, hm.IsModuleMonitorPaused("alpha"))
	assert.Empty(t, hm.PausedModules())
}

func TestResumeMonitorWithoutPauseIsNoop(t *testing.T) {
	hm := newTestManager(t)
	rm := addTestMonitor(hm, "alpha")

	hm.ResumeMonitor("alpha")
	assert.False(t, rm.IsPaused())
	assert.False(t, hm.IsModuleMonitorPaused("alpha"))
}

func TestPauseIntentSurvivesMissingAndRecreatedMonitor(t *testing.T) {
	hm := newTestManager(t)

	// Pause arrives before the monitor exists (first install window).
	hm.PauseMonitor("alpha")
	assert.True(t, hm.IsModuleMonitorPaused("alpha"))

	rm := addTestMonitor(hm, "alpha")
	assert.False(t, rm.IsPaused(), "manually added monitor is not auto-paused")

	// StartMonitor replaces the monitor and must carry over pause ownership.
	hm.StartMonitor("alpha", nil, "default", func(string) (string, string, error) {
		return "1", "deployed", nil
	})

	replaced := hm.GetMonitor("alpha")
	require.NotNil(t, replaced)
	assert.True(t, replaced.IsPaused(), "recreated monitor starts frozen")

	hm.ResumeMonitor("alpha")
	assert.False(t, replaced.IsPaused())
}

func TestStopMonitorsClearsPauseCounts(t *testing.T) {
	hm := newTestManager(t)
	addTestMonitor(hm, "alpha")
	addTestMonitor(hm, "beta")

	hm.PauseMonitor("alpha")
	hm.PauseMonitor("beta")

	hm.StopMonitors()

	assert.Empty(t, hm.PausedModules())

	// Late release of an execution after the full teardown must not create
	// negative counts.
	hm.ResumeMonitor("alpha")
	hm.ResumeMonitor("beta")
	assert.False(t, hm.IsModuleMonitorPaused("alpha"))
}

func TestPausedModulesSorted(t *testing.T) {
	hm := newTestManager(t)

	hm.PauseMonitor("zeta")
	hm.PauseMonitor("alpha")
	hm.PauseMonitor("mid")

	assert.Equal(t, []string{"alpha", "mid", "zeta"}, hm.PausedModules())
}
