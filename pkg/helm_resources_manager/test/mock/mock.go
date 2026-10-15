package mock

import (
	"context"
	"strconv"
	"sync"

	"github.com/deckhouse/deckhouse/pkg/log"

	. "github.com/flant/addon-operator/pkg/helm_resources_manager"
	. "github.com/flant/addon-operator/pkg/helm_resources_manager/types"
	klient "github.com/flant/kube-client/client"
	"github.com/flant/kube-client/manifest"
)

// MockHelmResourcesManager is a test double for HelmResourcesManager.
// Zero value is usable: monitor operations succeed by default and the
// prepared/swapped monitor revision is recorded.
type MockHelmResourcesManager struct {
	mu sync.Mutex

	// PrepareErr fails PrepareMonitor while it is non-nil.
	PrepareErr error
	// SwapErr fails SwapMonitor while it is non-nil.
	SwapErr error

	// PrepareCalls counts PrepareMonitor invocations.
	PrepareCalls int
	// SwapCalls counts successful SwapMonitor invocations.
	SwapCalls int
	// RestoreCalls counts RestoreMonitor invocations.
	RestoreCalls int

	// preparedRevisions records the revisions of every PrepareMonitor call.
	preparedRevisions []string

	// revisions maps module name to the revision served by its monitor.
	revisions map[string]string
}

func (h *MockHelmResourcesManager) WithContext(_ context.Context) {}

func (h *MockHelmResourcesManager) WithKubeClient(_ *klient.Client) {}

func (h *MockHelmResourcesManager) WithDefaultNamespace(_ string) {}

func (h *MockHelmResourcesManager) Stop() {}

func (h *MockHelmResourcesManager) StopMonitors() {}

func (h *MockHelmResourcesManager) PauseMonitors() {}

func (h *MockHelmResourcesManager) ResumeMonitors() {}

func (h *MockHelmResourcesManager) PrepareMonitor(_ context.Context, cfg *ResourceMonitorConfig) (*ResourcesMonitor, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.PrepareCalls++
	h.preparedRevisions = append(h.preparedRevisions, cfg.ReleaseRevision)

	if h.PrepareErr != nil {
		return nil, h.PrepareErr
	}

	if cfg.Logger == nil {
		cfg.Logger = log.NewNop()
	}

	rm := NewResourcesMonitor(context.Background(), cfg)

	return rm, nil
}

func (h *MockHelmResourcesManager) SwapMonitor(moduleName string, monitor *ResourcesMonitor) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.SwapErr != nil {
		return h.SwapErr
	}

	if current, ok := h.revisions[moduleName]; ok && revisionNumericNewer(current, monitor.ReleaseRevision()) {
		return ErrMonitorRevisionStale
	}

	if h.revisions == nil {
		h.revisions = make(map[string]string)
	}

	h.revisions[moduleName] = monitor.ReleaseRevision()
	h.SwapCalls++

	return nil
}

func (h *MockHelmResourcesManager) RestoreMonitor(moduleName string, snapshot MonitorSnapshot, _ func(string) (revision string, status string, err error)) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.RestoreCalls++

	if current, ok := h.revisions[moduleName]; ok && !revisionNumericNewer(snapshot.ReleaseRevision, current) {
		return nil
	}

	if h.revisions == nil {
		h.revisions = make(map[string]string)
	}

	h.revisions[moduleName] = snapshot.ReleaseRevision

	return nil
}

func (h *MockHelmResourcesManager) MonitorRevision(moduleName string) string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.revisions[moduleName]
}

func (h *MockHelmResourcesManager) PreparedRevisions() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]string, len(h.preparedRevisions))
	copy(out, h.preparedRevisions)

	return out
}

// SetMonitorRevision simulates a monitor already registered for the module.
func (h *MockHelmResourcesManager) SetMonitorRevision(moduleName, revision string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.revisions == nil {
		h.revisions = make(map[string]string)
	}

	h.revisions[moduleName] = revision
}

func (h *MockHelmResourcesManager) HasMonitor(moduleName string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, ok := h.revisions[moduleName]

	return ok
}

func (h *MockHelmResourcesManager) StopMonitor(moduleName string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.revisions, moduleName)
}

func (h *MockHelmResourcesManager) PauseMonitor(_ string)  {}
func (h *MockHelmResourcesManager) ResumeMonitor(_ string) {}

func (h *MockHelmResourcesManager) AbsentResources(_ string) ([]manifest.Manifest, error) {
	return nil, nil
}

func (h *MockHelmResourcesManager) GetMonitor(_ string) *ResourcesMonitor {
	return nil
}

func (h *MockHelmResourcesManager) GetAbsentResources(_ []manifest.Manifest, _ string) ([]manifest.Manifest, error) {
	return nil, nil
}

func (h *MockHelmResourcesManager) Ch() chan ReleaseStatusEvent {
	return nil
}

func (h *MockHelmResourcesManager) KubeClient() *klient.Client {
	return nil
}

func revisionNumericNewer(a, b string) bool {
	ai, errA := strconv.Atoi(a)
	bi, errB := strconv.Atoi(b)

	if errA == nil && errB == nil {
		return ai > bi
	}

	return a > b
}
