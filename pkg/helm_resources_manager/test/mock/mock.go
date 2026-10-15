package mock

import (
	"context"

	. "github.com/flant/addon-operator/pkg/helm_resources_manager"
	. "github.com/flant/addon-operator/pkg/helm_resources_manager/types"
	klient "github.com/flant/kube-client/client"
	"github.com/flant/kube-client/manifest"
)

type MockHelmResourcesManager struct {
	MonitorsNames []string

	// PauseRefs counts pause calls per module so tests can assert freeze
	// ownership behavior without a real manager.
	PauseRefs map[string]int
}

func (h *MockHelmResourcesManager) WithContext(_ context.Context) {}

func (h *MockHelmResourcesManager) WithKubeClient(_ *klient.Client) {}

func (h *MockHelmResourcesManager) WithDefaultNamespace(_ string) {}

func (h *MockHelmResourcesManager) Stop() {}

func (h *MockHelmResourcesManager) StopMonitors() {}

func (h *MockHelmResourcesManager) PauseMonitors() {}

func (h *MockHelmResourcesManager) ResumeMonitors() {}

func (h *MockHelmResourcesManager) StartMonitor(_ string, _ []manifest.Manifest, _ string, _ func(string) (revision string, status string, err error)) {
}

func (h *MockHelmResourcesManager) HasMonitor(_ string) bool {
	return false
}
func (h *MockHelmResourcesManager) StopMonitor(_ string) {}

func (h *MockHelmResourcesManager) PauseMonitor(module string) {
	if h.PauseRefs == nil {
		h.PauseRefs = make(map[string]int)
	}

	h.PauseRefs[module]++
}

func (h *MockHelmResourcesManager) ResumeMonitor(module string) {
	if h.PauseRefs == nil {
		return
	}

	h.PauseRefs[module]--
	if h.PauseRefs[module] <= 0 {
		delete(h.PauseRefs, module)
	}
}

func (h *MockHelmResourcesManager) IsModuleMonitorPaused(module string) bool {
	return h.PauseRefs != nil && h.PauseRefs[module] > 0
}

func (h *MockHelmResourcesManager) PausedModules() []string {
	return nil
}

func (h *MockHelmResourcesManager) ReconcileMonitor(_ string) {}

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
