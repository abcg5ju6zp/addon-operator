package helm_resources_manager

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/deckhouse/deckhouse/pkg/log"
	"k8s.io/apimachinery/pkg/labels"
	cr_cache "sigs.k8s.io/controller-runtime/pkg/cache"

	"github.com/flant/addon-operator/pkg"
	"github.com/flant/addon-operator/pkg/app"
	. "github.com/flant/addon-operator/pkg/helm_resources_manager/types"
	klient "github.com/flant/kube-client/client"
	"github.com/flant/kube-client/manifest"
)

// eventChannelBufferSize keeps absent-resource producers (one timer per
// module) from blocking each other or the startup reconciliation path
// before the event consumer starts.
const eventChannelBufferSize = 256

type HelmResourcesManager interface {
	WithDefaultNamespace(namespace string)
	Stop()
	StopMonitors()
	PauseMonitors()
	ResumeMonitors()
	StartMonitor(moduleName string, manifests []manifest.Manifest, defaultNamespace string, LastReleaseStatus func(releaseName string) (revision string, status string, err error))
	HasMonitor(moduleName string) bool
	StopMonitor(moduleName string)
	PauseMonitor(moduleName string)
	ResumeMonitor(moduleName string)
	IsModuleMonitorPaused(moduleName string) bool
	PausedModules() []string
	ReconcileMonitor(moduleName string)
	AbsentResources(moduleName string) ([]manifest.Manifest, error)
	GetMonitor(moduleName string) *ResourcesMonitor
	GetAbsentResources(templates []manifest.Manifest, defaultNamespace string) ([]manifest.Manifest, error)
	Ch() chan ReleaseStatusEvent
	KubeClient() *klient.Client
}

type helmResourcesManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	Namespace string

	cache cr_cache.Cache

	kubeClient *klient.Client

	eventCh chan ReleaseStatusEvent

	logger *log.Logger

	l        sync.RWMutex
	monitors map[string]*ResourcesMonitor

	// pauseMu guards pauseCounts. Pause intent is owned per module: it is
	// reference counted so overlapping (nested or parallel) executions do
	// not resume a monitor while another execution is still running. The
	// count survives a monitor replacement (StartMonitor) so a Helm upgrade
	// that recreates the monitor cannot accidentally resume the watch.
	pauseMu     sync.Mutex
	pauseCounts map[string]int
}

var _ HelmResourcesManager = &helmResourcesManager{}

func NewHelmResourcesManager(ctx context.Context, kclient *klient.Client, logger *log.Logger) (HelmResourcesManager, error) {
	cctx, cancel := context.WithCancel(ctx)
	if kclient == nil {
		cancel()
		return nil, fmt.Errorf("kube client not set")
	}

	cfg := kclient.RestConfig()

	defaultLabelSelector, err := labels.Parse(app.ExtraLabels)
	if err != nil {
		cancel()
		return nil, err
	}

	cache, err := cr_cache.New(cfg, cr_cache.Options{
		DefaultLabelSelector: defaultLabelSelector,
	})
	if err != nil {
		cancel()
		return nil, err
	}

	go func() {
		_ = cache.Start(cctx)
	}()

	log.Debug("Helm resource manager: cache's been started")

	if synced := cache.WaitForCacheSync(cctx); !synced {
		cancel()
		return nil, fmt.Errorf("Couldn't sync helm resource informer cache")
	}

	log.Debug("Helm resourcer manager: cache has been synced")

	return &helmResourcesManager{
		eventCh:     make(chan ReleaseStatusEvent, eventChannelBufferSize),
		monitors:    make(map[string]*ResourcesMonitor),
		pauseCounts: make(map[string]int),
		ctx:         cctx,
		cancel:      cancel,
		kubeClient:  kclient,
		cache:       cache,
		logger:      logger,
	}, nil
}

func (hm *helmResourcesManager) WithDefaultNamespace(namespace string) {
	hm.Namespace = namespace
}

func (hm *helmResourcesManager) Stop() {
	if hm.cancel != nil {
		hm.cancel()
	}
}

func (hm *helmResourcesManager) Ch() chan ReleaseStatusEvent {
	return hm.eventCh
}

func (hm *helmResourcesManager) StartMonitor(moduleName string, manifests []manifest.Manifest, defaultNamespace string, lastReleaseStatus func(releaseName string) (revision string, status string, err error)) {
	log.Debug("Start helm resources monitor for module",
		slog.String(pkg.LogKeyModule, moduleName))

	// Preserve pause ownership across the monitor replacement: a Helm
	// upgrade runs while an execution (hook or ModuleRun) froze the module.
	// The fresh monitor must start paused for the same number of owners,
	// otherwise recreating the monitor resumes the watch ahead of time.
	pauseCount := hm.monitorPauseCount(moduleName)
	hm.StopMonitor(moduleName)

	cfg := &ResourceMonitorConfig{
		ModuleName:       moduleName,
		Manifests:        manifests,
		DefaultNamespace: defaultNamespace,
		HelmStatusGetter: lastReleaseStatus,
		AbsentCb:         hm.absentResourcesCallback,
		KubeClient:       hm.kubeClient,
		Cache:            hm.cache,

		Logger: hm.logger.Named("resource-monitor"),
	}

	rm := NewResourcesMonitor(hm.ctx, cfg)

	if pauseCount > 0 {
		rm.Pause()
	}

	hm.l.Lock()
	hm.monitors[moduleName] = rm
	hm.l.Unlock()
	rm.Start()
}

func (hm *helmResourcesManager) absentResourcesCallback(moduleName string, unexpectedStatus bool, absent []manifest.Manifest, defaultNs string) {
	log.Debug("Detect absent resources for module",
		slog.String(pkg.LogKeyModule, moduleName))

	for _, m := range absent {
		log.Debug("absent module",
			slog.String(pkg.LogKeyNamespace, m.Namespace(defaultNs)),
			slog.String(pkg.LogKeyKind, m.Kind()),
			slog.String(pkg.LogKeyModule, m.Name()))
	}

	event := ReleaseStatusEvent{
		ModuleName:       moduleName,
		Absent:           absent,
		UnexpectedStatus: unexpectedStatus,
	}

	// Never block a monitor timer (or startup reconciliation) on a slow
	// consumer. A full buffer falls back to a dedicated goroutine.
	select {
	case hm.eventCh <- event:
	default:
		go func() {
			hm.eventCh <- event
		}()
	}
}

func (hm *helmResourcesManager) StopMonitors() {
	hm.l.Lock()
	for moduleName, monitor := range hm.monitors {
		monitor.Stop()
		delete(hm.monitors, moduleName)
	}
	hm.l.Unlock()

	// All watches are torn down (reload-all or shutdown): forget pause
	// ownership of removed monitors. Executions that still release later
	// hit the zero floor of ResumeMonitor.
	hm.pauseMu.Lock()
	clear(hm.pauseCounts)
	hm.pauseMu.Unlock()
}

func (hm *helmResourcesManager) PauseMonitors() {
	hm.l.RLock()
	names := make([]string, 0, len(hm.monitors))
	for moduleName := range hm.monitors {
		names = append(names, moduleName)
	}
	hm.l.RUnlock()

	for _, moduleName := range names {
		hm.PauseMonitor(moduleName)
	}
}

func (hm *helmResourcesManager) ResumeMonitors() {
	hm.l.RLock()
	names := make([]string, 0, len(hm.monitors))
	for moduleName := range hm.monitors {
		names = append(names, moduleName)
	}
	hm.l.RUnlock()

	for _, moduleName := range names {
		hm.ResumeMonitor(moduleName)
	}
}

func (hm *helmResourcesManager) StopMonitor(moduleName string) {
	hm.l.Lock()
	if monitor, ok := hm.monitors[moduleName]; ok {
		monitor.Stop()
		delete(hm.monitors, moduleName)
	}
	hm.l.Unlock()
}

// PauseMonitor adds one freeze reference for the module monitor. Calls
// nest: the monitor stays paused until the matching number of ResumeMonitor
// calls. Pause intent for a not-yet-created monitor is remembered and
// applied when StartMonitor creates it.
func (hm *helmResourcesManager) PauseMonitor(moduleName string) {
	hm.pauseMu.Lock()
	hm.pauseCounts[moduleName]++
	count := hm.pauseCounts[moduleName]
	hm.pauseMu.Unlock()

	if monitor := hm.getLiveMonitor(moduleName); monitor != nil && count > 0 {
		monitor.Pause()
	}
}

// ResumeMonitor releases one freeze reference. The physical monitor resumes
// only after the last reference is gone. An unmatched resume (e.g. after
// StopMonitors) is a safe no-op instead of flipping a live freeze.
func (hm *helmResourcesManager) ResumeMonitor(moduleName string) {
	hm.pauseMu.Lock()
	count := hm.pauseCounts[moduleName]
	if count <= 0 {
		hm.pauseMu.Unlock()
		return
	}

	count--
	if count == 0 {
		delete(hm.pauseCounts, moduleName)
	} else {
		hm.pauseCounts[moduleName] = count
	}
	hm.pauseMu.Unlock()

	if count == 0 {
		if monitor := hm.getLiveMonitor(moduleName); monitor != nil {
			monitor.Resume()
		}
	}
}

// IsModuleMonitorPaused reports whether the module monitor has at least one
// active freeze reference.
func (hm *helmResourcesManager) IsModuleMonitorPaused(moduleName string) bool {
	hm.pauseMu.Lock()
	defer hm.pauseMu.Unlock()

	return hm.pauseCounts[moduleName] > 0
}

// PausedModules returns the names of modules whose monitor currently has
// freeze references. It is used to recover orphaned pauses after a restart.
func (hm *helmResourcesManager) PausedModules() []string {
	hm.pauseMu.Lock()
	names := make([]string, 0, len(hm.pauseCounts))
	for moduleName, count := range hm.pauseCounts {
		if count > 0 {
			names = append(names, moduleName)
		}
	}
	hm.pauseMu.Unlock()

	sort.Strings(names)

	return names
}

// ReconcileMonitor immediately re-reads the latest Helm release status and
// cluster facts for the module. It is called when the last freeze owner of
// the module releases, so absent-resource decisions are based on the
// post-freeze reality rather than a late pre-freeze observation.
func (hm *helmResourcesManager) ReconcileMonitor(moduleName string) {
	monitor := hm.getLiveMonitor(moduleName)
	if monitor == nil {
		return
	}

	status, err := monitor.GetHelmReleaseStatus(moduleName)
	if err != nil {
		hm.logger.Error("reconcile after freeze: cannot get helm release status",
			slog.String(pkg.LogKeyModule, moduleName),
			log.Err(err))
	}

	switch {
	case status != "" && status != "deployed":
		hm.logger.Debug("reconcile after freeze: helm release is in unexpected status",
			slog.String(pkg.LogKeyModule, moduleName),
			slog.String(pkg.LogKeyStatus, status))

		hm.absentResourcesCallback(moduleName, true, []manifest.Manifest{}, monitor.DefaultNamespace())
	default:
		absent, err := monitor.AbsentResources()
		if err != nil {
			hm.logger.Error("reconcile after freeze: cannot list helm resources",
				slog.String(pkg.LogKeyModule, moduleName),
				log.Err(err))

			return
		}

		if len(absent) > 0 {
			hm.logger.Debug("reconcile after freeze: absent resources still present",
				slog.String(pkg.LogKeyModule, moduleName),
				slog.Int(pkg.LogKeyCount, len(absent)))

			hm.absentResourcesCallback(moduleName, false, absent, monitor.DefaultNamespace())
		} else {
			hm.logger.Debug("reconcile after freeze: all resources present",
				slog.String(pkg.LogKeyModule, moduleName))
		}
	}
}

func (hm *helmResourcesManager) monitorPauseCount(moduleName string) int {
	hm.pauseMu.Lock()
	defer hm.pauseMu.Unlock()

	return hm.pauseCounts[moduleName]
}

func (hm *helmResourcesManager) getLiveMonitor(moduleName string) *ResourcesMonitor {
	hm.l.RLock()
	defer hm.l.RUnlock()

	return hm.monitors[moduleName]
}

func (hm *helmResourcesManager) HasMonitor(moduleName string) bool {
	hm.l.RLock()
	_, ok := hm.monitors[moduleName]
	hm.l.RUnlock()

	return ok
}

func (hm *helmResourcesManager) AbsentResources(moduleName string) ([]manifest.Manifest, error) {
	hm.l.RLock()
	defer hm.l.RUnlock()

	if monitor, ok := hm.monitors[moduleName]; ok {
		return monitor.AbsentResources()
	}

	return nil, nil
}

func (hm *helmResourcesManager) GetMonitor(moduleName string) *ResourcesMonitor {
	hm.l.RLock()
	defer hm.l.RUnlock()

	return hm.monitors[moduleName]
}

func (hm *helmResourcesManager) GetAbsentResources(manifests []manifest.Manifest, defaultNamespace string) ([]manifest.Manifest, error) {
	cfg := &ResourceMonitorConfig{
		Manifests:        manifests,
		DefaultNamespace: defaultNamespace,
		KubeClient:       hm.kubeClient,
		Cache:            hm.cache,

		Logger: hm.logger.Named("resource-monitor"),
	}

	rm := NewResourcesMonitor(hm.ctx, cfg)

	return rm.AbsentResources()
}

func (hm *helmResourcesManager) KubeClient() *klient.Client {
	return hm.kubeClient
}
