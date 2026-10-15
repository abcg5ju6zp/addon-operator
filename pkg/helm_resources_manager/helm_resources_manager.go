package helm_resources_manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
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

// ErrMonitorRevisionStale means a monitor swap was refused because the
// registered monitor already serves a newer Helm release revision. A stale
// monitor-publish transaction must never overwrite the newer monitor.
var ErrMonitorRevisionStale = errors.New("registered monitor serves a newer release revision")

type HelmResourcesManager interface {
	WithDefaultNamespace(namespace string)
	Stop()
	StopMonitors()
	PauseMonitors()
	ResumeMonitors()
	// PrepareMonitor builds a monitor and proves (via Confirm) that it observes
	// the target release, without registering it or touching the current monitor.
	PrepareMonitor(ctx context.Context, cfg *ResourceMonitorConfig) (*ResourcesMonitor, error)
	// SwapMonitor atomically replaces the module's monitor. It refuses to
	// downgrade a monitor that already serves a newer release revision.
	SwapMonitor(moduleName string, monitor *ResourcesMonitor) error
	// RestoreMonitor registers a monitor rebuilt from a snapshot without
	// running Confirm, used to restore the previous watch after an interrupted
	// replacement. It never overwrites a monitor serving an equal or newer
	// revision.
	RestoreMonitor(moduleName string, snapshot MonitorSnapshot, lastReleaseStatus func(releaseName string) (revision string, status string, err error)) error
	// MonitorRevision returns the revision served by the registered monitor
	// (empty string when the module has no monitor).
	MonitorRevision(moduleName string) string
	HasMonitor(moduleName string) bool
	StopMonitor(moduleName string)
	PauseMonitor(moduleName string)
	ResumeMonitor(moduleName string)
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
		eventCh:    make(chan ReleaseStatusEvent),
		monitors:   make(map[string]*ResourcesMonitor),
		ctx:        cctx,
		cancel:     cancel,
		kubeClient: kclient,
		cache:      cache,
		logger:     logger,
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

// PrepareMonitor builds a monitor for the target release and runs one
// observation round (revision/status check, GVK resolution, cache lists)
// without registering it or stopping the monitor currently in use.
func (hm *helmResourcesManager) PrepareMonitor(ctx context.Context, cfg *ResourceMonitorConfig) (*ResourcesMonitor, error) {
	log.Debug("Prepare helm resources monitor for module",
		slog.String(pkg.LogKeyModule, cfg.ModuleName),
		slog.String(pkg.LogKeyRevision, cfg.ReleaseRevision))

	cfg.KubeClient = hm.kubeClient
	cfg.Cache = hm.cache
	cfg.AbsentCb = hm.absentResourcesCallback

	if cfg.Logger == nil {
		cfg.Logger = hm.logger.Named("resource-monitor")
	}

	rm := NewResourcesMonitor(hm.ctx, cfg)

	if err := rm.Confirm(ctx); err != nil {
		return nil, err
	}

	return rm, nil
}

// SwapMonitor atomically registers the prepared monitor, stopping the old one.
// It refuses to replace a monitor that already serves a newer revision, so a
// stale publish transaction cannot overwrite a monitor for a newer release.
func (hm *helmResourcesManager) SwapMonitor(moduleName string, monitor *ResourcesMonitor) error {
	hm.l.Lock()
	defer hm.l.Unlock()

	if current, ok := hm.monitors[moduleName]; ok {
		if revisionNewer(current.ReleaseRevision(), monitor.ReleaseRevision()) {
			return fmt.Errorf("%w: module %q has revision %q, refusing monitor for revision %q",
				ErrMonitorRevisionStale, moduleName, current.ReleaseRevision(), monitor.ReleaseRevision())
		}

		current.Stop()
	}

	hm.monitors[moduleName] = monitor
	monitor.Start()

	log.Debug("Helm resources monitor swapped for module",
		slog.String(pkg.LogKeyModule, moduleName),
		slog.String(pkg.LogKeyRevision, monitor.ReleaseRevision()))

	return nil
}

// RestoreMonitor rebuilds and registers a monitor from its snapshot without
// running Confirm, restoring the previous watch after an interrupted
// replacement. It never overwrites a monitor for an equal or newer revision.
func (hm *helmResourcesManager) RestoreMonitor(moduleName string, snapshot MonitorSnapshot, lastReleaseStatus func(releaseName string) (revision string, status string, err error)) error {
	hm.l.Lock()
	defer hm.l.Unlock()

	if current, ok := hm.monitors[moduleName]; ok && !revisionNewer(snapshot.ReleaseRevision, current.ReleaseRevision()) {
		// A monitor serving the same or a newer revision is already in place.
		return nil
	}

	cfg := &ResourceMonitorConfig{
		ModuleName:       snapshot.ModuleName,
		Manifests:        snapshot.Manifests,
		DefaultNamespace: snapshot.DefaultNamespace,
		ReleaseRevision:  snapshot.ReleaseRevision,
		HelmStatusGetter: lastReleaseStatus,
		AbsentCb:         hm.absentResourcesCallback,
		KubeClient:       hm.kubeClient,
		Cache:            hm.cache,
		Logger:           hm.logger.Named("resource-monitor"),
	}

	rm := NewResourcesMonitor(hm.ctx, cfg)

	if current, ok := hm.monitors[moduleName]; ok {
		current.Stop()
	}

	hm.monitors[moduleName] = rm
	rm.Start()

	log.Debug("Helm resources monitor restored for module",
		slog.String(pkg.LogKeyModule, moduleName),
		slog.String(pkg.LogKeyRevision, snapshot.ReleaseRevision))

	return nil
}

func (hm *helmResourcesManager) MonitorRevision(moduleName string) string {
	hm.l.RLock()
	defer hm.l.RUnlock()

	if monitor, ok := hm.monitors[moduleName]; ok {
		return monitor.ReleaseRevision()
	}

	return ""
}

// revisionNewer reports whether revision a is newer than revision b.
// Helm revisions are integer strings; an empty revision means "unknown" and
// is never considered newer (so it cannot block replacement).
func revisionNewer(a, b string) bool {
	if a == "" {
		return false
	}

	if b == "" {
		return true
	}

	ai, errA := strconv.Atoi(a)
	bi, errB := strconv.Atoi(b)

	if errA == nil && errB == nil {
		return ai > bi
	}

	return a > b
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

	hm.eventCh <- ReleaseStatusEvent{
		ModuleName:       moduleName,
		Absent:           absent,
		UnexpectedStatus: unexpectedStatus,
	}
}

func (hm *helmResourcesManager) StopMonitors() {
	hm.l.Lock()
	for moduleName, monitor := range hm.monitors {
		monitor.Stop()
		delete(hm.monitors, moduleName)
	}
	hm.l.Unlock()
}

func (hm *helmResourcesManager) PauseMonitors() {
	hm.l.RLock()

	for _, monitor := range hm.monitors {
		monitor.Pause()
	}

	hm.l.RUnlock()
}

func (hm *helmResourcesManager) ResumeMonitors() {
	hm.l.RLock()

	for _, monitor := range hm.monitors {
		monitor.Resume()
	}

	hm.l.RUnlock()
}

func (hm *helmResourcesManager) StopMonitor(moduleName string) {
	hm.l.Lock()
	if monitor, ok := hm.monitors[moduleName]; ok {
		monitor.Stop()
		delete(hm.monitors, moduleName)
	}
	hm.l.Unlock()
}

func (hm *helmResourcesManager) PauseMonitor(moduleName string) {
	hm.l.RLock()

	if monitor, ok := hm.monitors[moduleName]; ok {
		monitor.Pause()
	}

	hm.l.RUnlock()
}

func (hm *helmResourcesManager) ResumeMonitor(moduleName string) {
	hm.l.RLock()

	if monitor, ok := hm.monitors[moduleName]; ok {
		monitor.Resume()
	}

	hm.l.RUnlock()
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
