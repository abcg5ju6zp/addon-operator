package helm_resources_manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	cr_cache "sigs.k8s.io/controller-runtime/pkg/cache"
	cr_client "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/flant/addon-operator/pkg"
	klient "github.com/flant/kube-client/client"
	"github.com/flant/kube-client/manifest"
)

const monitorDelayBase = time.Minute*4 + time.Second*30

// ErrMonitorRevisionMismatch means a monitor was built for one release revision
// while the live Helm release is already at another revision.
var ErrMonitorRevisionMismatch = errors.New("monitor revision mismatch")

type ResourceMonitorConfig struct {
	ModuleName       string
	Manifests        []manifest.Manifest
	DefaultNamespace string

	// ReleaseRevision is the Helm release revision the manifests belong to.
	// Confirm refuses to observe a release that has moved to another revision.
	ReleaseRevision string

	KubeClient *klient.Client
	Cache      cr_cache.Cache

	AbsentCb func(moduleName string, unexpectedStatus bool, absent []manifest.Manifest, defaultNs string)

	HelmStatusGetter func(releaseName string) (revision string, status string, err error)

	Logger *log.Logger
}

type ResourcesMonitor struct {
	ctx    context.Context
	cancel context.CancelFunc
	paused bool

	moduleName       string
	manifests        []manifest.Manifest
	defaultNamespace string
	releaseRevision  string

	kubeClient *klient.Client
	cache      cr_cache.Cache

	absentCb func(moduleName string, unexpectedStatus bool, absent []manifest.Manifest, defaultNs string)

	helmStatusGetter func(releaseName string) (revision string, status string, err error)

	logger *log.Logger
}

// MonitorSnapshot captures everything needed to rebuild an identical monitor.
// The monitor-publish recovery transaction uses it to restore the previous
// watch when a replacement cannot be completed.
type MonitorSnapshot struct {
	ModuleName       string
	ReleaseRevision  string
	DefaultNamespace string
	Manifests        []manifest.Manifest
}

func NewResourcesMonitor(ctx context.Context, cfg *ResourceMonitorConfig) *ResourcesMonitor {
	cctx, cancel := context.WithCancel(ctx)

	if len(cfg.Manifests) == 0 {
		cfg.Manifests = make([]manifest.Manifest, 0)
	}

	return &ResourcesMonitor{
		paused: false,
		ctx:    cctx,
		cancel: cancel,

		kubeClient: cfg.KubeClient,
		cache:      cfg.Cache,

		moduleName:       cfg.ModuleName,
		defaultNamespace: cfg.DefaultNamespace,
		manifests:        cfg.Manifests,
		releaseRevision:  cfg.ReleaseRevision,
		absentCb:         cfg.AbsentCb,
		helmStatusGetter: cfg.HelmStatusGetter,

		logger: cfg.Logger.With(pkg.LogKeyOperatorComponent, "HelmResourceMonitor"),
	}
}

// ReleaseRevision returns the Helm release revision the monitor was built for.
func (r *ResourcesMonitor) ReleaseRevision() string {
	return r.releaseRevision
}

// Snapshot returns the data required to rebuild an identical monitor.
func (r *ResourcesMonitor) Snapshot() MonitorSnapshot {
	manifests := make([]manifest.Manifest, len(r.manifests))
	copy(manifests, r.manifests)

	return MonitorSnapshot{
		ModuleName:       r.moduleName,
		ReleaseRevision:  r.releaseRevision,
		DefaultNamespace: r.defaultNamespace,
		Manifests:        manifests,
	}
}

// Confirm performs one immediate observation round, proving the monitor can
// actually watch the target release before it replaces the previous monitor:
// the live release must be deployed at the revision the monitor was built for,
// every manifest must resolve against the API, and each informer cache must
// list successfully (e.g. a CRD introduced by the release has to be
// established). Absent resources are not a failure here — detecting them is
// the monitor's periodic job.
func (r *ResourcesMonitor) Confirm(ctx context.Context) error {
	if r.helmStatusGetter == nil {
		return errors.New("helm status getter is not set")
	}

	revision, status, err := r.helmStatusGetter(r.moduleName)
	if err != nil {
		return fmt.Errorf("get helm release status: %w", err)
	}

	if r.releaseRevision != "" && revision != r.releaseRevision {
		return fmt.Errorf("%w: monitor targets revision %q, live release is at %q",
			ErrMonitorRevisionMismatch, r.releaseRevision, revision)
	}

	if !strings.EqualFold(status, "deployed") {
		return fmt.Errorf("helm release %q status is %q, want %q", r.moduleName, status, "deployed")
	}

	if err := r.warmInformers(ctx); err != nil {
		return err
	}

	r.logger.Debug("Resources monitor confirmed",
		slog.String(pkg.LogKeyModule, r.moduleName),
		slog.String(pkg.LogKeyRevision, revision),
		slog.Int(pkg.LogKeyCount, len(r.manifests)))

	return nil
}

// warmInformers resolves GVKs of all manifests and lists each of them once,
// forcing the corresponding informers to start and sync.
func (r *ResourcesMonitor) warmInformers(ctx context.Context) error {
	gvkMap, err := r.buildGVKMap()
	if err != nil {
		return fmt.Errorf("resolve manifest gvk: %w", err)
	}

	if len(gvkMap) == 0 {
		return nil
	}

	concurrency := make(chan struct{}, 5)
	errC := make(chan error, len(gvkMap))

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for nsgvk := range gvkMap {
		wg.Add(1)

		go func(nsgvk namespacedGVK) {
			defer wg.Done()

			select {
			case concurrency <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-concurrency }()

			if ctx.Err() != nil {
				return
			}

			if _, err := r.listResources(ctx, nsgvk); err != nil {
				select {
				case errC <- err:
				default:
				}

				cancel()
			}
		}(nsgvk)
	}

	go func() {
		wg.Wait()
		close(errC)
	}()

	for listErr := range errC {
		if listErr != nil {
			return fmt.Errorf("list resources from cache: %w", listErr)
		}
	}

	return nil
}

func (r *ResourcesMonitor) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
}

// Start creates a timer and check if all deployed manifests are present in the cluster.
func (r *ResourcesMonitor) Start() {
	go func() {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		randSecondsDelay := time.Second * time.Duration(rnd.Int31n(60))
		timer := time.NewTicker(monitorDelayBase + randSecondsDelay)

		for {
			select {
			case <-timer.C:
				if r.paused {
					continue
				}
				// Check release status
				status, err := r.GetHelmReleaseStatus(r.moduleName)
				if err != nil {
					r.logger.Error("cannot get helm release status", log.Err(err))
				}

				if status != "deployed" {
					r.logger.Debug("Helm release is in unexpected status",
						slog.String(pkg.LogKeyModule, r.moduleName),
						slog.String(pkg.LogKeyStatus, status))

					if r.absentCb != nil {
						r.absentCb(r.moduleName, true, []manifest.Manifest{}, r.defaultNamespace)
					}
				}

				// Check resources
				absent, err := r.AbsentResources()
				if err != nil {
					r.logger.Error("cannot list helm resources", log.Err(err))
				}

				if len(absent) > 0 {
					r.logger.Debug("Absent resources detected")

					if r.absentCb != nil {
						r.absentCb(r.moduleName, false, absent, r.defaultNamespace)
					}
				} else {
					r.logger.Debug("No absent resources detected")
				}

			case <-r.ctx.Done():
				timer.Stop()
				return
			}
		}
	}()
}

// GetHelmReleaseStatus returns last release status
func (r *ResourcesMonitor) GetHelmReleaseStatus(moduleName string) (string, error) {
	revision, status, err := r.helmStatusGetter(moduleName)
	if err != nil {
		return "", err
	}

	r.logger.Debug("Helm release",
		slog.String(pkg.LogKeyModule, moduleName),
		slog.String(pkg.LogKeyRevision, revision),
		slog.String(pkg.LogKeyStatus, status))

	return status, nil
}

// Pause prevent execution of absent callback
func (r *ResourcesMonitor) Pause() {
	r.paused = true
}

// Resume allows execution of absent callback
func (r *ResourcesMonitor) Resume() {
	r.paused = false
}

func (r *ResourcesMonitor) AbsentResources() ([]manifest.Manifest, error) {
	gvkMap, err := r.buildGVKMap()
	if err != nil {
		return nil, err
	}

	concurrency := make(chan struct{}, 5)

	// Don't use non-buffered channel here.
	// Range on line 156 will read one message from the channel and quit but other goroutines will wait for the channel
	// in 'chan send' status and stuck forever. Also, GC will not grab the channel because it has wait functions.
	resC := make(chan manifestResult, len(gvkMap))

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()

	for nsgvk, manifests := range gvkMap {
		wg.Add(1)

		go r.checkGVKManifests(ctx, &wg, nsgvk, manifests, resC, concurrency)
	}

	go func() {
		wg.Wait()
		close(resC)
	}()

	for res := range resC {
		if res.err != nil {
			return nil, res.err
		}

		// return on the first absent resource, don't wait for all
		if res.hasAbsent {
			return []manifest.Manifest{res.manifest}, nil
		}
	}

	return nil, nil
}

type namespacedGVK struct {
	Namespace string
	GVK       schema.GroupVersionKind
}

// create gvk
func (r *ResourcesMonitor) buildGVKMap() (map[namespacedGVK][]manifest.Manifest, error) {
	gvkMap := make(map[namespacedGVK][]manifest.Manifest)

	for _, m := range r.manifests {
		// Get GVK
		// log.Debugf("%s: discover GVK for apiVersion '%s' kind '%s'...", ei.Monitor.Metadata.DebugName, ei.Monitor.ApiVersion, ei.Monitor.Kind)
		apiRes, err := r.kubeClient.APIResource(m.ApiVersion(), m.Kind())
		if err != nil {
			// log.Errorf("%s: Cannot get GroupVersionKind info for apiVersion '%s' kind '%s' from api-server. Possibly CRD is not created before informers are started. Error was: %v", ei.Monitor.Metadata.DebugName, ei.Monitor.ApiVersion, ei.Monitor.Kind, err)
			return nil, err
		}
		// log.Debugf("%s: GVK for kind '%s' is '%s'", ei.Monitor.Metadata.DebugName, ei.Monitor.Kind, ei.GroupVersionKind.String())

		gvk := schema.GroupVersionKind{
			Group:   apiRes.Group,
			Version: apiRes.Version,
			Kind:    apiRes.Kind,
		}

		ns := m.Namespace(r.defaultNamespace)
		if !apiRes.Namespaced {
			ns = ""
		}

		nsgvk := namespacedGVK{
			Namespace: ns,
			GVK:       gvk,
		}

		gvkMap[nsgvk] = append(gvkMap[nsgvk], m)
	}

	return gvkMap, nil
}

type manifestResult struct {
	hasAbsent bool
	manifest  manifest.Manifest
	err       error
}

func (r *ResourcesMonitor) checkGVKManifests(ctx context.Context, wg *sync.WaitGroup, nsgvk namespacedGVK, manifests []manifest.Manifest, resC chan<- manifestResult, concurrency chan struct{}) {
	defer wg.Done()

	concurrency <- struct{}{}

	defer func() {
		<-concurrency
	}()

	// if context is cancelled - return
	if ctx.Err() != nil {
		return
	}

	existingObjs, err := r.listResources(ctx, nsgvk)
	if err != nil {
		resC <- manifestResult{
			err: err,
		}

		return
	}

	for _, mf := range manifests {
		if _, ok := existingObjs[mf.Name()]; !ok {
			resC <- manifestResult{
				hasAbsent: true,
				manifest:  mf,
			}

			return
		}
	}
}

// list all objects in ns and return names of all existent objects
func (r *ResourcesMonitor) listResources(ctx context.Context, nsgvk namespacedGVK) (map[string]struct{}, error) {
	objList := &v1.PartialObjectMetadataList{}
	objList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   nsgvk.GVK.Group,
		Version: nsgvk.GVK.Version,
		Kind:    nsgvk.GVK.Kind + "List",
	})
	log.Debug("List objects from cache",
		slog.String(pkg.LogKeyNsgvk, fmt.Sprintf("%v", nsgvk)))

	err := r.cache.List(ctx, objList, cr_client.InNamespace(nsgvk.Namespace))
	if err != nil {
		return nil, fmt.Errorf("couldn't list objects from cache: %v", err)
	}

	existingObjs := make(map[string]struct{}, len(objList.Items))
	for _, eo := range objList.Items {
		existingObjs[eo.GetName()] = struct{}{}
	}

	return existingObjs, nil
}

func (r *ResourcesMonitor) ResourceIds() []string {
	res := make([]string, 0, len(r.manifests))

	for _, m := range r.manifests {
		id := fmt.Sprintf("%s/%s/%s", m.Namespace(r.defaultNamespace), m.Kind(), m.Name())
		res = append(res, id)
	}

	sort.Strings(res)

	return res
}
