package module_manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"

	"github.com/flant/addon-operator/pkg"
	"github.com/flant/addon-operator/pkg/helm_resources_manager"
	"github.com/flant/addon-operator/pkg/module_manager/models/modules"
	"github.com/flant/kube-client/manifest"
)

// monitorPublishStage is the registered stage of a monitor-publish
// transaction. The stage drives recovery when the task is retried or the
// process re-enters publication: re-fetch the manifest, continue the new
// monitor, or restore the previous one.
type monitorPublishStage string

const (
	// stageReleaseClaimed: the Helm run is confirmed; release revision,
	// manifest digest and raw manifests are registered; the previous monitor
	// (if any) is still in service.
	stageReleaseClaimed monitorPublishStage = "ReleaseClaimed"
	// stageManifestReady: the rendered YAML was parsed for the target
	// revision and the previous monitor state was snapshotted.
	stageManifestReady monitorPublishStage = "ManifestReady"
	// stageMonitorReady: the new monitor was built and proved it observes the
	// target release, but it is not registered yet; the previous monitor is
	// still in service.
	stageMonitorReady monitorPublishStage = "MonitorReady"
	// stageMonitorCommitted: the new monitor is registered in place of the
	// previous one; a final revision verification remains.
	stageMonitorCommitted monitorPublishStage = "Committed"
)

// monitorClaimFetcher re-anchors a publish transaction to the release
// currently in the cluster by rendering the chart (no Helm mutation).
type monitorClaimFetcher interface {
	FetchMonitorClaim(moduleName string) (*modules.HelmRunResult, error)
}

// helmReleaseStatusGetter returns the live Helm release revision and status.
type helmReleaseStatusGetter func(releaseName string) (revision string, status string, err error)

// monitorPublishState is the registered recovery record for one module.
// It records the release revision, manifest digest, the previous monitor
// state and the new monitor confirmation result, as required by the
// monitor-publish recovery phase.
type monitorPublishState struct {
	mu sync.Mutex

	moduleName       string
	revision         string
	manifestDigest   string
	rendered         string
	defaultNamespace string
	ranUpgrade       bool

	stage monitorPublishStage

	manifests []manifest.Manifest

	// oldMonitor snapshots the monitor in service before the replacement.
	// It allows restoring the previous watch if the swap cannot complete.
	oldMonitor *helm_resources_manager.MonitorSnapshot
	// prepared is the new monitor that observed the target revision.
	prepared *helm_resources_manager.ResourcesMonitor

	confirmed bool
	lastErr   error
	updatedAt time.Time
}

// MonitorPublisher owns per-module monitor-publish transactions. A failed
// publication is retried from the registered stage without re-running Helm;
// stale transactions (targeting a revision older than the live release) can
// never overwrite a monitor for a newer release.
type MonitorPublisher struct {
	resourcesManager helm_resources_manager.HelmResourcesManager
	claimFetcher     monitorClaimFetcher
	statusGetter     helmReleaseStatusGetter
	logger           *log.Logger

	mu     sync.Mutex
	states map[string]*monitorPublishState
}

// NewMonitorPublisher builds a MonitorPublisher.
func NewMonitorPublisher(
	hrm helm_resources_manager.HelmResourcesManager,
	claimFetcher monitorClaimFetcher,
	statusGetter helmReleaseStatusGetter,
	logger *log.Logger,
) *MonitorPublisher {
	return &MonitorPublisher{
		resourcesManager: hrm,
		claimFetcher:     claimFetcher,
		statusGetter:     statusGetter,
		logger:           logger.Named("monitor-publisher"),
		states:           make(map[string]*monitorPublishState),
	}
}

// Claim registers a fresh publish transaction for the result of a confirmed
// Helm run. It supersedes any older transaction for the module.
func (p *MonitorPublisher) Claim(moduleName string, result *modules.HelmRunResult) {
	if result == nil {
		return
	}

	st := &monitorPublishState{
		moduleName:       moduleName,
		revision:         result.Revision,
		manifestDigest:   result.ManifestChecksum,
		rendered:         result.RenderedManifests,
		defaultNamespace: result.DefaultNamespace,
		ranUpgrade:       result.RanUpgrade,
		stage:            stageReleaseClaimed,
		updatedAt:        time.Now(),
	}

	p.mu.Lock()
	if old := p.states[moduleName]; old != nil && old.prepared != nil {
		// An uncommitted prepared monitor from the superseded transaction
		// must never start: it is not registered anywhere, so just drop it.
		old.prepared = nil
	}

	p.states[moduleName] = st
	p.mu.Unlock()

	p.logger.Info("monitor publish claimed",
		slog.String(pkg.LogKeyModule, moduleName),
		slog.String(pkg.LogKeyRevision, result.Revision),
		slog.String(pkg.LogKeyChecksum, result.ManifestChecksum),
		slog.Bool("ranUpgrade", result.RanUpgrade))
}

// Discard drops the registered transaction for a module (module deletion,
// unmanaged release).
func (p *MonitorPublisher) Discard(moduleName string) {
	p.mu.Lock()
	delete(p.states, moduleName)
	p.mu.Unlock()
}

// HasTransaction reports whether a publish transaction is registered.
func (p *MonitorPublisher) HasTransaction(moduleName string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.states[moduleName]

	return ok
}

// Publish drives the registered transaction until the monitor for the current
// release is confirmed in service. It is idempotent and safe to retry: each
// attempt resumes from the registered stage.
func (p *MonitorPublisher) Publish(ctx context.Context, moduleName string) error {
	liveRevision, liveStatus, err := p.statusGetter(moduleName)
	if err != nil {
		return fmt.Errorf("get live helm release %q: %w", moduleName, err)
	}

	if !strings.EqualFold(liveStatus, "deployed") {
		return fmt.Errorf("helm release %q is %q, not deployed: monitor publish waits", moduleName, liveStatus)
	}

	st, err := p.alignState(moduleName, liveRevision)
	if err != nil {
		return err
	}

	if st == nil {
		// Nothing to publish (e.g. the module is no longer a Helm module).
		return nil
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	// Re-check against the registered monitor: a newer transaction may have
	// already published the live revision while we waited for the lock.
	if served := p.resourcesManager.MonitorRevision(moduleName); served != "" && compareRevisions(served, st.revision) >= 0 {
		p.logger.Info("monitor already serves the target revision",
			slog.String(pkg.LogKeyModule, moduleName),
			slog.String(pkg.LogKeyRevision, served))
		p.drop(moduleName)

		return nil
	}

	switch st.stage {
	case stageReleaseClaimed:
		if err := p.parseManifests(st); err != nil {
			return p.fail(st, err)
		}

		p.snapshotOldMonitor(st)

		st.stage = stageManifestReady
		p.logger.Debug("monitor publish: manifests parsed",
			slog.String(pkg.LogKeyModule, moduleName),
			slog.Int(pkg.LogKeyCount, len(st.manifests)))

	case stageManifestReady:
		// Old snapshot may be missing if the monitor appeared after claim.
		p.snapshotOldMonitor(st)
	}

	if st.stage == stageManifestReady {
		// Build and confirm the new monitor while the previous one keeps
		// serving. Parse/start failures leave the old monitor untouched and
		// only this phase is retried.
		prepared, err := p.resourcesManager.PrepareMonitor(ctx, &helm_resources_manager.ResourceMonitorConfig{
			ModuleName:       moduleName,
			Manifests:        st.manifests,
			DefaultNamespace: st.defaultNamespace,
			ReleaseRevision:  st.revision,
			HelmStatusGetter: p.statusGetter,
		})
		if err != nil {
			return p.fail(st, fmt.Errorf("prepare monitor for release revision %q: %w", st.revision, err))
		}

		st.prepared = prepared
		st.stage = stageMonitorReady
	}

	if st.stage == stageMonitorReady {
		// Atomically replace the old monitor. The swap refuses to downgrade
		// to an older revision, so a stale phase cannot overwrite a monitor
		// for a newer release.
		err := p.resourcesManager.SwapMonitor(moduleName, st.prepared)
		if err != nil {
			if errors.Is(err, helm_resources_manager.ErrMonitorRevisionStale) {
				// A newer monitor won. If it serves the live revision, finish;
				// otherwise re-anchor on the next attempt.
				if p.resourcesManager.MonitorRevision(moduleName) == liveRevision {
					p.drop(moduleName)

					return nil
				}

				st.prepared = nil
				st.stage = stageManifestReady

				return p.fail(st, err)
			}

			// Unexpected swap failure: make sure the previous watch is not
			// left without a monitor, then retry the replacement.
			p.restoreOldMonitor(st)
			st.prepared = nil
			st.stage = stageManifestReady

			return p.fail(st, fmt.Errorf("swap monitor for release revision %q: %w", st.revision, err))
		}

		st.stage = stageMonitorCommitted
	}

	// Committed: verify the registered monitor really serves the target
	// revision ("continue new watch" on retry).
	if st.stage == stageMonitorCommitted {
		served := p.resourcesManager.MonitorRevision(moduleName)
		switch {
		case served == st.revision:
			st.confirmed = true
			st.lastErr = nil
			p.logger.Info("monitor published for release",
				slog.String(pkg.LogKeyModule, moduleName),
				slog.String(pkg.LogKeyRevision, st.revision),
				slog.String(pkg.LogKeyChecksum, st.manifestDigest),
				slog.Bool("ranUpgrade", st.ranUpgrade),
				slog.Bool("confirmed", st.confirmed))
			p.drop(moduleName)

			return nil

		case served != "" && compareRevisions(served, st.revision) > 0:
			// A newer release monitor took over while committing; the old
			// phase must not overwrite it.
			p.logger.Info("newer monitor found after commit, keeping it",
				slog.String(pkg.LogKeyModule, moduleName),
				slog.String(pkg.LogKeyRevision, served))
			p.drop(moduleName)

			return nil

		default:
			// No monitor registered for the target revision: restore the
			// previous watch and retry publication from the manifest stage.
			p.restoreOldMonitor(st)
			st.prepared = nil
			st.stage = stageManifestReady

			return p.fail(st, fmt.Errorf("monitor for release revision %q is not registered after commit", st.revision))
		}
	}

	return nil
}

// alignState returns the transaction to execute, re-anchoring it when it is
// missing or targets a revision different from the live release.
func (p *MonitorPublisher) alignState(moduleName, liveRevision string) (*monitorPublishState, error) {
	p.mu.Lock()
	st := p.states[moduleName]
	p.mu.Unlock()

	if st == nil {
		// No registered transaction (discarded state, or re-entry after a
		// process restart): re-fetch the manifest for the live release.
		p.logger.Debug("no monitor publish transaction, fetching current release",
			slog.String(pkg.LogKeyModule, moduleName))

		return p.reanchor(moduleName)
	}

	st.mu.Lock()
	targetRevision := st.revision
	st.mu.Unlock()

	switch compareRevisions(liveRevision, targetRevision) {
	case 0:
		return st, nil
	case 1:
		// A subsequent config change produced a newer release: the old phase
		// must not overwrite the newer monitor.
		p.logger.Info("stale monitor publish transaction, re-anchoring to newer release",
			slog.String(pkg.LogKeyModule, moduleName),
			slog.String(pkg.LogKeyPrevious, targetRevision),
			slog.String(pkg.LogKeyRevision, liveRevision))
	default:
		// Live release is older than the registered target (rollback):
		// re-anchor to the release that is actually deployed.
		p.logger.Warn("live release is older than the monitor publish target, re-anchoring",
			slog.String(pkg.LogKeyModule, moduleName),
			slog.String(pkg.LogKeyPrevious, targetRevision),
			slog.String(pkg.LogKeyRevision, liveRevision))
	}

	// If a monitor already serves the live revision, there is nothing left
	// to publish and the stale transaction is discarded.
	if p.resourcesManager.MonitorRevision(moduleName) == liveRevision && liveRevision != "" {
		p.drop(moduleName)

		return nil, nil
	}

	return p.reanchor(moduleName)
}

// reanchor fetches the current release without any Helm mutation and starts a
// fresh transaction ("re-fetch manifest" recovery).
func (p *MonitorPublisher) reanchor(moduleName string) (*monitorPublishState, error) {
	if p.claimFetcher == nil {
		return nil, errors.New("monitor claim fetcher is not configured")
	}

	result, err := p.claimFetcher.FetchMonitorClaim(moduleName)
	if err != nil {
		return nil, fmt.Errorf("re-fetch manifest for module %q: %w", moduleName, err)
	}

	if result == nil {
		// Module is no longer a Helm chart: no monitor to publish.
		p.drop(moduleName)

		return nil, nil
	}

	st := &monitorPublishState{
		moduleName:       moduleName,
		revision:         result.Revision,
		manifestDigest:   result.ManifestChecksum,
		rendered:         result.RenderedManifests,
		defaultNamespace: result.DefaultNamespace,
		stage:            stageReleaseClaimed,
		updatedAt:        time.Now(),
	}

	p.mu.Lock()
	p.states[moduleName] = st
	p.mu.Unlock()

	return st, nil
}

func (p *MonitorPublisher) parseManifests(st *monitorPublishState) error {
	manifests, err := manifest.ListFromYamlDocs(st.rendered)
	if err != nil {
		return fmt.Errorf("parse release manifests for revision %q: %w", st.revision, err)
	}

	st.manifests = manifests

	return nil
}

// snapshotOldMonitor records the state of the monitor currently in service,
// so it can be restored if the replacement cannot be completed.
func (p *MonitorPublisher) snapshotOldMonitor(st *monitorPublishState) {
	if st.oldMonitor != nil {
		return
	}

	if monitor := p.resourcesManager.GetMonitor(st.moduleName); monitor != nil {
		snapshot := monitor.Snapshot()
		st.oldMonitor = &snapshot
	}
}

// restoreOldMonitor rebuilds the previous watch from the registered snapshot.
func (p *MonitorPublisher) restoreOldMonitor(st *monitorPublishState) {
	if st.oldMonitor == nil {
		return
	}

	if served := p.resourcesManager.MonitorRevision(st.moduleName); served != "" &&
		compareRevisions(served, st.oldMonitor.ReleaseRevision) >= 0 {
		return
	}

	if err := p.resourcesManager.RestoreMonitor(st.moduleName, *st.oldMonitor, p.statusGetter); err != nil {
		p.logger.Error("failed to restore previous monitor",
			slog.String(pkg.LogKeyModule, st.moduleName),
			slog.String(pkg.LogKeyRevision, st.oldMonitor.ReleaseRevision),
			log.Err(err))

		return
	}

	p.logger.Warn("restored previous monitor after interrupted replacement",
		slog.String(pkg.LogKeyModule, st.moduleName),
		slog.String(pkg.LogKeyRevision, st.oldMonitor.ReleaseRevision))
}

func (p *MonitorPublisher) fail(st *monitorPublishState, err error) error {
	st.lastErr = err
	st.updatedAt = time.Now()

	return err
}

func (p *MonitorPublisher) drop(moduleName string) {
	p.mu.Lock()
	delete(p.states, moduleName)
	p.mu.Unlock()
}

// compareRevisions compares Helm release revisions (integer strings).
// Returns 1 when a > b, -1 when a < b, 0 when equal. An empty revision is
// treated as the oldest possible.
func compareRevisions(a, b string) int {
	ai, errA := strconv.Atoi(a)
	bi, errB := strconv.Atoi(b)

	if errA == nil && errB == nil {
		switch {
		case ai > bi:
			return 1
		case ai < bi:
			return -1
		default:
			return 0
		}
	}

	return strings.Compare(a, b)
}
