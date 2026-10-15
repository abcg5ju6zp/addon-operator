package module_manager

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/flant/addon-operator/pkg/helm_resources_manager"
	mockhrm "github.com/flant/addon-operator/pkg/helm_resources_manager/test/mock"
	"github.com/flant/addon-operator/pkg/module_manager/models/modules"
)

const (
	mpTestModule     = "test-module"
	mpValidManifests = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: default\n"
	mpBadManifests   = "apiVersion: v1\nkind: ConfigMap\nmetadata: [unclosed"
)

func mpClaim(revision string) *modules.HelmRunResult {
	return &modules.HelmRunResult{
		Revision:          revision,
		ManifestChecksum:  "checksum-" + revision,
		RenderedManifests: mpValidManifests,
		DefaultNamespace:  "default",
	}
}

type fakeClaimFetcher struct {
	mu     sync.Mutex
	calls  int
	claims map[string]*modules.HelmRunResult
	err    error
}

func (f *fakeClaimFetcher) FetchMonitorClaim(moduleName string) (*modules.HelmRunResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++

	if f.err != nil {
		return nil, f.err
	}

	return f.claims[moduleName], nil
}

func (f *fakeClaimFetcher) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

type fakeReleaseStatus struct {
	revision string
	status   string
	err      error
}

func (s *fakeReleaseStatus) get(string) (string, string, error) {
	if s.err != nil {
		return "", "", s.err
	}

	return s.revision, s.status, nil
}

func newTestPublisher(t *testing.T, hrm helm_resources_manager.HelmResourcesManager, status *fakeReleaseStatus, fetcher *fakeClaimFetcher) *MonitorPublisher {
	t.Helper()

	return NewMonitorPublisher(hrm, fetcher, status.get, log.NewNop())
}

// TestMonitorPublisher_HappyPath: claim -> parse -> prepare -> swap -> confirm.
func TestMonitorPublisher_HappyPath(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	status := &fakeReleaseStatus{revision: "2", status: "deployed"}
	fetcher := &fakeClaimFetcher{}
	p := newTestPublisher(t, hrm, status, fetcher)

	p.Claim(mpTestModule, mpClaim("2"))
	require.True(t, p.HasTransaction(mpTestModule))

	require.NoError(t, p.Publish(context.Background(), mpTestModule))

	assert.Equal(t, "2", hrm.MonitorRevision(mpTestModule), "new monitor must serve revision 2")
	assert.False(t, p.HasTransaction(mpTestModule), "transaction is cleared after confirmation")
	assert.Equal(t, 1, hrm.PrepareCalls)
	assert.Equal(t, []string{"2"}, hrm.PreparedRevisions())
	assert.Equal(t, 1, hrm.SwapCalls)
	assert.Equal(t, 0, fetcher.Calls(), "no re-fetch for a fresh aligned claim")
}

// TestMonitorPublisher_ParseFailureRetriesOnlyPublishing: a bad manifest keeps
// the transaction in ReleaseClaimed and never invokes Helm (no re-fetch, no
// upgrade), prepare or swap. A fresh claim then succeeds.
func TestMonitorPublisher_ParseFailureRetriesOnlyPublishing(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	status := &fakeReleaseStatus{revision: "2", status: "deployed"}
	fetcher := &fakeClaimFetcher{}
	p := newTestPublisher(t, hrm, status, fetcher)

	bad := mpClaim("2")
	bad.RenderedManifests = mpBadManifests
	p.Claim(mpTestModule, bad)

	for range 2 {
		err := p.Publish(context.Background(), mpTestModule)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse release manifests")
	}

	assert.True(t, p.HasTransaction(mpTestModule), "transaction must survive a parse failure")
	assert.Equal(t, 0, hrm.PrepareCalls, "parse failure must not prepare a monitor")
	assert.Equal(t, 0, hrm.SwapCalls)
	assert.Equal(t, 0, fetcher.Calls(), "same-revision retry must not re-run Helm/render")
	assert.Equal(t, "", hrm.MonitorRevision(mpTestModule))

	p.mu.Lock()
	st := p.states[mpTestModule]
	p.mu.Unlock()
	require.NotNil(t, st)
	st.mu.Lock()
	assert.Equal(t, stageReleaseClaimed, st.stage)
	require.Error(t, st.lastErr)
	assert.Equal(t, "checksum-2", st.manifestDigest, "release revision and manifest digest stay registered")
	assert.Equal(t, "2", st.revision)
	st.mu.Unlock()

	// A new confirmed module run claims valid manifests for the same revision.
	p.Claim(mpTestModule, mpClaim("2"))
	require.NoError(t, p.Publish(context.Background(), mpTestModule))
	assert.Equal(t, "2", hrm.MonitorRevision(mpTestModule))
}

// TestMonitorPublisher_PrepareFailureKeepsOldMonitor: while the new monitor
// cannot start watching, the previous monitor keeps serving the old revision;
// retries only repeat monitor publishing and never touch Helm.
func TestMonitorPublisher_PrepareFailureKeepsOldMonitor(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	hrm.SetMonitorRevision(mpTestModule, "1")

	status := &fakeReleaseStatus{revision: "2", status: "deployed"}
	fetcher := &fakeClaimFetcher{}
	p := newTestPublisher(t, hrm, status, fetcher)

	p.Claim(mpTestModule, mpClaim("2"))

	hrm.PrepareErr = errors.New("transient: apiserver unavailable")

	for range 3 {
		err := p.Publish(context.Background(), mpTestModule)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "prepare monitor")
		assert.Equal(t, "1", hrm.MonitorRevision(mpTestModule), "old monitor must remain in service")
	}

	assert.Equal(t, 0, hrm.SwapCalls, "no swap while preparation fails")
	assert.Equal(t, 0, fetcher.Calls(), "no Helm re-run while preparation fails")
	assert.True(t, p.HasTransaction(mpTestModule))

	hrm.PrepareErr = nil
	require.NoError(t, p.Publish(context.Background(), mpTestModule))
	assert.Equal(t, "2", hrm.MonitorRevision(mpTestModule))
	assert.Equal(t, 4, hrm.PrepareCalls)
}

// TestMonitorPublisher_StaleTransactionNewerMonitorPresent: a subsequent
// config change already produced revision 3 with its monitor — the stale
// revision-2 transaction must not touch the newer monitor.
func TestMonitorPublisher_StaleTransactionNewerMonitorPresent(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	hrm.SetMonitorRevision(mpTestModule, "3")

	status := &fakeReleaseStatus{revision: "3", status: "deployed"}
	fetcher := &fakeClaimFetcher{}
	p := newTestPublisher(t, hrm, status, fetcher)

	p.Claim(mpTestModule, mpClaim("2"))

	require.NoError(t, p.Publish(context.Background(), mpTestModule))

	assert.Equal(t, "3", hrm.MonitorRevision(mpTestModule), "newer monitor must not be overwritten")
	assert.False(t, p.HasTransaction(mpTestModule))
	assert.Equal(t, 0, hrm.PrepareCalls)
	assert.Equal(t, 0, hrm.SwapCalls)
	assert.Equal(t, 0, fetcher.Calls())
}

// TestMonitorPublisher_StaleTransactionReanchors: live release moved to a
// newer revision but no monitor serves it yet — re-fetch the manifest and
// publish for the newer revision.
func TestMonitorPublisher_StaleTransactionReanchors(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	status := &fakeReleaseStatus{revision: "3", status: "deployed"}
	fetcher := &fakeClaimFetcher{claims: map[string]*modules.HelmRunResult{
		mpTestModule: mpClaim("3"),
	}}
	p := newTestPublisher(t, hrm, status, fetcher)

	p.Claim(mpTestModule, mpClaim("2"))

	require.NoError(t, p.Publish(context.Background(), mpTestModule))

	assert.Equal(t, "3", hrm.MonitorRevision(mpTestModule))
	assert.Equal(t, []string{"3"}, hrm.PreparedRevisions(), "monitor must be built from re-fetched manifests")
	assert.Equal(t, 1, fetcher.Calls())
	assert.False(t, p.HasTransaction(mpTestModule))
}

// TestMonitorPublisher_CommittedRetryContinuesNewWatch: re-entering Publish
// ("restart") after the swap committed only verifies the registered monitor
// and completes, without preparing or swapping again.
func TestMonitorPublisher_CommittedRetryContinuesNewWatch(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	hrm.SetMonitorRevision(mpTestModule, "2")

	status := &fakeReleaseStatus{revision: "2", status: "deployed"}
	fetcher := &fakeClaimFetcher{}
	p := newTestPublisher(t, hrm, status, fetcher)

	p.Claim(mpTestModule, mpClaim("2"))

	require.NoError(t, p.Publish(context.Background(), mpTestModule))
	assert.Equal(t, 0, hrm.SwapCalls, "already committed monitor must not be swapped again")
	assert.Equal(t, 0, hrm.PrepareCalls)
	assert.False(t, p.HasTransaction(mpTestModule))
}

// TestMonitorPublisher_MissingTransactionRefetches: no registered state
// (process restart / discarded state) re-fetches the current release manifest
// without any Helm mutation.
func TestMonitorPublisher_MissingTransactionRefetches(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	status := &fakeReleaseStatus{revision: "4", status: "deployed"}
	fetcher := &fakeClaimFetcher{claims: map[string]*modules.HelmRunResult{
		mpTestModule: mpClaim("4"),
	}}
	p := newTestPublisher(t, hrm, status, fetcher)

	require.NoError(t, p.Publish(context.Background(), mpTestModule))

	assert.Equal(t, "4", hrm.MonitorRevision(mpTestModule))
	assert.Equal(t, 1, fetcher.Calls())
	assert.False(t, p.HasTransaction(mpTestModule))
}

// TestMonitorPublisher_MissingTransactionNonHelm: re-fetch reports the module
// is not a Helm chart (nil claim): nothing to publish.
func TestMonitorPublisher_MissingTransactionNonHelm(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	status := &fakeReleaseStatus{revision: "4", status: "deployed"}
	fetcher := &fakeClaimFetcher{claims: map[string]*modules.HelmRunResult{}}
	p := newTestPublisher(t, hrm, status, fetcher)

	require.NoError(t, p.Publish(context.Background(), mpTestModule))
	assert.Equal(t, 0, hrm.PrepareCalls)
}

// TestMonitorPublisher_FetchErrorRetries: re-fetch failure is surfaced and no
// monitor operation happens.
func TestMonitorPublisher_FetchErrorRetries(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	status := &fakeReleaseStatus{revision: "4", status: "deployed"}
	fetcher := &fakeClaimFetcher{err: errors.New("render failed")}
	p := newTestPublisher(t, hrm, status, fetcher)

	err := p.Publish(context.Background(), mpTestModule)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "re-fetch manifest")
	assert.Equal(t, 0, hrm.PrepareCalls)
}

// TestMonitorPublisher_NonDeployedReleaseWaits: a live release in a non-deployed
// status fails publication before any monitor operation.
func TestMonitorPublisher_NonDeployedReleaseWaits(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	status := &fakeReleaseStatus{revision: "2", status: "failed"}
	fetcher := &fakeClaimFetcher{}
	p := newTestPublisher(t, hrm, status, fetcher)

	p.Claim(mpTestModule, mpClaim("2"))

	err := p.Publish(context.Background(), mpTestModule)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not deployed")
	assert.Equal(t, 0, hrm.PrepareCalls)
	assert.True(t, p.HasTransaction(mpTestModule), "transaction retained for retry")
}

// TestMonitorPublisher_Discard removes the registered transaction.
func TestMonitorPublisher_Discard(t *testing.T) {
	hrm := &mockhrm.MockHelmResourcesManager{}
	status := &fakeReleaseStatus{revision: "2", status: "deployed"}
	fetcher := &fakeClaimFetcher{}
	p := newTestPublisher(t, hrm, status, fetcher)

	p.Claim(mpTestModule, mpClaim("2"))
	p.Discard(mpTestModule)
	assert.False(t, p.HasTransaction(mpTestModule))
}

// interruptedSwapHRM simulates a process exit in the middle of the swap:
// SwapMonitor stops the old monitor without registering the new one and
// returns an error. GetMonitor exposes the old monitor until then.
type interruptedSwapHRM struct {
	*mockhrm.MockHelmResourcesManager

	mu           sync.Mutex
	revision     string
	oldMonitor   *helm_resources_manager.ResourcesMonitor
	swapCalls    int
	restoreCalls int
	prepareCalls int
}

func (h *interruptedSwapHRM) PrepareMonitor(_ context.Context, cfg *helm_resources_manager.ResourceMonitorConfig) (*helm_resources_manager.ResourcesMonitor, error) {
	h.mu.Lock()
	h.prepareCalls++
	h.mu.Unlock()

	cfg.KubeClient = nil
	cfg.Cache = nil
	cfg.Logger = log.NewNop()

	return helm_resources_manager.NewResourcesMonitor(context.Background(), cfg), nil
}

func (h *interruptedSwapHRM) SwapMonitor(_ string, _ *helm_resources_manager.ResourcesMonitor) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.swapCalls++
	h.revision = "" // old monitor detached, new one never registered

	return errors.New("interrupted during replacement")
}

func (h *interruptedSwapHRM) RestoreMonitor(_ string, snapshot helm_resources_manager.MonitorSnapshot, _ func(string) (string, string, error)) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.restoreCalls++
	h.revision = snapshot.ReleaseRevision

	return nil
}

func (h *interruptedSwapHRM) MonitorRevision(string) string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.revision
}

func (h *interruptedSwapHRM) GetMonitor(string) *helm_resources_manager.ResourcesMonitor {
	return h.oldMonitor
}

// TestMonitorPublisher_RestoreOldMonitorAfterInterruptedSwap: when a swap
// leaves no registered monitor, the previous watch is restored from the
// registered snapshot and the transaction rolls back to retry publication.
func TestMonitorPublisher_RestoreOldMonitorAfterInterruptedSwap(t *testing.T) {
	oldMonitor := helm_resources_manager.NewResourcesMonitor(context.Background(), &helm_resources_manager.ResourceMonitorConfig{
		ModuleName:       mpTestModule,
		ReleaseRevision:  "1",
		DefaultNamespace: "default",
		Logger:           log.NewNop(),
	})

	hrm := &interruptedSwapHRM{
		MockHelmResourcesManager: &mockhrm.MockHelmResourcesManager{},
		revision:                 "1",
		oldMonitor:               oldMonitor,
	}
	status := &fakeReleaseStatus{revision: "2", status: "deployed"}
	fetcher := &fakeClaimFetcher{}
	p := newTestPublisher(t, hrm, status, fetcher)

	p.Claim(mpTestModule, mpClaim("2"))

	err := p.Publish(context.Background(), mpTestModule)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "interrupted during replacement")

	assert.Equal(t, 1, hrm.prepareCalls)
	assert.Equal(t, 1, hrm.swapCalls, "swap was attempted once")
	assert.Equal(t, 1, hrm.restoreCalls, "old monitor must be restored")
	assert.Equal(t, "1", hrm.MonitorRevision(mpTestModule), "old revision watch is back in service")
	assert.True(t, p.HasTransaction(mpTestModule), "transaction stays for retry")

	p.mu.Lock()
	st := p.states[mpTestModule]
	p.mu.Unlock()
	require.NotNil(t, st)
	st.mu.Lock()
	assert.Equal(t, stageManifestReady, st.stage, "transaction rolls back to manifest stage")
	require.NotNil(t, st.oldMonitor)
	assert.Equal(t, "1", st.oldMonitor.ReleaseRevision, "old monitor state is recorded")
	assert.Nil(t, st.prepared)
	require.Error(t, st.lastErr)
	assert.False(t, st.confirmed)
	st.mu.Unlock()
}

func TestCompareRevisions(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"3", "2", 1},
		{"10", "9", 1},
		{"2", "2", 0},
		{"", "1", -1},
		{"1", "", 1},
		{"9", "10", -1},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.want, compareRevisions(tt.a, tt.b), "%s vs %s", tt.a, tt.b)
	}
}
