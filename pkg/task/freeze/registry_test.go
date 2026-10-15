package freeze

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kemtypes "github.com/flant/shell-operator/pkg/kube_events_manager/types"
)

type fakeControls struct {
	mu sync.Mutex

	pauses     []string
	resumes    []string
	reconciled []string
	replayed   [][]kemtypes.KubeEvent

	modules      map[string]bool
	kubeMonitors map[string]bool
}

func newFakeControls() *fakeControls {
	return &fakeControls{
		modules:      map[string]bool{"alpha": true, "beta": true, "gamma": true},
		kubeMonitors: map[string]bool{"m-alpha": true, "m-beta": true, "m-shared": true},
	}
}

func (f *fakeControls) PauseModuleMonitor(moduleName string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pauses = append(f.pauses, moduleName)
}

func (f *fakeControls) ResumeModuleMonitor(moduleName string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.resumes = append(f.resumes, moduleName)
}

func (f *fakeControls) ModuleMonitorExists(moduleName string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.modules[moduleName]
}

func (f *fakeControls) ReconcileModule(_ context.Context, moduleName string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.reconciled = append(f.reconciled, moduleName)
}

func (f *fakeControls) KubeMonitorExists(monitorID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.kubeMonitors[monitorID]
}

func (f *fakeControls) ReplayKubeEvents(events []kemtypes.KubeEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.replayed = append(f.replayed, events)
}

func (f *fakeControls) snapshot() (pauses, resumes, reconciled []string, replayed [][]kemtypes.KubeEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.pauses...),
		append([]string(nil), f.resumes...),
		append([]string(nil), f.reconciled...),
		append([][]kemtypes.KubeEvent(nil), f.replayed...)
}

func kubeEvent(monitorID string, op kemtypes.WatchEventType, resourceID string) kemtypes.KubeEvent {
	obj := kemtypes.ObjectAndFilterResult{}
	obj.Metadata.ResourceId = resourceID

	return kemtypes.KubeEvent{
		MonitorId:   monitorID,
		Type:        kemtypes.TypeEvent,
		WatchEvents: []kemtypes.WatchEventType{op},
		Objects:     []kemtypes.ObjectAndFilterResult{obj},
	}
}

func syncEvent(monitorID string) kemtypes.KubeEvent {
	return kemtypes.KubeEvent{MonitorId: monitorID, Type: kemtypes.TypeSynchronization}
}

func exec(taskID, module string, monitorIDs ...string) Execution {
	scope := Scope{}
	if module != "" {
		scope.Modules = []string{module}
	}

	scope.MonitorIDs = monitorIDs

	return Execution{TaskID: taskID, Queue: "main", Scope: scope}
}

func TestAcquireReleaseSingleOwner(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	o := registry.Acquire(exec("t1", "alpha", "m-alpha"))

	assert.True(t, registry.IsModuleFrozen("alpha"))
	assert.True(t, registry.IsMonitorFrozen("m-alpha"))

	pauses, _, _, _ := controls.snapshot()
	assert.Equal(t, []string{"alpha"}, pauses)

	// Events of the frozen monitor are held.
	assert.True(t, registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventModified, "res-1")))
	// Events of another monitor pass through.
	assert.False(t, registry.HoldKubeEvent(kubeEvent("m-beta", kemtypes.WatchEventModified, "res-2")))

	o.Release(context.Background(), OutcomeSucceeded)

	assert.False(t, registry.IsModuleFrozen("alpha"))
	assert.False(t, registry.IsMonitorFrozen("m-alpha"))

	_, resumes, reconciled, replayed := controls.snapshot()
	assert.Equal(t, []string{"alpha"}, resumes)
	assert.Equal(t, []string{"alpha"}, reconciled)

	require.Len(t, replayed, 1)
	require.Len(t, replayed[0], 1)
	assert.Equal(t, "res-1", replayed[0][0].Objects[0].Metadata.ResourceId)
}

func TestParallelOwnersLastReleaseThaws(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	// Two parallel executions affect the same module and monitor.
	o1 := registry.Acquire(exec("t1", "alpha", "m-shared"))
	o2 := registry.Acquire(exec("t2", "alpha", "m-shared"))

	pauses, _, _, _ := controls.snapshot()
	assert.Equal(t, []string{"alpha"}, pauses, "physical pause happens only on the first owner")

	assert.True(t, registry.HoldKubeEvent(kubeEvent("m-shared", kemtypes.WatchEventAdded, "res-1")))

	// First execution fails and releases only its own ownership: the watch
	// stays frozen for the second execution.
	o1.Release(context.Background(), OutcomeFailed)

	assert.True(t, registry.IsModuleFrozen("alpha"), "module stays frozen while the second owner is active")
	assert.True(t, registry.IsMonitorFrozen("m-shared"))

	_, resumes, reconciled, replayed := controls.snapshot()
	assert.Empty(t, resumes)
	assert.Empty(t, reconciled)
	assert.Empty(t, replayed)

	// Events keep accumulating while the remaining execution runs.
	assert.True(t, registry.HoldKubeEvent(kubeEvent("m-shared", kemtypes.WatchEventModified, "res-2")))

	// Canceled second execution thaws the scope.
	o2.Release(context.Background(), OutcomeCanceled)

	assert.False(t, registry.IsModuleFrozen("alpha"))
	assert.False(t, registry.IsMonitorFrozen("m-shared"))

	_, resumes, reconciled, replayed = controls.snapshot()
	assert.Equal(t, []string{"alpha"}, resumes, "resume happens once, on the last release")
	assert.Equal(t, []string{"alpha"}, reconciled)
	require.Len(t, replayed, 1)
	assert.Len(t, replayed[0], 2, "facts accumulated by both executions are replayed together")
}

func TestRetryAcquiresFreshOwnership(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	o1 := registry.Acquire(exec("t1", "alpha"))
	o1.Release(context.Background(), OutcomeRetrying)

	_, resumes, reconciled, _ := controls.snapshot()
	require.Len(t, resumes, 1)
	require.Len(t, reconciled, 1)

	// Retry of the same task is a new attempt with a fresh ownership.
	o2 := registry.Acquire(Execution{TaskID: "t1", Attempt: 2, Scope: Scope{Modules: []string{"alpha"}}})
	assert.True(t, registry.IsModuleFrozen("alpha"))
	o2.Release(context.Background(), OutcomeSucceeded)

	pauses, resumes, _, _ := controls.snapshot()
	assert.Equal(t, []string{"alpha", "alpha"}, pauses)
	assert.Equal(t, []string{"alpha", "alpha"}, resumes)
}

func TestIndependentModuleScopes(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	oa := registry.Acquire(exec("ta", "alpha", "m-alpha"))
	ob := registry.Acquire(exec("tb", "beta", "m-beta"))

	oa.Release(context.Background(), OutcomeSucceeded)

	assert.True(t, registry.IsModuleFrozen("beta"), "unrelated parallel execution is unaffected")
	assert.False(t, registry.IsModuleFrozen("alpha"))

	_, resumes, _, _ := controls.snapshot()
	assert.Equal(t, []string{"alpha"}, resumes)

	ob.Release(context.Background(), OutcomeSucceeded)
	_, resumes, _, _ = controls.snapshot()
	assert.Equal(t, []string{"alpha", "beta"}, resumes)
}

func TestReleaseIsIdempotent(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	o := registry.Acquire(exec("t1", "alpha"))
	o.Release(context.Background(), OutcomeSucceeded)
	o.Release(context.Background(), OutcomeSucceeded) // panic safety net

	_, resumes, reconciled, _ := controls.snapshot()
	assert.Len(t, resumes, 1)
	assert.Len(t, reconciled, 1)
}

func TestEmptyScopeChangesNothing(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	o := registry.Acquire(exec("t1", ""))
	o.Release(context.Background(), OutcomeSucceeded)

	pauses, resumes, reconciled, _ := controls.snapshot()
	assert.Empty(t, pauses)
	assert.Empty(t, resumes)
	assert.Empty(t, reconciled)
}

func TestHeldEventsCompactedToLatestFacts(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	o := registry.Acquire(exec("t1", "alpha", "m-alpha"))

	// add -> delete while frozen: resource never existed in pre-freeze
	// snapshot, must be dropped.
	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventAdded, "ephemeral"))
	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventDeleted, "ephemeral"))

	// add -> modified: stays Added with the latest payload.
	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventAdded, "created"))
	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventModified, "created"))

	// modified -> deleted: stays Deleted.
	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventModified, "removed"))
	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventDeleted, "removed"))

	// deleted -> re-added: existed before freeze and exists again -> Modified.
	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventDeleted, "recreated"))
	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventAdded, "recreated"))

	o.Release(context.Background(), OutcomeSucceeded)

	_, _, _, replayed := controls.snapshot()
	require.Len(t, replayed, 1)

	events := replayed[0]
	require.Len(t, events, 3, "ephemeral resource must be compacted out")

	got := map[string]kemtypes.WatchEventType{}
	order := make([]string, 0, 3)
	for _, ev := range events {
		id := ev.Objects[0].Metadata.ResourceId
		got[id] = ev.WatchEvents[0]
		order = append(order, id)
	}

	assert.Equal(t, kemtypes.WatchEventAdded, got["created"], "add then mod keeps Added label")
	assert.Equal(t, kemtypes.WatchEventDeleted, got["removed"])
	assert.Equal(t, kemtypes.WatchEventModified, got["recreated"], "delete then add becomes Modified")

	// First-seen order of resources is preserved.
	assert.Equal(t, []string{"created", "removed", "recreated"}, order)
}

func TestSynchronizationEventReplayedFirst(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	o := registry.Acquire(exec("t1", "alpha", "m-alpha"))

	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventModified, "res-1"))
	registry.HoldKubeEvent(syncEvent("m-alpha"))

	o.Release(context.Background(), OutcomeSucceeded)

	_, _, _, replayed := controls.snapshot()
	require.Len(t, replayed, 1)
	require.Len(t, replayed[0], 2)
	assert.Equal(t, kemtypes.TypeSynchronization, replayed[0][0].Type, "synchronization refreshes snapshots first")
	assert.Equal(t, kemtypes.TypeEvent, replayed[0][1].Type)
}

func TestThawDropEventsWhenMonitorDisappeared(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	o := registry.Acquire(exec("t1", "alpha", "m-alpha"))
	registry.HoldKubeEvent(kubeEvent("m-alpha", kemtypes.WatchEventModified, "res-1"))

	// Monitor is stopped while the execution is running.
	controls.kubeMonitors["m-alpha"] = false

	o.Release(context.Background(), OutcomeSucceeded)

	_, _, _, replayed := controls.snapshot()
	assert.Empty(t, replayed, "late facts of a missing monitor must not resurrect state")
}

func TestThawSkipResumeWhenModuleMonitorDisappeared(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	o := registry.Acquire(exec("t1", "alpha"))

	controls.modules["alpha"] = false // unmanaged release / module deleted

	o.Release(context.Background(), OutcomeSucceeded)

	_, resumes, reconciled, _ := controls.snapshot()
	assert.Empty(t, resumes)
	assert.Empty(t, reconciled)
}

func TestRebuildReleasesOrphansAndReacquiresRunning(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	// Existing in-process state: alpha is frozen by a running task; gamma is
	// an orphaned pause left without an owner.
	running := registry.Acquire(exec("run-1", "alpha", "m-alpha"))
	require.NotNil(t, running)

	registry.mu.Lock()
	registry.modules["gamma"] = &moduleFreeze{epoch: 1, owners: map[uint64]*Ownership{}}
	registry.monitors["m-ghost"] = &monitorFreeze{
		epoch:  1,
		owners: map[uint64]*Ownership{},
		facts:  make(map[string]*heldFact),
	}
	registry.mu.Unlock()

	// Physical pause state reports alpha (running) and gamma (orphan).
	pausedModules := []string{"alpha", "gamma"}

	// On rebuild only the alpha task is still processing.
	report := registry.Rebuild(context.Background(),
		[]Execution{exec("run-1", "alpha", "m-alpha")},
		pausedModules)

	assert.Equal(t, []string{"run-1"}, report.Reacquired)
	assert.Equal(t, []string{"gamma"}, report.ResumedModules)
	assert.Equal(t, []string{"m-ghost"}, report.DroppedMonitors)

	assert.True(t, registry.IsModuleFrozen("alpha"))
	assert.False(t, registry.IsModuleFrozen("gamma"))
	assert.False(t, registry.IsMonitorFrozen("m-ghost"))

	_, resumes, reconciled, _ := controls.snapshot()
	assert.Equal(t, []string{"gamma"}, resumes, "orphan monitor resumed")
	assert.Equal(t, []string{"gamma"}, reconciled, "orphan monitor reconciled against latest facts")
}

func TestRebuildFreshProcessOnlyOrphans(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	// Fresh registry state, but physical monitors report a paused module
	// left by the killed process.
	report := registry.Rebuild(context.Background(), nil, []string{"beta"})

	assert.Empty(t, report.Reacquired)
	assert.Equal(t, []string{"beta"}, report.ResumedModules)

	_, resumes, reconciled, _ := controls.snapshot()
	assert.Equal(t, []string{"beta"}, resumes)
	assert.Equal(t, []string{"beta"}, reconciled)
	assert.False(t, registry.IsModuleFrozen("beta"))
}

func TestRebuildDoesNotDoublePauseAlreadyFrozenModule(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	// alpha is logically and physically frozen by a running task.
	running := registry.Acquire(exec("run-1", "alpha"))

	// Rebuild with the same execution still processing: ownership is
	// replaced but physical pause must not be counted twice.
	registry.Rebuild(context.Background(),
		[]Execution{exec("run-1", "alpha")},
		[]string{"alpha"})

	pauses, _, _, _ := controls.snapshot()
	assert.Equal(t, []string{"alpha"}, pauses)

	running.Release(context.Background(), OutcomeSucceeded)

	_, resumes, _, _ := controls.snapshot()
	assert.Equal(t, []string{"alpha"}, resumes)
	assert.False(t, registry.IsModuleFrozen("alpha"))
}

func TestConcurrentAcquireRelease(t *testing.T) {
	controls := newFakeControls()
	registry := NewRegistry(controls, nil)

	const goroutines = 50

	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			module := "alpha"
			monitor := "m-shared"
			if i%2 == 0 {
				module = "beta"
				monitor = "m-beta"
			}

			o := registry.Acquire(exec(
				"task-"+string(rune('a'+i%5)),
				module,
				monitor,
			))
			registry.HoldKubeEvent(kubeEvent(monitor, kemtypes.WatchEventModified, "res"))
			o.Release(context.Background(), OutcomeSucceeded)
		}(i)
	}

	wg.Wait()

	assert.False(t, registry.IsModuleFrozen("alpha"))
	assert.False(t, registry.IsModuleFrozen("beta"))
	assert.False(t, registry.IsMonitorFrozen("m-shared"))
	assert.False(t, registry.IsMonitorFrozen("m-beta"))
}
