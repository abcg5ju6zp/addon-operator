// Package freeze implements scoped ownership of resource-watch freezes
// during hook and module executions.
//
// A hook execution may mutate module values, a Helm release or the
// synchronization state while Kubernetes resource watches keep emitting
// events. While such an execution is in flight, the resource events of the
// modules and monitors it actually affects must not enter task queues.
//
// Several executions can overlap: a hook task running in a parallel queue
// while the same module runs its Helm phase in another queue, or nested
// converge runs. Every execution acquires its OWN ownership references for
// its scope. The first owner of a module or monitor freezes the watch; the
// watch is resumed only after the LAST still-valid owner releases its
// references (on completion, failure, cancellation or retry). Each release
// therefore only removes the references of that particular attempt.
//
// When a monitor thaws, the Kubernetes facts buffered while it was frozen
// are compacted to the latest per-resource state and replayed through the
// normal task-creation path. Events belonging to monitors that disappeared
// while frozen are dropped instead of resurrecting stale state. When a
// module thaws, the control layer re-reads the latest cluster facts instead
// of trusting late results of finished executions.
package freeze

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/deckhouse/deckhouse/pkg/log"

	kemtypes "github.com/flant/shell-operator/pkg/kube_events_manager/types"
)

// Outcome describes why an execution released its ownership.
type Outcome string

const (
	// OutcomeSucceeded means the execution finished successfully.
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeFailed means the execution failed and its task is retried.
	OutcomeFailed Outcome = "failed"
	// OutcomeCanceled means the execution (or its parent parallel run) was
	// canceled.
	OutcomeCanceled Outcome = "canceled"
	// OutcomeRetrying means the execution is repeated without a terminal
	// result (queue Repeat/Keep semantics) and will acquire a fresh attempt.
	OutcomeRetrying Outcome = "retrying"
)

// Scope is the set of modules and Kubernetes monitors an execution may
// affect. A module scope freezes the module Helm resources monitor; monitor
// IDs scope the ingestion gate for Kubernetes binding events. Empty scope
// elements are ignored, so executions that do not touch resource watches
// (schedule-only hooks, modules without a monitor) keep the original,
// unfrozen behavior.
type Scope struct {
	// Modules contains module names whose Helm resources monitor is frozen.
	Modules []string
	// MonitorIDs contains IDs of Kubernetes binding monitors whose events
	// must not enter queues while the execution is in flight.
	MonitorIDs []string
}

// Execution describes one attempt of a queue task that acquires freeze
// ownership. TaskID stays stable across retries of the same queue task,
// Attempt distinguishes consecutive attempts and is used for fencing logs.
type Execution struct {
	TaskID  string
	Queue   string
	Attempt uint64
	// Reason is a short human-readable marker (e.g. "module-hook-run").
	Reason string
	Scope  Scope
}

// Controls applies the physical effects of a freeze. Implementations must
// be safe for concurrent use. Methods are called by the registry outside of
// its internal lock, so implementations may re-enter the registry.
type Controls interface {
	// PauseModuleMonitor stops the absent-resource callback of a module
	// monitor. Calls for overlapping executions must be reference counted.
	PauseModuleMonitor(moduleName string)
	// ResumeModuleMonitor releases one pause reference of a module monitor.
	ResumeModuleMonitor(moduleName string)
	// ModuleMonitorExists reports whether a live module monitor exists.
	ModuleMonitorExists(moduleName string) bool
	// ReconcileModule re-reads the latest cluster facts of a module after
	// its freeze thaws and enqueues repair work if resources are still
	// absent. It must not make decisions based on stale, pre-freeze results.
	ReconcileModule(ctx context.Context, moduleName string)
	// KubeMonitorExists reports whether a live Kubernetes binding monitor
	// exists. Held events of missing monitors are dropped on thaw.
	KubeMonitorExists(monitorID string) bool
	// ReplayKubeEvents feeds the events accumulated while a monitor was
	// frozen back through the normal event-to-task path, preserving order.
	ReplayKubeEvents(events []kemtypes.KubeEvent)
}

// RebuildReport summarizes what a Registry.Rebuild pass did. It is mainly
// used for startup logging and tests.
type RebuildReport struct {
	// Reacquired is the list of task IDs for which ownership was rebuilt.
	Reacquired []string
	// ResumedModules is the list of module monitors found paused without a
	// matching live execution; they were resumed and reconciled.
	ResumedModules []string
	// DroppedMonitors is the list of Kubernetes monitor IDs whose frozen
	// state was discarded because no live execution owns them anymore.
	DroppedMonitors []string
}

// Registry tracks freeze ownership for modules and Kubernetes monitors.
type Registry struct {
	controls Controls
	logger   *log.Logger

	mu       sync.Mutex
	seq      uint64
	modules  map[string]*moduleFreeze
	monitors map[string]*monitorFreeze
}

// Ownership is the set of freeze references acquired by one execution
// attempt. Releasing it removes exactly those references.
type Ownership struct {
	id       uint64
	exec     Execution
	reg      *Registry
	released atomic.Bool
}

type moduleFreeze struct {
	// epoch is bumped on every freeze -> thaw transition, so logs and
	// callers can distinguish consecutive freeze generations.
	epoch  uint64
	owners map[uint64]*Ownership
}

type monitorFreeze struct {
	epoch  uint64
	owners map[uint64]*Ownership
	// order preserves first-seen order of resource IDs while facts stores
	// the latest accumulated fact per resource.
	order []string
	facts map[string]*heldFact
	// raw holds events without per-object payload (Synchronization or
	// synthetic events); they are replayed before incremental facts.
	raw []kemtypes.KubeEvent
}

type heldFact struct {
	firstOp kemtypes.WatchEventType
	lastOp  kemtypes.WatchEventType
	event   kemtypes.KubeEvent
}

// NewRegistry creates a freeze ownership registry.
func NewRegistry(controls Controls, logger *log.Logger) *Registry {
	if logger == nil {
		logger = log.NewLogger()
	}

	return &Registry{
		controls: controls,
		logger:   logger.Named("freeze-registry"),
		modules:  make(map[string]*moduleFreeze),
		monitors: make(map[string]*monitorFreeze),
	}
}

// Acquire registers ownership of the execution scope. The first owner of a
// module or monitor performs the physical freeze; overlapping owners only
// increment the reference count. The returned Ownership MUST be released
// exactly once per terminal path (success, failure, cancel or retry);
// Release is idempotent so a deferred safety release is harmless.
func (r *Registry) Acquire(exec Execution) *Ownership {
	modules := dedupe(exec.Scope.Modules)
	monitorIDs := dedupe(exec.Scope.MonitorIDs)

	r.mu.Lock()
	r.seq++

	o := &Ownership{
		id:   r.seq,
		exec: exec,
		reg:  r,
	}

	frozenModules := make([]string, 0, len(modules))

	for _, moduleName := range modules {
		mf := r.modules[moduleName]
		if mf == nil {
			mf = &moduleFreeze{epoch: 1, owners: make(map[uint64]*Ownership)}
			r.modules[moduleName] = mf
			frozenModules = append(frozenModules, moduleName)
		}

		mf.owners[o.id] = o
	}

	for _, monitorID := range monitorIDs {
		kf := r.monitors[monitorID]
		if kf == nil {
			kf = &monitorFreeze{
				epoch:  1,
				owners: make(map[uint64]*Ownership),
				facts:  make(map[string]*heldFact),
			}
			r.monitors[monitorID] = kf
		}

		kf.owners[o.id] = o
	}

	r.mu.Unlock()

	// Pause outside of the registry lock: the control may be slow (a Helm
	// status call) or re-enter the registry.
	for _, moduleName := range frozenModules {
		r.controls.PauseModuleMonitor(moduleName)
	}

	r.logger.Debug("freeze ownership acquired",
		slog.Uint64("ownership_id", o.id),
		slog.String("task_id", exec.TaskID),
		slog.String("queue", exec.Queue),
		slog.Uint64("attempt", exec.Attempt),
		slog.String("reason", exec.Reason),
		slog.Any("modules", modules),
		slog.Any("monitor_ids", monitorIDs))

	return o
}

// Execution returns the execution metadata of the ownership.
func (o *Ownership) Execution() Execution {
	return o.exec
}

// Released reports whether Release was already called.
func (o *Ownership) Released() bool {
	return o.released.Load()
}

// Release removes the ownership references from every scoped key. Keys
// whose last valid owner is released are thawed: module monitors resume and
// reconcile against the latest cluster facts, Kubernetes monitors replay
// their compacted facts. It is safe to call concurrently and more than once.
func (o *Ownership) Release(ctx context.Context, outcome Outcome) {
	if !o.released.CompareAndSwap(false, true) {
		return
	}

	r := o.reg

	thawedModules := make([]string, 0)
	thawedMonitors := make(map[string][]kemtypes.KubeEvent)

	r.mu.Lock()

	for _, moduleName := range dedupe(o.exec.Scope.Modules) {
		mf := r.modules[moduleName]
		if mf == nil {
			continue
		}

		if _, ok := mf.owners[o.id]; !ok {
			continue
		}

		delete(mf.owners, o.id)

		if len(mf.owners) == 0 {
			mf.epoch++
			thawedModules = append(thawedModules, moduleName)
			delete(r.modules, moduleName)
		}
	}

	for _, monitorID := range dedupe(o.exec.Scope.MonitorIDs) {
		kf := r.monitors[monitorID]
		if kf == nil {
			continue
		}

		if _, ok := kf.owners[o.id]; !ok {
			continue
		}

		delete(kf.owners, o.id)

		if len(kf.owners) == 0 {
			kf.epoch++
			thawedMonitors[monitorID] = r.compactLocked(kf)
			delete(r.monitors, monitorID)
		}
	}

	r.mu.Unlock()

	r.logger.Debug("freeze ownership released",
		slog.Uint64("ownership_id", o.id),
		slog.String("task_id", o.exec.TaskID),
		slog.String("queue", o.exec.Queue),
		slog.String("outcome", string(outcome)),
		slog.Any("thawed_modules", thawedModules),
		slog.Any("thawed_monitors", monitorKeys(thawedMonitors)))

	for _, moduleName := range thawedModules {
		// Another execution may have re-frozen the module in the window
		// between unlock and this point. It now owns the resume/reconcile.
		if r.IsModuleFrozen(moduleName) {
			r.logger.Debug("module re-frozen during thaw, hand over resume and reconcile",
				slog.String("module", moduleName))

			continue
		}

		// The monitor may have been stopped while the execution was running
		// (unmanaged release, module deletion, reload-all). Nothing to
		// resume or reconcile in that case.
		if !r.controls.ModuleMonitorExists(moduleName) {
			r.logger.Debug("skip reconcile after thaw: module monitor does not exist",
				slog.String("module", moduleName))

			continue
		}

		r.controls.ResumeModuleMonitor(moduleName)
		r.controls.ReconcileModule(ctx, moduleName)
	}

	for monitorID, events := range thawedMonitors {
		// Re-frozen before replay: hand the accumulated facts to the new
		// freeze generation instead of bypassing its gate.
		if r.IsMonitorFrozen(monitorID) {
			r.absorbEvents(monitorID, events)

			r.logger.Debug("monitor re-frozen during thaw, facts absorbed by the new freeze",
				slog.String("monitor_id", monitorID),
				slog.Int("events", len(events)))

			continue
		}

		// A monitor removed or recreated while frozen must not receive late,
		// stale facts from the old generation.
		if !r.controls.KubeMonitorExists(monitorID) {
			r.logger.Debug("drop held events after thaw: kube monitor does not exist",
				slog.String("monitor_id", monitorID),
				slog.Int("dropped_events", len(events)))

			continue
		}

		if len(events) == 0 {
			continue
		}

		r.logger.Debug("replay held kube events after thaw",
			slog.String("monitor_id", monitorID),
			slog.Int("events", len(events)))

		r.controls.ReplayKubeEvents(events)
	}
}

// HoldKubeEvent buffers an event while its monitor is frozen and returns
// true. Events of monitors without active owners return false and must be
// processed immediately, preserving the original event ordering.
func (r *Registry) HoldKubeEvent(ev kemtypes.KubeEvent) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	kf := r.monitors[ev.MonitorId]
	if kf == nil || len(kf.owners) == 0 {
		return false
	}

	if ev.Type == kemtypes.TypeSynchronization || len(ev.Objects) == 0 {
		kf.raw = append(kf.raw, ev)
		return true
	}

	r.bufferFactsLocked(kf, ev)

	return true
}

// bufferFactsLocked compacts a TypeEvent into the latest-fact buffer of a
// monitor. The caller must hold r.mu.
func (r *Registry) bufferFactsLocked(kf *monitorFreeze, ev kemtypes.KubeEvent) {
	watchEvents := ev.WatchEvents
	for i, obj := range ev.Objects {
		op := kemtypes.WatchEventModified
		switch {
		case i < len(watchEvents):
			op = watchEvents[i]
		case len(watchEvents) > 0:
			op = watchEvents[0]
		}

		id := obj.Metadata.ResourceId
		fact := kf.facts[id]

		if fact == nil {
			kf.order = append(kf.order, id)
			kf.facts[id] = &heldFact{
				firstOp: op,
				lastOp:  op,
				event:   singleObjectEvent(ev.MonitorId, op, obj),
			}

			continue
		}

		// Compact to the latest fact for this resource.
		switch {
		case op == kemtypes.WatchEventDeleted && fact.firstOp == kemtypes.WatchEventAdded:
			// Appeared and disappeared while frozen: no change versus the
			// pre-freeze snapshot, drop the resource.
			delete(kf.facts, id)
			kf.removeOrder(id)
		case op == kemtypes.WatchEventDeleted:
			fact.lastOp = kemtypes.WatchEventDeleted
			fact.event = singleObjectEvent(ev.MonitorId, op, obj)
		case fact.firstOp == kemtypes.WatchEventDeleted:
			// Deleted and re-created while frozen: the object existed in the
			// pre-freeze snapshot and exists again, so replay it as Modified
			// with the latest payload.
			fact.lastOp = kemtypes.WatchEventModified
			fact.event = singleObjectEvent(ev.MonitorId, kemtypes.WatchEventModified, obj)
		default:
			// Added/Modified: keep the original operation label but carry
			// the latest object payload.
			fact.lastOp = op
			fact.event = singleObjectEvent(ev.MonitorId, fact.firstOp, obj)
		}
	}
}

// absorbEvents merges already-compacted events into a new freeze
// generation that started before the previous owner could replay them.
func (r *Registry) absorbEvents(monitorID string, events []kemtypes.KubeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	kf := r.monitors[monitorID]
	if kf == nil || len(kf.owners) == 0 {
		return
	}

	for _, ev := range events {
		if ev.Type == kemtypes.TypeSynchronization || len(ev.Objects) == 0 {
			kf.raw = append(kf.raw, ev)
			continue
		}

		r.bufferFactsLocked(kf, ev)
	}
}

// IsModuleFrozen reports whether at least one execution currently owns a
// freeze of the module.
func (r *Registry) IsModuleFrozen(moduleName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	mf := r.modules[moduleName]
	return mf != nil && len(mf.owners) > 0
}

// IsMonitorFrozen reports whether at least one execution currently owns a
// freeze of the Kubernetes monitor.
func (r *Registry) IsMonitorFrozen(monitorID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	kf := r.monitors[monitorID]
	return kf != nil && len(kf.owners) > 0
}

// FrozenModules returns a sorted snapshot of currently frozen modules.
func (r *Registry) FrozenModules() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, 0, len(r.modules))
	for name, mf := range r.modules {
		if len(mf.owners) > 0 {
			out = append(out, name)
		}
	}

	return sortedStrings(out)
}

// Rebuild reconciles the registry state after a process restart or a state
// loss suspicion. Executions are the tasks currently being processed by the
// queues; ownership is rebuilt for their scopes. Paused module monitors
// without a matching execution (orphans left by a crashed process or a lost
// task) are resumed and reconciled; frozen Kubernetes monitors without an
// owner are discarded together with their stale held events.
func (r *Registry) Rebuild(ctx context.Context, executions []Execution, pausedModules []string) RebuildReport {
	report := RebuildReport{}

	wantedModules := make(map[string]struct{})
	wantedMonitors := make(map[string]struct{})
	taskSeen := make(map[string]struct{})

	r.mu.Lock()

	// Rebuild reconciles ownership with the tasks that are ACTUALLY
	// processing. Ownership of executions which died with the old process
	// can never be released anymore, so every key starts with an empty owner
	// set. Running tasks which already hold ownership (an in-process rebuild)
	// keep their original ownership object so their regular Release keeps
	// working; running tasks without ownership get a synthetic one. Held
	// facts of the old generation are discarded.
	existingOwners := make(map[string]*Ownership)
	claimed := make(map[uint64]bool)

	for _, mf := range r.modules {
		for _, o := range mf.owners {
			if _, seen := existingOwners[o.exec.TaskID]; !seen {
				existingOwners[o.exec.TaskID] = o
			}
		}

		mf.owners = make(map[uint64]*Ownership)
	}

	for _, kf := range r.monitors {
		for _, o := range kf.owners {
			if _, seen := existingOwners[o.exec.TaskID]; !seen {
				existingOwners[o.exec.TaskID] = o
			}
		}

		kf.owners = make(map[uint64]*Ownership)
		kf.raw = nil
		kf.order = nil
		kf.facts = make(map[string]*heldFact)
	}

	previouslyFrozen := make(map[string]struct{}, len(r.modules))
	for moduleName := range r.modules {
		previouslyFrozen[moduleName] = struct{}{}
	}

	// Register ownership for the executions that are actually processing.
	for _, exec := range executions {
		modules := dedupe(exec.Scope.Modules)
		monitorIDs := dedupe(exec.Scope.MonitorIDs)

		if len(modules) == 0 && len(monitorIDs) == 0 {
			continue
		}

		if _, ok := taskSeen[exec.TaskID]; !ok {
			taskSeen[exec.TaskID] = struct{}{}
			report.Reacquired = append(report.Reacquired, exec.TaskID)
		}

		// Keep the ownership object a live task will release itself; only
		// allocate a synthetic ownership when none exists.
		o, exists := existingOwners[exec.TaskID]
		if !exists || claimed[o.id] {
			if exists {
				// Several executions claim the same task id only on
				// malformed input; give the duplicates a fresh ownership.
				o = nil
			}

			r.seq++
			o = &Ownership{id: r.seq, exec: exec, reg: r}
		}

		claimed[o.id] = true

		for _, moduleName := range modules {
			wantedModules[moduleName] = struct{}{}

			mf := r.modules[moduleName]
			if mf == nil {
				mf = &moduleFreeze{epoch: 1, owners: make(map[uint64]*Ownership)}
				r.modules[moduleName] = mf
			}

			mf.owners[o.id] = o
		}

		for _, monitorID := range monitorIDs {
			wantedMonitors[monitorID] = struct{}{}

			kf := r.monitors[monitorID]
			if kf == nil {
				kf = &monitorFreeze{
					epoch:  1,
					owners: make(map[uint64]*Ownership),
					facts:  make(map[string]*heldFact),
				}
				r.monitors[monitorID] = kf
			}

			kf.owners[o.id] = o
		}
	}

	// Orphaned module freezes: registry state without a live execution.
	for moduleName, mf := range r.modules {
		if _, wanted := wantedModules[moduleName]; wanted {
			continue
		}

		mf.epoch++
		delete(r.modules, moduleName)
		report.ResumedModules = append(report.ResumedModules, moduleName)
	}

	// Orphaned monitor freezes: drop held facts, they belong to a dead
	// generation and must never overwrite current state.
	for monitorID, kf := range r.monitors {
		if _, wanted := wantedMonitors[monitorID]; wanted {
			continue
		}

		kf.epoch++
		delete(r.monitors, monitorID)
		report.DroppedMonitors = append(report.DroppedMonitors, monitorID)
	}

	r.mu.Unlock()

	// Physical monitors that were paused when the process died but have no
	// execution anymore are orphans too.
	physicallyPaused := make(map[string]struct{}, len(pausedModules))
	for _, moduleName := range pausedModules {
		physicallyPaused[moduleName] = struct{}{}
	}

	report.Reacquired = sortedStrings(report.Reacquired)
	report.ResumedModules = sortedStrings(unionStrings(report.ResumedModules, pausedModules, wantedModules))
	report.DroppedMonitors = sortedStrings(report.DroppedMonitors)

	r.logger.Info("rebuild freeze ownership",
		slog.Any("reacquired_tasks", report.Reacquired),
		slog.Any("resumed_modules", report.ResumedModules),
		slog.Any("dropped_monitors", report.DroppedMonitors))

	// Restore physical pause state: pause wanted modules that were not
	// already physically frozen, release and reconcile orphaned monitors.
	for moduleName := range wantedModules {
		if _, wasPaused := previouslyFrozen[moduleName]; wasPaused {
			continue
		}

		if _, wasPaused := physicallyPaused[moduleName]; wasPaused {
			continue
		}

		if r.controls.ModuleMonitorExists(moduleName) {
			r.controls.PauseModuleMonitor(moduleName)
		}
	}

	for _, moduleName := range report.ResumedModules {
		if !r.controls.ModuleMonitorExists(moduleName) {
			continue
		}

		r.controls.ResumeModuleMonitor(moduleName)
		r.controls.ReconcileModule(ctx, moduleName)
	}

	return report
}

// compactLocked builds the ordered, latest-fact event list for a thawed
// monitor. Synchronization events are emitted first so snapshots are
// refreshed before incremental facts are applied.
func (r *Registry) compactLocked(kf *monitorFreeze) []kemtypes.KubeEvent {
	events := make([]kemtypes.KubeEvent, 0, len(kf.raw)+len(kf.order))
	events = append(events, kf.raw...)

	for _, id := range kf.order {
		fact := kf.facts[id]
		if fact == nil {
			continue
		}

		events = append(events, fact.event)
	}

	return events
}

func (kf *monitorFreeze) removeOrder(id string) {
	for i, v := range kf.order {
		if v == id {
			kf.order = append(kf.order[:i], kf.order[i+1:]...)
			return
		}
	}
}

func singleObjectEvent(monitorID string, op kemtypes.WatchEventType, obj kemtypes.ObjectAndFilterResult) kemtypes.KubeEvent {
	return kemtypes.KubeEvent{
		MonitorId:   monitorID,
		Type:        kemtypes.TypeEvent,
		WatchEvents: []kemtypes.WatchEventType{op},
		Objects:     []kemtypes.ObjectAndFilterResult{obj},
	}
}

func dedupe(in []string) []string {
	if len(in) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))

	for _, v := range in {
		if v == "" {
			continue
		}

		if _, ok := seen[v]; ok {
			continue
		}

		seen[v] = struct{}{}
		out = append(out, v)
	}

	return out
}

func monitorKeys(m map[string][]kemtypes.KubeEvent) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return sortedStrings(out)
}

func sortedStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}

	out := append([]string(nil), in...)
	sort.Strings(out)

	return out
}

// unionStrings returns the sorted union of the provided values and extra,
// excluding keys present in exclude.
func unionStrings(values []string, extra []string, exclude map[string]struct{}) []string {
	seen := make(map[string]struct{}, len(values)+len(extra))

	for _, v := range values {
		seen[v] = struct{}{}
	}

	for _, v := range extra {
		seen[v] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for v := range seen {
		if _, skip := exclude[v]; skip {
			continue
		}

		out = append(out, v)
	}

	return sortedStrings(out)
}

// String renders an execution for logs.
func (e Execution) String() string {
	return fmt.Sprintf("Execution{task=%s queue=%s attempt=%d reason=%s}",
		e.TaskID, e.Queue, e.Attempt, e.Reason)
}
