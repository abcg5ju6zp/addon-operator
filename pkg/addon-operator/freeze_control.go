package addon_operator

import (
	"context"
	"log/slog"

	"github.com/gofrs/uuid/v5"

	"github.com/flant/addon-operator/pkg"
	"github.com/flant/addon-operator/pkg/task"
	"github.com/flant/addon-operator/pkg/task/freeze"
	modulehookrun "github.com/flant/addon-operator/pkg/task/tasks/module-hook-run"
	htypes "github.com/flant/shell-operator/pkg/hook/types"
	kemtypes "github.com/flant/shell-operator/pkg/kube_events_manager/types"
	sh_task "github.com/flant/shell-operator/pkg/task"
)

// freezeControls adapts the generic freeze registry to the addon-operator
// components: module Helm resource monitors, Kubernetes binding monitors
// and the event-to-task creation path.
type freezeControls struct {
	op *AddonOperator
}

func newFreezeControls(op *AddonOperator) *freezeControls {
	return &freezeControls{op: op}
}

func (c *freezeControls) PauseModuleMonitor(moduleName string) {
	c.op.HelmResourcesManager.PauseMonitor(moduleName)
}

func (c *freezeControls) ResumeModuleMonitor(moduleName string) {
	c.op.HelmResourcesManager.ResumeMonitor(moduleName)
}

func (c *freezeControls) ModuleMonitorExists(moduleName string) bool {
	return c.op.HelmResourcesManager.HasMonitor(moduleName)
}

func (c *freezeControls) ReconcileModule(_ context.Context, moduleName string) {
	c.op.HelmResourcesManager.ReconcileMonitor(moduleName)
}

func (c *freezeControls) KubeMonitorExists(monitorID string) bool {
	return c.op.engine.KubeEventsManager.HasMonitor(monitorID)
}

// ReplayKubeEvents sends facts accumulated while a monitor was frozen
// through the same handler as live events, so queue compaction, binding
// combination and ordering remain identical. Each fact gets a fresh event
// ID label for tracing.
func (c *freezeControls) ReplayKubeEvents(events []kemtypes.KubeEvent) {
	for _, kubeEvent := range events {
		logLabels := map[string]string{
			pkg.LogKeyEventID: uuid.Must(uuid.NewV4()).String(),
			pkg.LogKeyBinding: string(htypes.OnKubernetesEvent),
			"freeze-replay":   "true",
		}

		tailTasks := c.op.ModuleManager.HandleKubeEvent(
			context.Background(),
			kubeEvent,
			c.op.createGlobalHookTaskFactory(logLabels, htypes.OnKubernetesEvent, "Kubernetes", true),
			c.op.createModuleHookTaskFactory(logLabels, htypes.OnKubernetesEvent, "Kubernetes"),
		)

		c.op.engine.TaskQueues.AddTailTasks(tailTasks...)
	}
}

// rebuildFreezeOwnership reconciles freeze ownership with the actual queue,
// synchronization and monitor state after a process restart (or before
// queue workers start).
//
//   - Tasks currently being processed re-acquire ownership for the modules
//     and monitors they affect. Unfinished Synchronization bindings without
//     a running task are rebuilt by the ModuleRun startup phases
//     (EnableKubernetesBindings is idempotent) and acquire ownership again
//     when their Synchronization task runs.
//   - Module monitors left paused by a dead process without a matching
//     execution are resumed and immediately reconciled against the latest
//     cluster facts.
//   - Frozen Kubernetes monitors without an owner are discarded together
//     with their stale held events.
func (op *AddonOperator) rebuildFreezeOwnership() freeze.RebuildReport {
	var executions []freeze.Execution

	for _, q := range op.engine.TaskQueues.GetSnapshot() {
		if q == nil {
			continue
		}

		q.IterateSnapshot(func(t sh_task.Task) {
			// Only the task actually being processed held freeze ownership
			// when the process died. Tasks waiting in the queue must not
			// freeze watches for their whole waiting time.
			if !t.IsProcessing() {
				return
			}

			exec, ok := op.freezeExecutionForTask(t)
			if !ok {
				return
			}

			executions = append(executions, exec)
		})
	}

	report := op.FreezeRegistry.Rebuild(op.ctx, executions, op.HelmResourcesManager.PausedModules())

	op.Logger.Info("Rebuild freeze ownership",
		slog.Any("reacquired_tasks", report.Reacquired),
		slog.Any("resumed_modules", report.ResumedModules),
		slog.Any("dropped_monitors", report.DroppedMonitors))

	return report
}

// freezeExecutionForTask maps an in-flight queue task to a freeze execution
// scope. Only tasks that actually mutate module values, releases or
// synchronization state hold ownership.
func (op *AddonOperator) freezeExecutionForTask(t sh_task.Task) (freeze.Execution, bool) {
	hm := task.HookMetadataAccessor(t)

	scope := freeze.Scope{}

	switch t.GetType() {
	case task.ModuleRun:
		if hm.ModuleName == "" {
			return freeze.Execution{}, false
		}

		scope.Modules = []string{hm.ModuleName}

	case task.ModuleHookRun:
		if hm.ModuleName == "" {
			return freeze.Execution{}, false
		}

		scope.Modules = []string{hm.ModuleName}

		if hm.BindingType == htypes.OnKubernetesEvent {
			module := op.ModuleManager.GetModule(hm.ModuleName)
			if module != nil {
				if moduleHook := module.GetHookByName(hm.HookName); moduleHook != nil {
					scope.MonitorIDs = modulehookrun.HookMonitorIDs(moduleHook, hm.Binding)
				}
			}
		}

	default:
		return freeze.Execution{}, false
	}

	return freeze.Execution{
		TaskID:  t.GetId(),
		Queue:   t.GetQueueName(),
		Attempt: uint64(t.GetFailureCount()) + 1,
		Reason:  "rebuild",
		Scope:   scope,
	}, true
}
