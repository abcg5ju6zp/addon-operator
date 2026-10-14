package kube_config_manager

import (
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"

	"github.com/flant/addon-operator/pkg/kube_config_manager/backend"
	"github.com/flant/addon-operator/pkg/kube_config_manager/config"
	"github.com/flant/addon-operator/pkg/utils"
)

// CommitStage is a stage of a hook values writeback.
type CommitStage string

const (
	// StagePrepared means the writeback was recorded locally (read baseline,
	// target content and stable identity) but was not confirmed in the backend
	// yet.
	StagePrepared CommitStage = "prepared"

	// StageCommitted means the backend resource confirmed the write, the
	// checksum was published and in-memory state was updated. The entry is kept
	// until the watcher delivers the matching snapshot, so a restart between
	// commit and observation can be recovered from the resource itself.
	StageCommitted CommitStage = "committed"
)

// valuesCommit is one writeback recorded by the manager.
type valuesCommit struct {
	id         string
	key        string // "global" or module name in kebab-case
	dataKey    string // ConfigMap key: "global" or camelCased module name
	baselineRV string
	target     utils.Values
	patch      *utils.ValuesPatch
	stage      CommitStage
	outcome    *backend.CommitOutcome
	createdAt  time.Time
}

// commitRegistry tracks in-flight and recently committed writebacks.
// It is intentionally in-memory: the durable part of a commit (its identity
// and content) lives in the backend resource annotations, so unfinished stages
// are recovered from there on process restart.
type commitRegistry struct {
	mu    sync.Mutex
	byID  map[string]*valuesCommit
	byKey map[string]*valuesCommit
}

func newCommitRegistry() *commitRegistry {
	return &commitRegistry{
		byID:  make(map[string]*valuesCommit),
		byKey: make(map[string]*valuesCommit),
	}
}

func (r *commitRegistry) begin(key, dataKey, baselineRV string, target utils.Values, patch *utils.ValuesPatch) *valuesCommit {
	r.mu.Lock()
	defer r.mu.Unlock()

	c := &valuesCommit{
		id:         uuid.Must(uuid.NewV4()).String(),
		key:        key,
		dataKey:    dataKey,
		baselineRV: baselineRV,
		target:     target.Copy(),
		patch:      patch,
		stage:      StagePrepared,
		createdAt:  time.Now(),
	}

	r.byID[c.id] = c
	r.byKey[key] = c

	return c
}

func (r *commitRegistry) committed(id string, outcome *backend.CommitOutcome) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if c, ok := r.byID[id]; ok {
		c.stage = StageCommitted
		c.outcome = outcome
	}
}

func (r *commitRegistry) forget(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	c, ok := r.byID[id]
	if !ok {
		return
	}

	delete(r.byID, id)

	if current := r.byKey[c.key]; current != nil && current.id == id {
		delete(r.byKey, c.key)
	}
}

// observe drops committed entries whose section checksum already matches the
// snapshot delivered by the watcher.
func (r *commitRegistry) observe(cfg *config.KubeConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, c := range r.byKey {
		if c.stage != StageCommitted || c.outcome == nil {
			continue
		}

		switch key {
		case utils.GlobalValuesKey:
			if cfg.Global != nil && cfg.Global.Checksum == c.outcome.Checksum {
				r.forgetLocked(c)
			}
		default:
			if moduleCfg, ok := cfg.Modules[key]; ok && moduleCfg.Checksum == c.outcome.Checksum {
				r.forgetLocked(c)
			}
		}
	}
}

func (r *commitRegistry) forgetLocked(c *valuesCommit) {
	delete(r.byID, c.id)

	if current := r.byKey[c.key]; current != nil && current.id == c.id {
		delete(r.byKey, c.key)
	}
}

// recover adopts commits found in the backend resource after a process
// restart. Memory was loaded from the same resource, so these commits are
// already at StageCommitted from the operator's point of view; their entries
// are dropped as soon as the first watcher snapshot confirms the checksums.
func (r *commitRegistry) recover(cfg *config.KubeConfig) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	recovered := make([]string, 0)

	for dataKey, commitID := range cfg.LastCommits {
		key := sectionKeyFromDataKey(dataKey)

		c := &valuesCommit{
			id:        commitID,
			key:       key,
			dataKey:   dataKey,
			stage:     StageCommitted,
			createdAt: time.Now(),
		}

		switch key {
		case utils.GlobalValuesKey:
			if cfg.Global != nil {
				c.outcome = &backend.CommitOutcome{DataKey: dataKey, Checksum: cfg.Global.Checksum}
			}
		default:
			if moduleCfg, ok := cfg.Modules[key]; ok {
				c.outcome = &backend.CommitOutcome{DataKey: dataKey, Checksum: moduleCfg.Checksum}
			}
		}

		if c.outcome == nil {
			continue
		}

		r.byID[c.id] = c
		r.byKey[c.key] = c
		recovered = append(recovered, key)
	}

	return recovered
}

// CommitConfigValues commits hook-produced values for a section.
//
// It records the read baseline (resourceVersion and section content), the
// target content and a stable commit identity, then delegates the
// resourceVersion-aware merge to the backend. On a same-path conflict the
// remote content is adopted in the in-memory snapshot and a single
// KubeConfigChanged event schedules the section recalculation. A definitive
// or unconfirmed remote failure publishes nothing: no checksum, no snapshot
// update, no event.
func (kcm *KubeConfigManager) CommitConfigValues(key string, values utils.Values, patch *utils.ValuesPatch) (*config.CommitResult, error) {
	baselineRV, baselineValues := kcm.snapshotBaseline(key)

	c := kcm.commits.begin(key, sectionDataKey(key), baselineRV, values, patch)

	req := backend.CommitRequest{
		Key:                     key,
		Values:                  values,
		Patch:                   patch,
		BaselineValues:          baselineValues,
		BaselineResourceVersion: baselineRV,
		CommitID:                c.id,
	}

	outcome, err := kcm.backend.CommitConfigValues(kcm.ctx, req)
	if err != nil {
		// Remote write failed or cannot be confirmed: do not publish the new
		// checksum and keep in-memory config untouched.
		kcm.commits.forget(c.id)

		return nil, err
	}

	kcm.commits.committed(c.id, outcome)

	conflicted := len(outcome.ConflictPaths) > 0
	// Merged sections (with or without same-path conflicts) no longer match the
	// hook target: remote facts must be reapplied and the section recalculated.
	diverged := conflicted || outcome.Merged

	var recomputeEvent *config.KubeConfigEvent

	kcm.m.Lock()
	kcm.applyCommittedSection(key, outcome)

	if diverged {
		recomputeEvent = kcm.buildConflictEvent(key)
	} else {
		// Suppress exactly one self-update event from the watcher.
		kcm.knownChecksums.Add(key, outcome.Checksum)
	}
	kcm.m.Unlock()

	if diverged {
		if conflicted {
			kcm.logger.Info("Config values writeback conflicted with a concurrent remote update, remote values kept",
				"key", key,
				"commit", c.id,
				"paths", outcome.ConflictPaths)
		} else {
			kcm.logger.Info("Config values writeback merged with a concurrent remote update, syncing remote content",
				"key", key,
				"commit", c.id)
		}

		// Send without holding the snapshot lock. The event re-applies remote
		// values (with schema validation) and reruns the section hooks, so
		// global and module hooks always observe the persisted version.
		kcm.configEventCh <- *recomputeEvent
	}

	return &config.CommitResult{
		ResourceVersion: outcome.ResourceVersion,
		Checksum:        outcome.Checksum,
		Values:          outcome.Values,
		ConflictPaths:   outcome.ConflictPaths,
		Merged:          outcome.Merged,
	}, nil
}

// snapshotBaseline returns the resourceVersion and the section content the
// hook read its input from.
func (kcm *KubeConfigManager) snapshotBaseline(key string) (string, utils.Values) {
	kcm.m.Lock()
	defer kcm.m.Unlock()

	rv := kcm.currentConfig.ResourceVersion

	var values utils.Values

	switch key {
	case utils.GlobalValuesKey:
		if kcm.currentConfig.Global != nil {
			values = kcm.currentConfig.Global.GetValues()
		}
	default:
		if moduleCfg, ok := kcm.currentConfig.Modules[key]; ok {
			values = moduleCfg.GetValues()
		}
	}

	if values == nil {
		values = utils.Values{}
	}

	return rv, values.Copy()
}

// applyCommittedSection replaces the section content in the cached snapshot
// with the content confirmed in the backend resource.
func (kcm *KubeConfigManager) applyCommittedSection(key string, outcome *backend.CommitOutcome) {
	if outcome == nil {
		return
	}

	switch key {
	case utils.GlobalValuesKey:
		kcm.currentConfig.Global = &config.GlobalKubeConfig{
			Values:   utils.Values{utils.GlobalValuesKey: outcome.Values},
			Checksum: outcome.Checksum,
		}
	default:
		moduleCfg := utils.NewModuleConfig(key, nil)
		_, _ = moduleCfg.LoadFromValues(utils.Values{
			outcome.DataKey: map[string]interface{}(outcome.Values),
		})

		newCfg := &config.ModuleKubeConfig{
			ModuleConfig: *moduleCfg,
			Checksum:     outcome.Checksum,
		}

		// Hook commits never touch enabled/maintenance keys: preserve them.
		if existing, ok := kcm.currentConfig.Modules[key]; ok {
			newCfg.IsEnabled = existing.IsEnabled
			newCfg.Maintenance = existing.Maintenance
		}

		kcm.currentConfig.Modules[key] = newCfg
	}
}

// reset drops all tracked commits, e.g. when the backend resource is deleted.
func (r *commitRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.byID = make(map[string]*valuesCommit)
	r.byKey = make(map[string]*valuesCommit)
}

// buildConflictEvent assembles the change event that schedules a section
// recalculation after a same-path conflict. Must be called with kcm.m held.
func (kcm *KubeConfigManager) buildConflictEvent(key string) *config.KubeConfigEvent {
	if key == utils.GlobalValuesKey {
		return &config.KubeConfigEvent{
			Type:                 config.KubeConfigChanged,
			GlobalSectionChanged: true,
		}
	}

	return &config.KubeConfigEvent{
		Type:                config.KubeConfigChanged,
		ModuleValuesChanged: []string{key},
	}
}

func sectionDataKey(key string) string {
	if key == utils.GlobalValuesKey {
		return utils.GlobalValuesKey
	}

	return utils.ModuleNameToValuesKey(key)
}

func sectionKeyFromDataKey(dataKey string) string {
	if dataKey == utils.GlobalValuesKey {
		return utils.GlobalValuesKey
	}

	return utils.ModuleNameFromValuesKey(dataKey)
}
