package config

import (
	"github.com/flant/addon-operator/pkg/utils"
)

type KubeConfig struct {
	Global  *GlobalKubeConfig
	Modules map[string]*ModuleKubeConfig

	// ResourceVersion is the version of the backend resource this snapshot was
	// read from. It is used as the optimistic-concurrency baseline for hook
	// values writebacks.
	ResourceVersion string

	// LastCommits stores stable identifiers of the last commit applied to every
	// section (map key is the ConfigMap key, e.g. "global" or "myModule").
	// Commit identifiers are written atomically with the section content and
	// survive process restarts, so they can be used to confirm whether an
	// uncertain write has landed.
	LastCommits map[string]string
}

type GlobalKubeConfig struct {
	Values   utils.Values
	Checksum string
}

// GetValues returns global values, enrich them with top level key 'global'
func (gkc GlobalKubeConfig) GetValues() utils.Values {
	if len(gkc.Values) == 0 {
		return gkc.Values
	}

	if gkc.Values.HasKey("global") {
		switch v := gkc.Values["global"].(type) {
		case map[string]interface{}:
			return utils.Values(v)

		case utils.Values:
			return v
		}
	}

	return gkc.Values
}

type ModuleKubeConfig struct {
	utils.ModuleConfig
	Checksum string
}

func NewConfig() *KubeConfig {
	return &KubeConfig{
		Modules: make(map[string]*ModuleKubeConfig),
	}
}

type (
	KubeConfigType  string
	KubeConfigEvent struct {
		Type                      KubeConfigType
		ModuleEnabledStateChanged []string
		ModuleValuesChanged       []string
		GlobalSectionChanged      bool
		ModuleMaintenanceChanged  map[string]utils.Maintenance
	}
)

const (
	KubeConfigChanged KubeConfigType = "Changed"
	KubeConfigInvalid KubeConfigType = "Invalid"
)

// CommitResult describes how a hook values writeback ended up.
//
// A writeback is "committed" only when its content is confirmed to be present
// in the backend resource. ConflictPaths lists hook-modified paths that were
// not written because the same paths were changed externally since the read
// baseline: remote values are kept for such paths and the section is scheduled
// for recalculation.
type CommitResult struct {
	// ResourceVersion of the backend resource after the commit.
	ResourceVersion string

	// Checksum of the section content as it exists remotely. It is calculated
	// with the same rules as checksums of informer snapshots, so it suppresses
	// the self-update event exactly once.
	Checksum string

	// Values is the section content now present in the backend resource
	// (without the root "global"/"<moduleName>" key).
	Values utils.Values

	// ConflictPaths are patch paths (e.g. "/myModule/foo") where the remote
	// value conflicted with the hook target value. Empty for an uncontended
	// writeback.
	ConflictPaths []string

	// Merged is true when the remote section changed since the read baseline
	// and the committed content differs from the hook target: either because
	// external updates of other paths were preserved or because conflicts were
	// resolved in favor of remote values. In-memory config must adopt the
	// remote content in this case and recalculate the section.
	Merged bool
}

// HasConflicts returns true if at least one hook-modified path conflicted
// with a concurrent remote update.
func (r *CommitResult) HasConflicts() bool {
	return r != nil && len(r.ConflictPaths) > 0
}

// RemoteDiverged returns true when the committed section does not exactly
// match the hook target, so callers must not apply the target in memory and
// must wait for the remote values to be reapplied by the change event.
func (r *CommitResult) RemoteDiverged() bool {
	return r != nil && (r.Merged || len(r.ConflictPaths) > 0)
}
