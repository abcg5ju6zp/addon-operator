package configmap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/gofrs/uuid/v5"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"gopkg.in/yaml.v3"

	"github.com/flant/addon-operator/pkg/kube_config_manager/backend"
	"github.com/flant/addon-operator/pkg/utils"
)

// commitAnnotationPrefix stores the stable identity of the last commit for a
// section. The annotation is written in the same request as the section data,
// so it lands atomically and can confirm an ambiguous write afterwards.
const commitAnnotationPrefix = "commit.addon-operator.io/"

// maxCommitAttempts bounds retries on optimistic-concurrency conflicts.
const maxCommitAttempts = 5

// commitRetryBackoff is the base delay between conflict retries.
const commitRetryBackoff = 10 * time.Millisecond

// CommitConfigValues writes hook-produced values for one section with
// optimistic concurrency.
//
// It merges on resourceVersion changes, retries definitive 409 conflicts,
// confirms ambiguous write results with a follow-up read and never silently
// overwrites external updates of paths the hook did not touch.
func (b Backend) CommitConfigValues(ctx context.Context, req backend.CommitRequest) (*backend.CommitOutcome, error) {
	if req.Key == "" {
		return nil, fmt.Errorf("commit config values: empty section key")
	}

	dataKey, rootKey, target, err := normalizeTarget(req.Key, req.Values)
	if err != nil {
		return nil, err
	}

	commitID := req.CommitID
	if commitID == "" {
		commitID = uuid.Must(uuid.NewV4()).String()
	}

	for attempt := 0; attempt < maxCommitAttempts; attempt++ {
		obj, err := b.getConfigMap(ctx)
		if err != nil {
			return nil, err
		}

		if obj == nil {
			outcome, cerr := b.createSection(ctx, req.Key, dataKey, commitID, target)
			if cerr == nil {
				return outcome, nil
			}

			if !apierrors.IsAlreadyExists(cerr) {
				confirmed, ferr := b.confirmAfterAmbiguous(ctx, req.Key, dataKey, commitID, target, cerr)
				if ferr != nil {
					return nil, ferr
				} else if confirmed != nil {
					return confirmed, nil
				}

				return nil, cerr
			}

			// ConfigMap was created concurrently: retry the update path.
			if err := sleepOrDone(ctx, commitRetryBackoff); err != nil {
				return nil, err
			}

			continue
		}

		remote, err := readSectionValues(req.Key, obj.Data)
		if err != nil {
			return nil, err
		}

		mergedValues, conflictPaths, merged, err := b.mergeIfNeeded(rootKey, req, target, remote, obj.ResourceVersion)
		if err != nil {
			return nil, err
		}

		sectionYAML, err := marshalSection(mergedValues)
		if err != nil {
			return nil, err
		}

		if obj.Data == nil {
			obj.Data = make(map[string]string)
		}

		obj.Data[dataKey] = sectionYAML

		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}

		obj.Annotations[commitAnnotationKey(dataKey)] = commitID

		updated, updateErr := b.client.CoreV1().ConfigMaps(b.namespace).Update(ctx, obj, metav1.UpdateOptions{})

		switch {
		case updateErr == nil:
			return b.buildOutcome(req.Key, dataKey, updated.GetResourceVersion(), merged, conflictPaths, sectionYAML)

		case apierrors.IsConflict(updateErr):
			b.logger.Debug("Config commit conflict, retrying with fresh remote content",
				"key", dataKey,
				"commit", commitID,
				"attempt", attempt+1)

			if attempt+1 >= maxCommitAttempts {
				return nil, fmt.Errorf("%w: section %q after %d attempts", backend.ErrCommitConflict, dataKey, maxCommitAttempts)
			}

			if err := sleepOrDone(ctx, time.Duration(attempt+1)*commitRetryBackoff); err != nil {
				return nil, err
			}

		default:
			confirmed, ferr := b.confirmAfterAmbiguous(ctx, req.Key, dataKey, commitID, target, updateErr)
			if ferr != nil {
				return nil, ferr
			}

			if confirmed != nil {
				return confirmed, nil
			}

			if isAmbiguousWriteError(updateErr) {
				return nil, fmt.Errorf("%w: section %q: %v", backend.ErrCommitNotConfirmed, dataKey, updateErr)
			}

			return nil, updateErr
		}
	}

	// Unreachable: the loop either returns an outcome or ErrCommitConflict.
	return nil, fmt.Errorf("%w: section %q", backend.ErrCommitConflict, dataKey)
}

// createSection creates the ConfigMap with the single committed section.
func (b Backend) createSection(ctx context.Context, key, dataKey, commitID string, target map[string]interface{}) (*backend.CommitOutcome, error) {
	sectionYAML, err := marshalSection(target)
	if err != nil {
		return nil, err
	}

	obj := &v1.ConfigMap{}
	obj.Name = b.name
	obj.Data = map[string]string{dataKey: sectionYAML}
	obj.Annotations = map[string]string{
		commitAnnotationKey(dataKey): commitID,
	}

	created, err := b.client.CoreV1().ConfigMaps(b.namespace).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	return b.buildOutcome(key, dataKey, created.GetResourceVersion(), false, nil, sectionYAML)
}

// mergeIfNeeded decides what content the update should carry.
// When the remote resourceVersion still equals the read baseline (or the
// caller did not provide a baseline/patch), the target is written directly.
// Otherwise the target is merged with the remote content.
func (b Backend) mergeIfNeeded(rootKey string, req backend.CommitRequest, target, remote map[string]interface{}, remoteRV string) (map[string]interface{}, []string, bool, error) {
	if req.Patch == nil || req.BaselineResourceVersion == "" || remoteRV == req.BaselineResourceVersion {
		return target, nil, false, nil
	}

	baseline, err := asSectionMap(rootKey, req.BaselineValues)
	if err != nil {
		return nil, nil, false, err
	}

	targetMap, err := asSectionMap(rootKey, target)
	if err != nil {
		return nil, nil, false, err
	}

	mergedValues, conflicts, err := mergeSectionCommit(rootKey, baseline, targetMap, remote, req.Patch.Operations)
	if err != nil {
		return nil, nil, false, err
	}

	// A version bump with identical section content (e.g. an annotation-only
	// write or a change of another section) is indistinguishable from the
	// uncontended path: the target equals what the remote section would be.
	merged := len(conflicts) > 0 || !mapsEqual(mergedValues, targetMap)

	return mergedValues, conflicts, merged, nil
}

// confirmAfterAmbiguous checks whether an uncertain write actually landed.
// The returned outcome is nil with nil error when the error is definitive
// (i.e. the commit is certainly absent).
func (b Backend) confirmAfterAmbiguous(ctx context.Context, key, dataKey, commitID string, target map[string]interface{}, writeErr error) (*backend.CommitOutcome, error) {
	if !isAmbiguousWriteError(writeErr) {
		return nil, nil
	}

	b.logger.Debug("Config commit result is ambiguous, confirming with a read",
		"key", dataKey,
		"commit", commitID,
		"error", writeErr.Error())

	obj, err := b.getConfigMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: confirm read failed: %v (write error: %v)", backend.ErrCommitNotConfirmed, err, writeErr)
	}

	if obj == nil || obj.Annotations[commitAnnotationKey(dataKey)] != commitID {
		return nil, nil
	}

	// The annotation is written atomically with the section data, so the
	// remote section is exactly a payload of this commit. Rebuild the outcome
	// from remote content and re-derive which hook paths lost to remote facts
	// (possible when one of the retries landed before the response was lost).
	remote, err := readSectionValues(key, obj.Data)
	if err != nil {
		return nil, err
	}

	conflicts := divergedPaths(target, remote)
	merged := len(conflicts) > 0 || !mapsEqual(target, remote)

	sectionYAML, err := marshalSection(remote)
	if err != nil {
		return nil, err
	}

	outcome, err := b.buildOutcome(key, dataKey, obj.ResourceVersion, merged, conflicts, sectionYAML)
	if err != nil {
		return nil, err
	}

	return outcome, nil
}

// divergedPaths is used only for outcomes recovered after an ambiguous write:
// the exact attempt that landed is unknown, so any content difference between
// the hook target and remote means a merge happened and remote facts may win
// on some paths. The conflicted branch then reapplies remote values and
// recalculates the section.
func divergedPaths(target, remote map[string]interface{}) []string {
	if !mapsEqual(target, remote) {
		return []string{"*"}
	}

	return nil
}

func mapsEqual(a, b map[string]interface{}) bool {
	na, _ := normalizeValues(a)
	nb, _ := normalizeValues(b)

	return reflect.DeepEqual(na, nb)
}

// normalizeTarget extracts the plain section map accepted by the merge and
// serialization helpers, tolerating both root-wrapped and plain inputs.
func normalizeTarget(key string, values utils.Values) (dataKey, rootKey string, target map[string]interface{}, err error) {
	if key == utils.GlobalValuesKey {
		dataKey = utils.GlobalValuesKey
		rootKey = utils.GlobalValuesKey

		if values != nil && values.HasKey(utils.GlobalValuesKey) {
			values = values.GetKeySection(utils.GlobalValuesKey)
		}

		target, err = asSectionMap(rootKey, values)

		return
	}

	dataKey = utils.ModuleNameToValuesKey(key)
	rootKey = dataKey

	moduleConfig := utils.NewModuleConfig(key, values)
	target, err = asSectionMap(rootKey, moduleConfig.GetValues())

	return
}

// readSectionValues parses the current section content straight from
// ConfigMap data using the same rules as informer snapshots.
func readSectionValues(key string, data map[string]string) (map[string]interface{}, error) {
	if key == utils.GlobalValuesKey {
		globalCfg, err := getGlobalKubeConfigFromConfigData(data)
		if err != nil {
			return nil, err
		}

		if globalCfg == nil {
			return map[string]interface{}{}, nil
		}

		return asSectionMap(utils.GlobalValuesKey, globalCfg.GetValues())
	}

	moduleCfg, err := extractModuleKubeConfig(key, data)
	if err != nil {
		return nil, err
	}

	return asSectionMap(utils.ModuleNameToValuesKey(key), moduleCfg.GetValues())
}

// asSectionMap converts arbitrary section values to a plain JSON-compatible
// map. Sections with non-map roots are not supported by the path-level merge;
// such targets may only be written when the resource is unchanged.
func asSectionMap(rootKey string, values interface{}) (map[string]interface{}, error) {
	if values == nil {
		return map[string]interface{}{}, nil
	}

	normalized, err := normalizeValues(values)
	if err != nil {
		return nil, err
	}

	switch v := normalized.(type) {
	case map[string]interface{}:
		return v, nil
	case nil:
		return map[string]interface{}{}, nil
	default:
		return nil, fmt.Errorf("section %q has unsupported non-map root type %T", rootKey, normalized)
	}
}

// marshalSection renders a section map the same way legacy writes did.
func marshalSection(section map[string]interface{}) (string, error) {
	if section == nil {
		section = map[string]interface{}{}
	}

	data, err := yaml.Marshal(section)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// buildOutcome calculates the canonical checksum by round-tripping the exact
// payload through the snapshot parser, so checksum-based self-update
// suppression matches what the informer will observe.
func (b Backend) buildOutcome(key, dataKey, resourceVersion string, merged bool, conflictPaths []string, sectionYAML string) (*backend.CommitOutcome, error) {
	data := map[string]string{dataKey: sectionYAML}

	var (
		values   utils.Values
		checksum string
	)

	if key == utils.GlobalValuesKey {
		globalCfg, err := getGlobalKubeConfigFromConfigData(data)
		if err != nil {
			return nil, err
		}

		if globalCfg == nil {
			values = utils.Values{}
		} else {
			checksum = globalCfg.Checksum
			values = globalCfg.GetValues()
		}
	} else {
		moduleCfg, err := extractModuleKubeConfig(key, data)
		if err != nil {
			return nil, err
		}

		checksum = moduleCfg.Checksum
		values = moduleCfg.GetValues()
	}

	return &backend.CommitOutcome{
		DataKey:          dataKey,
		ResourceVersion:  resourceVersion,
		Checksum:         checksum,
		Values:           values,
		ConflictPaths:    conflictPaths,
		Merged:           merged,
	}, nil
}

func commitAnnotationKey(dataKey string) string {
	return commitAnnotationPrefix + dataKey
}

// commitIDsFromAnnotations extracts per-section last commit identifiers.
func commitIDsFromAnnotations(annotations map[string]string) map[string]string {
	var commits map[string]string

	for key, value := range annotations {
		if !strings.HasPrefix(key, commitAnnotationPrefix) {
			continue
		}

		if commits == nil {
			commits = make(map[string]string)
		}

		commits[strings.TrimPrefix(key, commitAnnotationPrefix)] = value
	}

	return commits
}

func sleepOrDone(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// isAmbiguousWriteError reports whether a write error leaves the server-side
// outcome unknown.
func isAmbiguousWriteError(err error) bool {
	if err == nil {
		return false
	}

	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) || apierrors.IsInternalError(err) ||
		apierrors.IsServiceUnavailable(err) {
		return true
	}

	var status apierrors.APIStatus
	if errors.As(err, &status) {
		if code := status.Status().Code; code >= 500 {
			return true
		}
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true // any network-level error means the response was never received
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return true
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return true
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ECONNABORTED):
		return true
	}

	return false
}
