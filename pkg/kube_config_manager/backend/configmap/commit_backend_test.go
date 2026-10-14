package configmap

import (
	"context"
	"errors"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	sdkutils "github.com/deckhouse/module-sdk/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	corev1fake "k8s.io/client-go/kubernetes/typed/core/v1/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/flant/addon-operator/pkg/kube_config_manager/backend"
	"github.com/flant/addon-operator/pkg/utils"
	klient "github.com/flant/kube-client/client"
)

const (
	testNamespace = "default"
	testCMName    = "test-addon-operator"
)

func newTestBackendWithCM(t *testing.T, data map[string]string) (*Backend, *klient.Client, *v1.ConfigMap) {
	t.Helper()

	kubeClient := klient.NewFake(nil)

	cm := &v1.ConfigMap{}
	cm.SetNamespace(testNamespace)
	cm.SetName(testCMName)
	// The fake tracker does not assign resourceVersions.
	cm.ResourceVersion = "10"
	cm.Data = data

	created, err := kubeClient.CoreV1().ConfigMaps(testNamespace).Create(context.TODO(), cm, metav1.CreateOptions{})
	require.NoError(t, err)

	bk := New(kubeClient, testNamespace, testCMName, log.NewNop())

	return bk, kubeClient, created
}

func configMapsGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
}

func updateReactors(client *klient.Client) *corev1fake.FakeCoreV1 {
	return client.CoreV1().(*corev1fake.FakeCoreV1)
}

// Test_CommitConfigValues_uncontended checks the basic write: content,
// canonical checksum and stable commit annotation.
func Test_CommitConfigValues_uncontended(t *testing.T) {
	bk, _, cm := newTestBackendWithCM(t, map[string]string{
		"alpha": "a: 1\nkeep: x\n",
	})

	target := utils.Values{"a": 2.0, "keep": "x", "new": 3.0}
	patch := &utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{
		{Op: "add", Path: "/alpha/a", Value: []byte("2")},
		{Op: "add", Path: "/alpha/new", Value: []byte("3")},
	}}

	outcome, err := bk.CommitConfigValues(context.Background(), backend.CommitRequest{
		Key:                     "alpha",
		Values:                  target,
		Patch:                   patch,
		BaselineValues:          utils.Values{"a": 1.0, "keep": "x"},
		BaselineResourceVersion: cm.ResourceVersion,
	})
	require.NoError(t, err)
	require.NotNil(t, outcome)

	assert.Empty(t, outcome.ConflictPaths)
	assert.False(t, outcome.Merged)

	obj, err := bk.getConfigMap(context.Background())
	require.NoError(t, err)

	// Annotation stores the stable identity atomically with the data.
	assert.Equal(t, "a: 2\nkeep: x\nnew: 3\n", obj.Data["alpha"])
	assert.NotEmpty(t, obj.Annotations[commitAnnotationKey("alpha")])

	// Outcome checksum must match the checksum of the informer snapshot parse.
	parsed, err := extractModuleKubeConfig("alpha", obj.Data)
	require.NoError(t, err)
	assert.Equal(t, parsed.Checksum, outcome.Checksum)
}

// Test_CommitConfigValues_creates_missing_configmap covers the create path.
func Test_CommitConfigValues_creates_missing_configmap(t *testing.T) {
	kubeClient := klient.NewFake(nil)
	bk := New(kubeClient, testNamespace, testCMName, log.NewNop())

	outcome, err := bk.CommitConfigValues(context.Background(), backend.CommitRequest{
		Key:    utils.GlobalValuesKey,
		Values: utils.Values{"project": "p"},
	})
	require.NoError(t, err)
	assert.Equal(t, "global", outcome.DataKey)

	obj, err := bk.getConfigMap(context.Background())
	require.NoError(t, err)
	assert.Contains(t, obj.Data, "global")
	assert.Contains(t, obj.Annotations, commitAnnotationKey("global"))
}

// Test_CommitConfigValues_retry_on_conflict verifies that a 409 is retried and
// the retry merges against fresh remote content, preserving other sections.
func Test_CommitConfigValues_retry_on_conflict(t *testing.T) {
	bk, client, cm := newTestBackendWithCM(t, map[string]string{
		"alpha": "a: 1\n",
	})

	// Concurrent external update: a new section appears and the remote RV
	// advances past the hook baseline.
	tracker := client.Interface.(*fake.Clientset).Tracker()
	current, err := bk.getConfigMap(context.Background())
	require.NoError(t, err)
	current.Data["beta"] = "enabled: true\n"
	// The fake tracker keeps resourceVersion on update, so advance it manually
	// to emulate the real API server behavior.
	current.ResourceVersion = "999"
	require.NoError(t, tracker.Update(configMapsGVR(), current, testNamespace))

	conflicted := false
	updateReactors(client).PrependReactor("update", "configmaps",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			if !conflicted {
				conflicted = true

				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, testCMName, errors.New("stale resourceVersion"))
			}

			return false, nil, nil
		})

	target := utils.Values{"a": 2.0}
	patch := &utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{
		{Op: "add", Path: "/alpha/a", Value: []byte("2")},
	}}

	outcome, err := bk.CommitConfigValues(context.Background(), backend.CommitRequest{
		Key:                     "alpha",
		Values:                  target,
		Patch:                   patch,
		BaselineValues:          utils.Values{"a": 1.0},
		BaselineResourceVersion: cm.ResourceVersion,
	})
	require.NoError(t, err)
	assert.True(t, conflicted, "update must be retried after conflict")
	// The remote alpha section content is identical to the hook target
	// (only another section changed), so the merge produces no divergence.
	assert.False(t, outcome.Merged)
	assert.Empty(t, outcome.ConflictPaths)

	// Both the hook section and the externally added section survive.
	obj, err := bk.getConfigMap(context.Background())
	require.NoError(t, err)
	assert.Contains(t, obj.Data, "alpha")
	assert.Contains(t, obj.Data, "beta")
	assert.Equal(t, "a: 2\n", obj.Data["alpha"])
}

// Test_CommitConfigValues_ambiguous_landed simulates a timeout after the
// server applied the write: the follow-up read must confirm the commit.
func Test_CommitConfigValues_ambiguous_landed(t *testing.T) {
	bk, client, cm := newTestBackendWithCM(t, map[string]string{
		"alpha": "a: 1\n",
	})

	tracker := client.Interface.(*fake.Clientset).Tracker()
	updateReactors(client).PrependReactor("update", "configmaps",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			// Apply the payload as if the server committed it...
			updateAction := action.(k8stesting.UpdateAction)
			require.NoError(t, tracker.Update(configMapsGVR(), updateAction.GetObject(), testNamespace))

			// ...but the response is lost.
			return true, nil, apierrors.NewTimeoutError("simulated lost response", 0)
		})

	outcome, err := bk.CommitConfigValues(context.Background(), backend.CommitRequest{
		Key:                     "alpha",
		Values:                  utils.Values{"a": 2.0},
		Patch:                   &utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{{Op: "add", Path: "/alpha/a", Value: []byte("2")}}},
		BaselineValues:          utils.Values{"a": 1.0},
		BaselineResourceVersion: cm.ResourceVersion,
		CommitID:                "stable-commit-id",
	})
	require.NoError(t, err)
	require.NotNil(t, outcome)

	obj, err := bk.getConfigMap(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "stable-commit-id", obj.Annotations[commitAnnotationKey("alpha")])
	assert.Equal(t, "a: 2\n", obj.Data["alpha"])
}

// Test_CommitConfigValues_ambiguous_not_landed simulates a timeout with no
// server-side write: the result must be unconfirmed and nothing changes.
func Test_CommitConfigValues_ambiguous_not_landed(t *testing.T) {
	bk, client, cm := newTestBackendWithCM(t, map[string]string{
		"alpha": "a: 1\n",
	})

	updateReactors(client).PrependReactor("update", "configmaps",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewTimeoutError("simulated timeout", 0)
		})

	_, err := bk.CommitConfigValues(context.Background(), backend.CommitRequest{
		Key:                     "alpha",
		Values:                  utils.Values{"a": 2.0},
		Patch:                   &utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{{Op: "add", Path: "/alpha/a", Value: []byte("2")}}},
		BaselineValues:          utils.Values{"a": 1.0},
		BaselineResourceVersion: cm.ResourceVersion,
		CommitID:                "stable-commit-id",
	})
	require.ErrorIs(t, err, backend.ErrCommitNotConfirmed)

	obj, err := bk.getConfigMap(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "a: 1\n", obj.Data["alpha"])
	_, hasAnnotation := obj.Annotations[commitAnnotationKey("alpha")]
	assert.False(t, hasAnnotation)
}

// Test_CommitConfigValues_conflict_exhausted verifies definitive failure after
// too many conflicts.
func Test_CommitConfigValues_conflict_exhausted(t *testing.T) {
	bk, client, cm := newTestBackendWithCM(t, map[string]string{
		"alpha": "a: 1\n",
	})

	updateReactors(client).PrependReactor("update", "configmaps",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, testCMName, errors.New("always conflicting"))
		})

	_, err := bk.CommitConfigValues(context.Background(), backend.CommitRequest{
		Key:                     "alpha",
		Values:                  utils.Values{"a": 2.0},
		Patch:                   &utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{{Op: "add", Path: "/alpha/a", Value: []byte("2")}}},
		BaselineValues:          utils.Values{"a": 1.0},
		BaselineResourceVersion: cm.ResourceVersion,
	})
	require.ErrorIs(t, err, backend.ErrCommitConflict)

	obj, err := bk.getConfigMap(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "a: 1\n", obj.Data["alpha"])
}
