package kube_config_manager

import (
	"context"
	"testing"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"
	sdkutils "github.com/deckhouse/module-sdk/pkg/utils"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	corev1fake "k8s.io/client-go/kubernetes/typed/core/v1/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/flant/addon-operator/pkg/kube_config_manager/backend/configmap"
	"github.com/flant/addon-operator/pkg/kube_config_manager/config"
	"github.com/flant/addon-operator/pkg/utils"
	klient "github.com/flant/kube-client/client"
)

const commitTestCM = "test-commit-cm"

func newCommitTestEnv(t *testing.T, startInformer bool, data map[string]string) (*KubeConfigManager, *klient.Client) {
	t.Helper()
	g := NewWithT(t)

	kubeClient := klient.NewFake(nil)

	cm := &v1.ConfigMap{}
	cm.SetNamespace("default")
	cm.SetName(commitTestCM)
	cm.ResourceVersion = "10"
	cm.Data = data

	_, err := kubeClient.CoreV1().ConfigMaps("default").Create(context.TODO(), cm, metav1.CreateOptions{})
	g.Expect(err).ShouldNot(HaveOccurred())

	bk := configmap.New(kubeClient, "default", commitTestCM, log.NewNop())
	kcm := NewKubeConfigManager(context.Background(), bk, nil, log.NewNop())
	g.Expect(kcm.Init()).ShouldNot(HaveOccurred())

	if startInformer {
		kcm.Start()
	}

	t.Cleanup(kcm.Stop)

	return kcm, kubeClient
}

func addOp(path, value string) *sdkutils.ValuesPatchOperation {
	return &sdkutils.ValuesPatchOperation{Op: "add", Path: path, Value: []byte(value)}
}

func drainEvents(kcm *KubeConfigManager, d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		select {
		case <-kcm.KubeConfigEventCh():
		case <-time.After(50 * time.Millisecond):
			return
		}
	}
}

func replaceConfigMap(t *testing.T, client *klient.Client, mutate func(cm *v1.ConfigMap), rv string) {
	t.Helper()
	g := NewWithT(t)

	obj, err := client.CoreV1().ConfigMaps("default").Get(context.TODO(), commitTestCM, metav1.GetOptions{})
	g.Expect(err).ShouldNot(HaveOccurred())

	if obj.Annotations == nil {
		obj.Annotations = make(map[string]string)
	}

	mutate(obj)
	obj.ResourceVersion = rv

	_, err = client.CoreV1().ConfigMaps("default").Update(context.TODO(), obj, metav1.UpdateOptions{})
	g.Expect(err).ShouldNot(HaveOccurred())
}

// Test_Commit_self_update_emits_no_event guards the checksum consistency:
// an uncontended hook writeback must not look like an external change.
func Test_Commit_self_update_emits_no_event(t *testing.T) {
	g := NewWithT(t)
	kcm, _ := newCommitTestEnv(t, true, map[string]string{
		"global": "a: 1\nkeep: x\n",
	})

	// Let the initial informer add settle.
	drainEvents(kcm, 500*time.Millisecond)

	result, err := kcm.CommitConfigValues(utils.GlobalValuesKey,
		utils.Values{"a": 2.0, "keep": "x"},
		&utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{
			addOp("/global/a", "2"),
		}})
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(result.HasConflicts()).To(BeFalse())

	// Snapshot adopts the committed content immediately.
	kcm.SafeReadConfig(func(cfg *config.KubeConfig) {
		g.Expect(cfg.Global.GetValues()).To(HaveKeyWithValue("a", 2.0))
	})

	// No change event must leak for a self-write.
	g.Consistently(kcm.KubeConfigEventCh(), "1s", "50ms").ShouldNot(Receive())
}

// Test_Commit_same_path_conflict_keeps_remote_and_recomputes covers the core
// concurrency semantics: external updates on other paths survive, a real
// same-path conflict keeps the remote value and triggers one recompute event.
func Test_Commit_same_path_conflict_keeps_remote_and_recomputes(t *testing.T) {
	g := NewWithT(t)
	kcm, client := newCommitTestEnv(t, false, map[string]string{
		"global": "a: 1\n",
	})

	// Administrator updates the same path the hook is about to write.
	replaceConfigMap(t, client, func(cm *v1.ConfigMap) {
		cm.Data["global"] = "a: 7\n"
	}, "11")

	result, err := kcm.CommitConfigValues(utils.GlobalValuesKey,
		utils.Values{"a": 2.0, "b": 3.0},
		&utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{
			addOp("/global/a", "2"),
			addOp("/global/b", "3"),
		}})
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(result.HasConflicts()).To(BeTrue())
	g.Expect(result.ConflictPaths).To(ContainElement("/global/a"))

	// Exactly one recompute event with the global change.
	var event config.KubeConfigEvent
	g.Eventually(kcm.KubeConfigEventCh(), "1s", "20ms").Should(Receive(&event))
	g.Expect(event.Type).To(Equal(config.KubeConfigChanged))
	g.Expect(event.GlobalSectionChanged).To(BeTrue())

	// Snapshot matches the persisted content: remote a=7 kept, hook b=3 applied.
	kcm.SafeReadConfig(func(cfg *config.KubeConfig) {
		g.Expect(cfg.Global.GetValues()).To(BeEquivalentTo(map[string]interface{}{
			"a": 7.0,
			"b": 3.0,
		}))
	})

	obj, err := client.CoreV1().ConfigMaps("default").Get(context.TODO(), commitTestCM, metav1.GetOptions{})
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(obj.Data["global"]).To(Equal("a: 7\nb: 3\n"))

	// The watcher snapshot of the same content must not emit another event.
	loaded, err := kcm.backend.LoadConfig(context.Background())
	g.Expect(err).ShouldNot(HaveOccurred())
	kcm.handleBatchConfigEvent(config.Event{Key: "batch", Config: loaded})
	select {
	case ev := <-kcm.KubeConfigEventCh():
		t.Fatalf("unexpected duplicate event: %+v", ev)
	default:
	}
}

// Test_Commit_other_paths_external_update_preserved verifies that external
// changes on paths the hook never touches are silently merged: no conflict,
// no recompute event, remote content survives.
func Test_Commit_other_paths_external_update_preserved(t *testing.T) {
	g := NewWithT(t)
	kcm, client := newCommitTestEnv(t, false, map[string]string{
		"global":          "a: 1\n",
		"alpha":           "x: 1\n",
		"alphaEnabled":    "true",
		"unrelatedModule": "keep: me\n",
	})

	replaceConfigMap(t, client, func(cm *v1.ConfigMap) {
		cm.Data["global"] = "a: 1\nadmin: true\n"
		cm.Data["unrelatedModule"] = "keep: me\nextra: 1\n"
	}, "11")

	result, err := kcm.CommitConfigValues(utils.GlobalValuesKey,
		utils.Values{"a": 2.0},
		&utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{
			addOp("/global/a", "2"),
		}})
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(result.HasConflicts()).To(BeFalse())
	// The section content differs from the hook target (admin value merged),
	// so remote content must be reapplied through a change event.
	g.Expect(result.RemoteDiverged()).To(BeTrue())

	var event config.KubeConfigEvent
	g.Eventually(kcm.KubeConfigEventCh(), "1s", "20ms").Should(Receive(&event))
	g.Expect(event.GlobalSectionChanged).To(BeTrue())

	obj, err := client.CoreV1().ConfigMaps("default").Get(context.TODO(), commitTestCM, metav1.GetOptions{})
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(obj.Data["global"]).To(Equal("a: 2\nadmin: true\n"))
	g.Expect(obj.Data["unrelatedModule"]).To(Equal("keep: me\nextra: 1\n"))
	g.Expect(obj.Data).To(HaveKey("alpha"))
}

// Test_Commit_other_section_version_bump_no_divergence verifies that a
// resourceVersion bump caused solely by another section change does not look
// like a divergence for the hook's own section.
func Test_Commit_other_section_version_bump_no_divergence(t *testing.T) {
	g := NewWithT(t)
	kcm, client := newCommitTestEnv(t, false, map[string]string{
		"global": "a: 1\n",
		"alpha":  "x: 1\n",
	})

	replaceConfigMap(t, client, func(cm *v1.ConfigMap) {
		cm.Data["alpha"] = "x: 2\n"
	}, "11")

	result, err := kcm.CommitConfigValues(utils.GlobalValuesKey,
		utils.Values{"a": 2.0},
		&utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{
			addOp("/global/a", "2"),
		}})
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(result.RemoteDiverged()).To(BeFalse(), "global section equals target, no recompute event")

	select {
	case ev := <-kcm.KubeConfigEventCh():
		t.Fatalf("unexpected event for unchanged committed section: %+v", ev)
	default:
	}

	obj, err := client.CoreV1().ConfigMaps("default").Get(context.TODO(), commitTestCM, metav1.GetOptions{})
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(obj.Data["global"]).To(Equal("a: 2\n"))
	g.Expect(obj.Data["alpha"]).To(Equal("x: 2\n"), "other section update must survive")
}

// Test_Commit_module_conflict_event_and_enabled checks the module section
// event shape and preservation of the enabled flag across a conflict.
func Test_Commit_module_conflict_event_and_enabled(t *testing.T) {
	g := NewWithT(t)
	kcm, client := newCommitTestEnv(t, false, map[string]string{
		"alpha":        "a: 1\n",
		"alphaEnabled": "true",
	})

	replaceConfigMap(t, client, func(cm *v1.ConfigMap) {
		cm.Data["alpha"] = "a: 7\n"
	}, "11")

	result, err := kcm.CommitConfigValues("alpha",
		utils.Values{"a": 2.0},
		&utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{
			addOp("/alpha/a", "2"),
		}})
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(result.HasConflicts()).To(BeTrue())

	var event config.KubeConfigEvent
	g.Eventually(kcm.KubeConfigEventCh(), "1s", "20ms").Should(Receive(&event))
	g.Expect(event.ModuleValuesChanged).To(Equal([]string{"alpha"}))

	kcm.SafeReadConfig(func(cfg *config.KubeConfig) {
		moduleCfg := cfg.Modules["alpha"]
		g.Expect(moduleCfg).NotTo(BeNil())
		g.Expect(moduleCfg.IsEnabled).NotTo(BeNil())
		g.Expect(*moduleCfg.IsEnabled).To(BeTrue())
		g.Expect(moduleCfg.GetValues()).To(BeEquivalentTo(map[string]interface{}{"a": 7.0}))
	})
}

// Test_Commit_remote_failure_publishes_nothing ensures a definitive remote
// failure does not update the snapshot, publish a checksum or emit an event.
func Test_Commit_remote_failure_publishes_nothing(t *testing.T) {
	g := NewWithT(t)
	kcm, client := newCommitTestEnv(t, false, map[string]string{
		"global": "a: 1\n",
	})

	// Install a reactor returning a definitive 403.
	client.CoreV1().(*corev1fake.FakeCoreV1).PrependReactor("update", "configmaps",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, commitTestCM, nil)
		})

	_, err := kcm.CommitConfigValues(utils.GlobalValuesKey,
		utils.Values{"a": 2.0},
		&utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{
			addOp("/global/a", "2"),
		}})
	g.Expect(err).To(HaveOccurred())

	select {
	case ev := <-kcm.KubeConfigEventCh():
		t.Fatalf("remote failure must not emit events: %+v", ev)
	default:
	}

	kcm.SafeReadConfig(func(cfg *config.KubeConfig) {
		g.Expect(cfg.Global.GetValues()).To(BeEquivalentTo(map[string]interface{}{"a": 1.0}))
	})
}

// Test_Commit_restart_recovery verifies that a new process rebuilds its
// snapshot from the ConfigMap and adopts the unfinished commit stages.
func Test_Commit_restart_recovery(t *testing.T) {
	g := NewWithT(t)
	kcm, client := newCommitTestEnv(t, false, map[string]string{
		"global": "a: 1\n",
	})

	replaceConfigMap(t, client, func(cm *v1.ConfigMap) {
		cm.Data["global"] = "a: 7\n"
	}, "11")

	_, err := kcm.CommitConfigValues(utils.GlobalValuesKey,
		utils.Values{"a": 2.0},
		&utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{
			addOp("/global/a", "2"),
		}})
	g.Expect(err).ShouldNot(HaveOccurred())
	<-kcm.KubeConfigEventCh()

	// Simulate a process restart over the same backend resource.
	bk2 := configmap.New(client, "default", commitTestCM, log.NewNop())
	kcm2 := NewKubeConfigManager(context.Background(), bk2, nil, log.NewNop())
	g.Expect(kcm2.Init()).ShouldNot(HaveOccurred())
	t.Cleanup(kcm2.Stop)

	kcm2.SafeReadConfig(func(cfg *config.KubeConfig) {
		g.Expect(cfg.Global.GetValues()).To(BeEquivalentTo(map[string]interface{}{"a": 7.0}))
		g.Expect(cfg.ResourceVersion).To(Equal("11"))
	})

	kcm2.commits.mu.Lock()
	_, recovered := kcm2.commits.byKey[utils.GlobalValuesKey]
	kcm2.commits.mu.Unlock()
	g.Expect(recovered).To(BeTrue(), "unfinished commit stage must be recovered from annotations")

	// After the watcher confirms the checksum, the recovered entry is observed.
	kcm2.Start()
	g.Eventually(func() bool {
		kcm2.commits.mu.Lock()
		defer kcm2.commits.mu.Unlock()

		_, ok := kcm2.commits.byKey[utils.GlobalValuesKey]

		return !ok
	}, "3s", "50ms").Should(BeTrue())
}
