package configmap

import (
	"encoding/json"
	"testing"

	sdkutils "github.com/deckhouse/module-sdk/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func opAdd(path string, value string) *sdkutils.ValuesPatchOperation {
	return &sdkutils.ValuesPatchOperation{Op: "add", Path: path, Value: json.RawMessage(value)}
}

func opRemove(path string) *sdkutils.ValuesPatchOperation {
	return &sdkutils.ValuesPatchOperation{Op: "remove", Path: path}
}

func Test_mergeSectionCommit_uncontended(t *testing.T) {
	baseline := map[string]interface{}{"a": 1.0, "b": 2.0}
	target := map[string]interface{}{"a": 1.0, "b": 2.0, "c": 3.0}
	remote := map[string]interface{}{"a": 1.0, "b": 2.0}

	merged, conflicts, err := mergeSectionCommit("myModule", baseline, target, remote, []*sdkutils.ValuesPatchOperation{
		opAdd("/myModule/c", "3"),
	})
	require.NoError(t, err)
	assert.Empty(t, conflicts)
	assert.Equal(t, map[string]interface{}{"a": 1.0, "b": 2.0, "c": 3.0}, merged)
}

func Test_mergeSectionCommit_preserves_external_other_paths(t *testing.T) {
	baseline := map[string]interface{}{"a": 1.0}
	target := map[string]interface{}{"a": 1.0, "c": 3.0}
	// Administrator adds "b" and changes nested "ext.deep" outside hook paths.
	remote := map[string]interface{}{
		"a":   1.0,
		"b":   9.0,
		"ext": map[string]interface{}{"deep": "kept"},
	}

	merged, conflicts, err := mergeSectionCommit("myModule", baseline, target, remote, []*sdkutils.ValuesPatchOperation{
		opAdd("/myModule/c", "3"),
	})
	require.NoError(t, err)
	assert.Empty(t, conflicts)
	assert.Equal(t, map[string]interface{}{
		"a":   1.0,
		"b":   9.0,
		"c":   3.0,
		"ext": map[string]interface{}{"deep": "kept"},
	}, merged)
}

func Test_mergeSectionCommit_same_path_conflict_keeps_remote(t *testing.T) {
	baseline := map[string]interface{}{"a": 1.0, "keep": "x"}
	target := map[string]interface{}{"a": 2.0, "keep": "x", "new": 3.0}
	// Administrator changed the same path "a", hook also writes a new key.
	remote := map[string]interface{}{"a": 7.0, "keep": "x"}

	merged, conflicts, err := mergeSectionCommit("myModule", baseline, target, remote, []*sdkutils.ValuesPatchOperation{
		opAdd("/myModule/a", "2"),
		opAdd("/myModule/new", "3"),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"/myModule/a"}, conflicts)
	assert.Equal(t, 7.0, merged["a"], "remote fact must win on conflict")
	assert.Equal(t, 3.0, merged["new"], "hook value on other path must apply")
	assert.Equal(t, "x", merged["keep"])
}

func Test_mergeSectionCommit_nested_paths(t *testing.T) {
	baseline := map[string]interface{}{
		"obj": map[string]interface{}{"x": 1.0, "y": 2.0},
	}
	target := map[string]interface{}{
		"obj": map[string]interface{}{"x": 9.0, "y": 2.0},
	}
	// Administrator changed "y" only; hook changed "x": no conflict, both kept.
	remote := map[string]interface{}{
		"obj": map[string]interface{}{"x": 1.0, "y": 3.0},
	}

	merged, conflicts, err := mergeSectionCommit("myModule", baseline, target, remote, []*sdkutils.ValuesPatchOperation{
		opAdd("/myModule/obj/x", "9"),
	})
	require.NoError(t, err)
	assert.Empty(t, conflicts)
	assert.Equal(t, map[string]interface{}{"x": 9.0, "y": 3.0}, merged["obj"])

	// Administrator changed "x" as well: conflict on the nested path.
	remote2 := map[string]interface{}{
		"obj": map[string]interface{}{"x": 42.0, "y": 2.0},
	}
	merged, conflicts, err = mergeSectionCommit("myModule", baseline, target, remote2, []*sdkutils.ValuesPatchOperation{
		opAdd("/myModule/obj/x", "9"),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"/myModule/obj/x"}, conflicts)
	assert.Equal(t, 42.0, merged["obj"].(map[string]interface{})["x"])
}

func Test_mergeSectionCommit_converged_changes(t *testing.T) {
	baseline := map[string]interface{}{"a": 1.0, "b": 2.0}
	target := map[string]interface{}{"a": 2.0}
	// Remote applied exactly the same changes independently: a=2, b removed.
	remote := map[string]interface{}{"a": 2.0}

	merged, conflicts, err := mergeSectionCommit("myModule", baseline, target, remote, []*sdkutils.ValuesPatchOperation{
		opAdd("/myModule/a", "2"),
		opRemove("/myModule/b"),
	})
	require.NoError(t, err)
	assert.Empty(t, conflicts, "converged changes are not conflicts")
	assert.Equal(t, map[string]interface{}{"a": 2.0}, merged)
}

func Test_mergeSectionCommit_remove_operations(t *testing.T) {
	baseline := map[string]interface{}{"a": 1.0, "b": 2.0}
	target := map[string]interface{}{"b": 2.0}

	// Remote already removed "a": converged removal.
	merged, conflicts, err := mergeSectionCommit("myModule", baseline, target, map[string]interface{}{"b": 2.0},
		[]*sdkutils.ValuesPatchOperation{opRemove("/myModule/a")})
	require.NoError(t, err)
	assert.Empty(t, conflicts)
	_, hasA := merged["a"]
	assert.False(t, hasA)

	// Remote changed "a" instead of removing: conflict, remote value kept.
	merged, conflicts, err = mergeSectionCommit("myModule", baseline, target, map[string]interface{}{"a": 5.0, "b": 2.0},
		[]*sdkutils.ValuesPatchOperation{opRemove("/myModule/a")})
	require.NoError(t, err)
	assert.Equal(t, []string{"/myModule/a"}, conflicts)
	assert.Equal(t, 5.0, merged["a"])
}

func Test_mergeSectionCommit_arrays_are_atomic_paths(t *testing.T) {
	baseline := map[string]interface{}{"list": []interface{}{1.0, 2.0}}
	target := map[string]interface{}{"list": []interface{}{1.0, 9.0, 2.0}}

	// Uncontended array insert applies.
	merged, conflicts, err := mergeSectionCommit("myModule", baseline, target, baseline,
		[]*sdkutils.ValuesPatchOperation{opAdd("/myModule/list/1", "9")})
	require.NoError(t, err)
	assert.Empty(t, conflicts)
	assert.Equal(t, []interface{}{1.0, 9.0, 2.0}, merged["list"])

	// Remote modified the same array: conflict keeps the remote array.
	remote := map[string]interface{}{"list": []interface{}{1.0, 3.0}}
	merged, conflicts, err = mergeSectionCommit("myModule", baseline, target, remote,
		[]*sdkutils.ValuesPatchOperation{opAdd("/myModule/list/1", "9")})
	require.NoError(t, err)
	assert.Equal(t, []string{"/myModule/list/1"}, conflicts)
	assert.Equal(t, []interface{}{1.0, 3.0}, merged["list"])
}

func Test_mergeSectionCommit_ignores_ops_for_other_sections(t *testing.T) {
	baseline := map[string]interface{}{"a": 1.0}
	remote := map[string]interface{}{"a": 1.0, "b": 9.0}

	merged, conflicts, err := mergeSectionCommit("myModule", baseline, baseline, remote, []*sdkutils.ValuesPatchOperation{
		opAdd("/otherModule/b", "3"),
	})
	require.NoError(t, err)
	assert.Empty(t, conflicts)
	// Remote content is copied as-is because the hook did not touch this section.
	assert.Equal(t, remote, merged)
}
