package configmap

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	sdkutils "github.com/deckhouse/module-sdk/pkg/utils"

	"github.com/flant/addon-operator/pkg/utils"
)

// jsonNode is a value addressed by an RFC 6901 pointer together with its
// existence flag. Generic values produced by encoding/json are used so that
// numbers unify to float64 regardless of their YAML/JSON origin.
type jsonNode struct {
	value interface{}
	ok    bool
}

// mergeSectionCommit performs a three-way merge of a single config section.
//
// baseline is the section content the hook read, target is the section content
// after applying hook operations and remote is the section content currently
// stored in the ConfigMap. All maps are expected to be plain section values
// without the root key.
//
// Only paths touched by the hook (patch operations rooted at rootKey, e.g.
// "/global" or "/myModule") participate in the merge:
//   - if remote changed a path since the baseline and the remote value differs
//     from the hook target, it is a conflict: the remote fact is kept and the
//     path is reported;
//   - otherwise the hook target is applied;
//   - paths the hook never touched are copied verbatim from remote, so
//     concurrent external updates survive.
//
// Operations are applied to the remote document in their original order, so
// multi-operation patches (e.g. creating an object and its fields) behave the
// same as in the uncontended path.
func mergeSectionCommit(rootKey string, baseline, target, remote map[string]interface{}, ops []*sdkutils.ValuesPatchOperation) (map[string]interface{}, []string, error) {
	bDoc, err := normalizeSection(rootKey, baseline)
	if err != nil {
		return nil, nil, fmt.Errorf("normalize baseline: %w", err)
	}

	tDoc, err := normalizeSection(rootKey, target)
	if err != nil {
		return nil, nil, fmt.Errorf("normalize target: %w", err)
	}

	mergedDoc, err := normalizeSection(rootKey, remote)
	if err != nil {
		return nil, nil, fmt.Errorf("normalize remote: %w", err)
	}

	// Operations must be applied in the exact order the hook emitted them:
	// later operations can depend on values created by earlier ones and
	// reordering would produce spurious structural conflicts.
	var conflicts []string

	for _, op := range ops {
		if sectionRootOf(op.Path) != rootKey {
			continue
		}

		bNode := getJSONPointer(bDoc, op.Path)
		rNode := getJSONPointer(mergedDoc, op.Path)
		tNode := getJSONPointer(tDoc, op.Path)

		switch {
		case nodesEqual(rNode, tNode):
			// Remote already has (or already lacks) exactly what the hook
			// wants: the changes converged, nothing to do.
			continue
		case !nodesEqual(bNode, rNode):
			// Remote changed the path since the read baseline and its value is
			// not the hook target: a real same-path conflict. Keep the remote
			// fact and let the section recalculate.
			conflicts = append(conflicts, op.Path)
			continue
		}

		// Remote matches the baseline at this path: apply the hook operation.
		applied, applyErr := applySingleOperation(mergedDoc, op)
		if applyErr != nil {
			// Structural mismatch (e.g. parent node is a scalar): treat it as
			// a conflict, remote value stays intact.
			conflicts = append(conflicts, op.Path)

			continue
		}

		mergedDoc = applied
	}

	section, _ := mergedDoc[rootKey].(map[string]interface{})
	if section == nil {
		section = map[string]interface{}{}
	}

	return section, conflicts, nil
}

// normalizeSection wraps a plain section map under its root key and converts
// the whole document to canonical encoding/json types.
func normalizeSection(rootKey string, section map[string]interface{}) (map[string]interface{}, error) {
	var wrapped interface{}
	if len(section) > 0 {
		wrapped = map[string]interface{}{rootKey: section}
	} else {
		wrapped = map[string]interface{}{rootKey: map[string]interface{}{}}
	}

	data, err := json.Marshal(wrapped)
	if err != nil {
		return nil, err
	}

	doc := make(map[string]interface{})
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	return doc, nil
}

// normalizeValues canonicalizes arbitrary values through a JSON round-trip.
func normalizeValues(in interface{}) (interface{}, error) {
	data, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}

	var out interface{}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}

	return out, nil
}

// sectionRootOf returns the first path segment of an RFC 6901 pointer
// ("/myModule/foo" -> "myModule").
func sectionRootOf(pointer string) string {
	parts := strings.Split(pointer, "/")
	if len(parts) < 2 {
		return ""
	}

	return decodeJSONPointerToken(parts[1])
}

// getJSONPointer reads a value at an RFC 6901 pointer.
func getJSONPointer(doc interface{}, pointer string) jsonNode {
	if pointer == "" || pointer == "/" {
		return jsonNode{value: doc, ok: true}
	}

	current := doc

	parts := strings.Split(pointer, "/")
	for _, raw := range parts[1:] {
		token := decodeJSONPointerToken(raw)

		switch node := current.(type) {
		case map[string]interface{}:
			value, ok := node[token]
			if !ok {
				return jsonNode{}
			}

			current = value
		case []interface{}:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(node) {
				return jsonNode{}
			}

			current = node[index]
		default:
			return jsonNode{}
		}
	}

	return jsonNode{value: current, ok: true}
}

func decodeJSONPointerToken(token string) string {
	token = strings.ReplaceAll(token, "~1", "/")
	token = strings.ReplaceAll(token, "~0", "~")

	return token
}

// nodesEqual reports equality of two pointer reads, treating a missing node
// and a JSON-null node as different states.
func nodesEqual(a, b jsonNode) bool {
	if a.ok != b.ok {
		return false
	}

	return reflect.DeepEqual(a.value, b.value)
}

// applySingleOperation applies one patch operation to the document, returning
// a new document map. A failure means the document structure does not allow
// the operation (e.g. an intermediate node is a scalar).
func applySingleOperation(doc map[string]interface{}, op *sdkutils.ValuesPatchOperation) (map[string]interface{}, error) {
	single := utils.ValuesPatch{Operations: []*sdkutils.ValuesPatchOperation{op}}

	jsonDoc, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	applied, err := single.ApplyStrict(jsonDoc)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	if err := json.Unmarshal(applied, &result); err != nil {
		return nil, err
	}

	return result, nil
}
