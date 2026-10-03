package agent

import (
	"encoding/json"
	"sort"

	"pompos/internal/spec"
)

func runtimeDigest(d *Draft) string {
	data, _ := json.Marshal(struct {
		Request string
		Lock    any
	}{runtimeRequest(d), d.DependencyLock})
	return spec.Digest(data)
}

func runtimeRequest(d *Draft) string {
	dependencies := append([]string(nil), d.Dependencies...)
	sort.Strings(dependencies)
	data, _ := json.Marshal(struct {
		Python       string
		Dependencies []string
	}{d.Python, dependencies})
	return string(data)
}

func sameRuntimeRequest(a, b *Draft) bool { return runtimeRequest(a) == runtimeRequest(b) }
