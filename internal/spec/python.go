package spec

import (
	"fmt"
	"regexp"
	"strings"

	"pompos/internal/ingestion"
)

// Accept named registry packages, including extras and version constraints.
// Paths, URLs, pip options and multiline input are deliberately unsupported.
var pythonVersion = regexp.MustCompile(`^3\.[0-9]+(\.[0-9]+)?$`)
var requirement = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*(\[[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*\])?([ ]*(==|!=|~=|>=|<=|>|<)[ ]*[A-Za-z0-9.*+!_-]+([ ]*,[ ]*(==|!=|~=|>=|<=|>|<)[ ]*[A-Za-z0-9.*+!_-]+)*)?$`)
var pinnedRequirement = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*==[A-Za-z0-9.+!_-]+$`)

func ValidatePythonRuntime(python string, dependencies []string, lock *ingestion.DependencyLock) error {
	if python != "" && !pythonVersion.MatchString(python) {
		return fmt.Errorf("runtime.python: use a Python 3 version such as 3.12")
	}
	if len(dependencies) > 64 {
		return fmt.Errorf("runtime.dependencies: at most 64 packages are supported")
	}
	for _, dep := range dependencies {
		if len(dep) > 500 || !requirement.MatchString(dep) {
			return fmt.Errorf("runtime.dependencies: invalid registry requirement %q", dep)
		}
	}
	if lock != nil {
		if !scriptDigest.MatchString(lock.InputDigest) || !pythonVersion.MatchString(lock.Python) || lock.Platform == "" || len(lock.Requirements) == 0 || len(lock.Requirements) > 1024*1024 {
			return fmt.Errorf("runtime.dependencyLock: invalid lock; probe the ingestion again")
		}
		for _, line := range strings.Split(strings.TrimSpace(lock.Requirements), "\n") {
			if !pinnedRequirement.MatchString(strings.TrimSpace(line)) {
				return fmt.Errorf("runtime.dependencyLock: requirements must contain exact package versions")
			}
		}
	}
	return nil
}
