package spec

import (
	"fmt"
	"regexp"
)

// Accept named registry packages, including extras and version constraints.
// Paths, URLs, pip options and multiline input are deliberately unsupported.
var pythonVersion = regexp.MustCompile(`^3\.[0-9]+(\.[0-9]+)?$`)
var requirement = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*(\[[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*\])?([ ]*(==|!=|~=|>=|<=|>|<)[ ]*[A-Za-z0-9.*+!_-]+([ ]*,[ ]*(==|!=|~=|>=|<=|>|<)[ ]*[A-Za-z0-9.*+!_-]+)*)?$`)

func ValidatePythonRuntime(python string, dependencies []string, lockDigest string) error {
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
	if lockDigest != "" && !scriptDigest.MatchString(lockDigest) {
		return fmt.Errorf("runtime.lockDigest: must be a SHA-256 digest")
	}
	return nil
}
