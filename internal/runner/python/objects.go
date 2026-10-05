package python

import (
	_ "embed"
	"fmt"
)

// ObjectValidationBudget accepts agent-selected budgets; zero means unlimited.
func ObjectValidationBudget(maxBytes int64, seconds int) (int64, int, error) {
	if maxBytes < 0 || seconds < 0 {
		return 0, 0, fmt.Errorf("file validation budgets must be nonnegative; 0 means unlimited")
	}
	return maxBytes, seconds, nil
}

//go:embed objects.py
var objectsEntrypoint string

func WrapObjectsWithRuntime(code, python string, dependencies []string) string {
	return ScriptMetadata(python, dependencies) + code + "\n\n" + destinationLockScript + "\n\n" + objectsEntrypoint
}
