package python

import (
	_ "embed"
	"fmt"
)

const DefaultObjectValidationBytes int64 = 50 * 1024 * 1024

// ObjectValidationBudget bounds temporary downloads, while allowing large videos.
func ObjectValidationBudget(maxBytes int64, seconds int) (int64, int, error) {
	if maxBytes == 0 {
		maxBytes = DefaultObjectValidationBytes
	}
	if seconds == 0 {
		seconds = 90
	}
	if maxBytes < 1 || maxBytes > 10*1024*1024*1024 || seconds < 1 || seconds > 1800 {
		return 0, 0, fmt.Errorf("file validation requires max_bytes between 1 and 10737418240 and timeout_seconds between 1 and 1800")
	}
	return maxBytes, seconds, nil
}

//go:embed objects.py
var objectsEntrypoint string

func WrapObjectsWithRuntime(code, python string, dependencies []string) string {
	return ScriptMetadata(python, dependencies) + code + "\n\n" + objectsEntrypoint
}
