package worker

import "testing"

func TestShouldCleanupTrash(t *testing.T) {
	if shouldCleanupTrash(false, 7) {
		t.Fatal("disabled")
	}
	if shouldCleanupTrash(true, 0) {
		t.Fatal("zero days")
	}
	if !shouldCleanupTrash(true, 7) {
		t.Fatal("enabled")
	}
}
