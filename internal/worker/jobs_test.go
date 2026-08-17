package worker

import (
	"testing"

	"github.com/cyxc1124/osspilot-tenant-worker/internal/storage"
)

func TestSkipS3(t *testing.T) {
	if !skipS3(nil, "objects:inventory") {
		t.Fatal("nil s3 should skip")
	}
}

func TestOverlayS3(t *testing.T) {
	got := overlayS3(storage.Config{Region: "us-east-1"}, map[string]string{
		"s3_endpoint":    "https://rgw.example",
		"rgw_access_key": "ak",
		"rgw_secret_key": "sk",
	})
	if !got.Ready() || got.Endpoint != "https://rgw.example" {
		t.Fatalf("overlay %+v", got)
	}
}

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
