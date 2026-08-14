package storage

import "testing"

func TestRewriteCDN(t *testing.T) {
	in := "https://rgw.example/bucket/key?X-Amz-Signature=1"
	got := rewriteCDN(in, "https://cdn.example")
	if got != "https://cdn.example/bucket/key?X-Amz-Signature=1" {
		t.Fatalf("got %q", got)
	}
	if rewriteCDN(in, "") != in {
		t.Fatal("empty cdn")
	}
}
