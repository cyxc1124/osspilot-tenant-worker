package buildinfo

import "testing"

func TestCurrentPrefersTag(t *testing.T) {
	t.Setenv("GIT_TAG", "v1.0.1")
	t.Setenv("GIT_BRANCH", "develop")
	t.Setenv("GIT_COMMIT", "abcdef12ffff")
	got := Current()
	if got.Version != "v1.0.1" || got.GitCommit != "abcdef12" {
		t.Fatalf("%#v", got)
	}
}
