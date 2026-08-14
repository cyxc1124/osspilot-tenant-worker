package objects

import "testing"

func TestValidUserKey(t *testing.T) {
	if !ValidUserKey("docs/a.txt") {
		t.Fatal("docs/a.txt")
	}
	if !ValidUserKey("docs/") {
		t.Fatal("docs/")
	}
	for _, k := range []string{"", ".trash", ".versions", ".trash/a", ".versions/x"} {
		if ValidUserKey(k) {
			t.Fatalf("accepted %q", k)
		}
	}
}

func TestTrashKeyRoundTrip(t *testing.T) {
	if got := ToTrashKey("docs/a.txt"); got != ".trash/docs/a.txt" {
		t.Fatal(got)
	}
	orig, ok := FromTrashKey(".trash/docs/a.txt")
	if !ok || orig != "docs/a.txt" {
		t.Fatalf("orig=%q ok=%v", orig, ok)
	}
	if _, ok := FromTrashKey(".trash/"); ok {
		t.Fatal("empty original")
	}
	if _, ok := FromTrashKey("docs/a.txt"); ok {
		t.Fatal("live key")
	}
}

func TestIsDirectoryKey(t *testing.T) {
	if !IsDirectoryKey("docs/") || IsDirectoryKey("docs") || IsDirectoryKey("/") {
		t.Fatal("directory key")
	}
}

func TestUniqueOriginalKeys(t *testing.T) {
	got, err := uniqueOriginalKeys([]string{"a.txt", "b.txt", "a.txt"})
	if err != nil || len(got) != 2 || got[0] != "a.txt" || got[1] != "b.txt" {
		t.Fatalf("got=%v err=%v", got, err)
	}
	if _, err := uniqueOriginalKeys([]string{""}); err != errInvalidKey {
		t.Fatalf("empty: %v", err)
	}
	if _, err := uniqueOriginalKeys([]string{".trash/a.txt"}); err != errTrashPath {
		t.Fatalf("trash path: %v", err)
	}
}
