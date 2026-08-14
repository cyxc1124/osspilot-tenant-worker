package objects

import "testing"

func TestFoldList(t *testing.T) {
	recs := []record{
		{Key: "a.txt"},
		{Key: "docs/readme.md"},
		{Key: "docs/a/b.txt"},
		{Key: "z.txt"},
	}
	items, prefixes, token, truncated := foldList("", 10, recs, false)
	if truncated || token != nil {
		t.Fatalf("truncated=%v token=%v", truncated, token)
	}
	if len(items) != 2 || items[0].Key != "a.txt" || items[1].Key != "z.txt" {
		t.Fatalf("items %#v", items)
	}
	if len(prefixes) != 1 || prefixes[0] != "docs/" {
		t.Fatalf("prefixes %#v", prefixes)
	}

	items, prefixes, _, _ = foldList("docs/", 10, recs[1:3], false)
	if len(prefixes) != 1 || prefixes[0] != "docs/a/" {
		t.Fatalf("nested prefixes %#v", prefixes)
	}
	if len(items) != 1 || items[0].Key != "docs/readme.md" {
		t.Fatalf("nested items %#v", items)
	}

	_, _, token, truncated = foldList("", 1, recs, false)
	if !truncated || token == nil || *token != "a.txt" {
		t.Fatalf("page token=%v truncated=%v", token, truncated)
	}
}

func TestLikePrefixEscapes(t *testing.T) {
	if got := likePrefix(`a%b_c\d`); got != `a\%b\_c\\d%` {
		t.Fatal(got)
	}
}
