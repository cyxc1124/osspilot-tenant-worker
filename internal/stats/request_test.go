package stats

import "testing"

func TestFirstLevelPrefix(t *testing.T) {
	if firstLevelPrefix("docs/readme.txt") != "docs/" {
		t.Fatal(firstLevelPrefix("docs/readme.txt"))
	}
	if firstLevelPrefix("readme.txt") != "" {
		t.Fatal("file")
	}
	if firstLevelPrefix(".trash/docs/a.txt") != "" {
		t.Fatal("trash")
	}
}

func TestIsObjectRequest(t *testing.T) {
	if !isObjectRequest("upload") || !isObjectRequest("download") || !isObjectRequest("copy") {
		t.Fatal("object")
	}
	if isObjectRequest("bucket_create") || isObjectRequest("login_failed") {
		t.Fatal("skip")
	}
}

func TestCounterAdd(t *testing.T) {
	c := &counter{}
	uid := int64(5)
	c.add("download", "success", 1024, &uid)
	c.add("upload", "success", 512, &uid)
	c.add("delete", "failure", 0, &uid)
	if c.requestCount != 3 || c.getCount != 1 || c.putCount != 1 || c.deleteCount != 1 {
		t.Fatalf("%+v", c)
	}
	if c.downloadBytes != 1024 || c.uploadBytes != 512 || c.errorCount != 1 || len(c.active) != 1 {
		t.Fatalf("bytes %+v", c)
	}
	u := &userCounter{}
	u.add("download", 1024, "docs/a.txt")
	u.add("upload", 512, "docs/a.txt")
	if u.accessCount != 2 || u.downloadCount != 1 || u.uploadCount != 1 {
		t.Fatalf("%+v", u)
	}
}
