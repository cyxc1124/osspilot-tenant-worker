package objects

import "strings"

const (
	defaultMaxKeys = 100
	maxListKeys    = 1000
	maxOpKeys      = 1000
	// ponytail: fold a bounded scan instead of S3 ListObjectsV2; T4 can switch to RGW.
	scanCap = 2000
)

type record struct {
	Key          string
	Size         int64
	ContentType  *string
	ETag         *string
	StorageClass *string
	UploadedBy   *int64
	LastModified *string
	CreatedAt    *string
	UpdatedAt    *string
	Username     *string
}

func foldList(prefix string, maxKeys int, recs []record, moreInDB bool) (items []record, prefixes []string, token *string, truncated bool) {
	if maxKeys < 1 {
		maxKeys = 1
	}
	items = []record{}
	prefixes = []string{}
	lastPrefix := ""
	tokenKey := ""
	n := 0
	for _, rec := range recs {
		if strings.HasPrefix(rec.Key, TrashPrefix) || strings.HasPrefix(rec.Key, VersionPrefix) {
			tokenKey = rec.Key
			continue
		}
		if prefix != "" && !strings.HasPrefix(rec.Key, prefix) {
			tokenKey = rec.Key
			continue
		}
		rest := rec.Key[len(prefix):]
		if rest == "" {
			tokenKey = rec.Key
			continue
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			p := prefix + rest[:i+1]
			if p == lastPrefix {
				tokenKey = rec.Key
				continue
			}
			if n >= maxKeys {
				truncated = true
				break
			}
			prefixes = append(prefixes, p)
			lastPrefix = p
			n++
			tokenKey = rec.Key
			continue
		}
		lastPrefix = ""
		if n >= maxKeys {
			truncated = true
			break
		}
		items = append(items, rec)
		n++
		tokenKey = rec.Key
	}
	if !truncated && moreInDB {
		truncated = true
	}
	if truncated && tokenKey != "" {
		token = &tokenKey
	}
	return
}

func likePrefix(prefix string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(prefix) + "%"
}
