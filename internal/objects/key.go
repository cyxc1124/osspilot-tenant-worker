package objects

import (
	"errors"
	"strings"
)

const (
	TrashPrefix   = ".trash/"
	VersionPrefix = ".versions/"
)

var (
	errInvalidKey = errors.New("Invalid object key")
	errTrashPath  = errors.New("Pass original keys, not trash paths")
)

func ValidUserKey(key string) bool {
	if key == "" || key == ".trash" || key == ".versions" {
		return false
	}
	return !strings.HasPrefix(key, TrashPrefix) && !strings.HasPrefix(key, VersionPrefix)
}

func ToTrashKey(key string) string {
	return TrashPrefix + key
}

func FromTrashKey(key string) (string, bool) {
	if !strings.HasPrefix(key, TrashPrefix) {
		return "", false
	}
	orig := key[len(TrashPrefix):]
	if orig == "" {
		return "", false
	}
	return orig, true
}

func IsDirectoryKey(key string) bool {
	return strings.HasSuffix(key, "/") && strings.Trim(key, "/") != ""
}

func uniqueOriginalKeys(keys []string) ([]string, error) {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			return nil, errInvalidKey
		}
		if strings.HasPrefix(key, TrashPrefix) {
			return nil, errTrashPath
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out, nil
}
