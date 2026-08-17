package objects

import (
	"context"
	"errors"
	"time"

	"github.com/cyxc1124/osspilot-tenant-worker/internal/bucket"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/storage"
)

func ApplyDelete(ctx context.Context, s3 *storage.Client, store *Store, b *bucket.Bucket, userID int64, key string, permanent bool, now time.Time) error {
	meta, err := s3.HeadObject(ctx, b.BucketName, key)
	if err != nil {
		return err
	}
	if permanent {
		if err := s3.DeleteObject(ctx, b.BucketName, key); err != nil {
			return err
		}
		return store.Delete(ctx, b.ID, key)
	}
	trashKey := ToTrashKey(key)
	if _, err := s3.CopyObject(ctx, b.BucketName, trashKey, b.BucketName, key); err != nil {
		return err
	}
	if err := s3.DeleteObject(ctx, b.BucketName, key); err != nil {
		return err
	}
	return store.MoveKey(ctx, b.ID, b.BucketName, key, trashKey, meta.Size, meta.ETag, meta.ContentType, userID, now)
}

var ErrDestExists = errors.New("Destination already exists")

func rejectIfExists(ctx context.Context, s3 *storage.Client, bucket, key string) error {
	_, err := s3.HeadObject(ctx, bucket, key)
	if err == nil {
		return ErrDestExists
	}
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	return err
}

func ApplyCopy(ctx context.Context, s3 *storage.Client, store *Store, src, dest *bucket.Bucket, userID int64, srcKey, destKey string, now time.Time) error {
	if err := rejectIfExists(ctx, s3, dest.BucketName, destKey); err != nil {
		return err
	}
	meta, err := s3.HeadObject(ctx, src.BucketName, srcKey)
	if err != nil {
		return err
	}
	etag, err := s3.CopyObject(ctx, dest.BucketName, destKey, src.BucketName, srcKey)
	if err != nil {
		return err
	}
	return store.Upsert(ctx, dest.ID, dest.BucketName, destKey, meta.Size, etag, meta.ContentType, userID, now)
}

func ApplyMove(ctx context.Context, s3 *storage.Client, store *Store, b *bucket.Bucket, userID int64, src, dest string, now time.Time) error {
	if src != dest {
		if err := rejectIfExists(ctx, s3, b.BucketName, dest); err != nil {
			return err
		}
	}
	meta, err := s3.HeadObject(ctx, b.BucketName, src)
	if err != nil {
		return err
	}
	etag, err := s3.CopyObject(ctx, b.BucketName, dest, b.BucketName, src)
	if err != nil {
		return err
	}
	if err := s3.DeleteObject(ctx, b.BucketName, src); err != nil {
		return err
	}
	return store.MoveKey(ctx, b.ID, b.BucketName, src, dest, meta.Size, etag, meta.ContentType, userID, now)
}

func ExpandDeleteKeys(ctx context.Context, s3 *storage.Client, store *Store, b *bucket.Bucket, keys []string) ([]string, []opFailure) {
	seen := make(map[string]struct{}, len(keys))
	resolved := make([]string, 0, len(keys))
	var failed []opFailure
	for _, key := range keys {
		if !ValidUserKey(key) {
			failed = append(failed, opFailure{Key: key, Error: errInvalidKey.Error()})
			continue
		}
		if !IsDirectoryKey(key) {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			resolved = append(resolved, key)
			continue
		}
		children, err := store.ListLiveKeys(ctx, b.ID, key)
		if err != nil {
			failed = append(failed, opFailure{Key: key, Error: "database error"})
			continue
		}
		if len(children) == 0 {
			if _, err := s3.HeadObject(ctx, b.BucketName, key); err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					failed = append(failed, opFailure{Key: key, Error: "Directory not found or empty"})
				} else {
					failed = append(failed, opFailure{Key: key, Error: "storage error"})
				}
				continue
			}
			children = []string{key}
		}
		for _, child := range children {
			if _, ok := seen[child]; ok {
				continue
			}
			seen[child] = struct{}{}
			resolved = append(resolved, child)
		}
	}
	if failed == nil {
		failed = []opFailure{}
	}
	return resolved, failed
}

type opFailure struct {
	Key   string `json:"key"`
	Error string `json:"error"`
}

func CopyErr(err error) string {
	if errors.Is(err, ErrDestExists) {
		return ErrDestExists.Error()
	}
	if errors.Is(err, storage.ErrNotFound) {
		return "Object not found"
	}
	if err == nil {
		return "unknown error"
	}
	return err.Error()
}
