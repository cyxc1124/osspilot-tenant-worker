package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"github.com/cyxc1124/osspilot-tenant-worker/internal/audit"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/bucket"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/objects"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/platform"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/queue"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/stats"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/storage"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/uploads"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/versions"
)

const (
	TaskInventory       = "objects:inventory"
	TaskInventoryBucket = queue.TaskInventoryBucket
	TaskTrash           = "objects:trash"
	TaskVersions        = "objects:versions"
	TaskMultipart       = "objects:multipart"
	TaskRequestStats    = "stats:requests"
)

type Jobs struct {
	Buckets  *bucket.Store
	Objects  *objects.Store
	Versions *versions.Store
	Uploads  *uploads.Store
	Settings *platform.Store
	Stats    *stats.Store
	S3       *storage.Client
	Log      *audit.Logger
}

func skipS3(s3 *storage.Client, task string) bool {
	if s3 != nil {
		return false
	}
	slog.Info("skipped, S3 not configured", "task", task)
	return true
}

func (j *Jobs) Inventory(ctx context.Context, _ *asynq.Task) error {
	if skipS3(j.S3, TaskInventory) {
		return nil
	}
	items, err := j.Buckets.ListActive(ctx)
	if err != nil {
		return err
	}
	for _, b := range items {
		if err := j.scanBucket(ctx, b); err != nil {
			return fmt.Errorf("inventory %s: %w", b.BucketName, err)
		}
	}
	slog.Info("inventory done", "buckets", len(items))
	return nil
}

func (j *Jobs) InventoryBucket(ctx context.Context, t *asynq.Task) error {
	if skipS3(j.S3, TaskInventoryBucket) {
		return nil
	}
	var req struct {
		BucketName string `json:"bucket_name"`
	}
	if err := json.Unmarshal(t.Payload(), &req); err != nil || strings.TrimSpace(req.BucketName) == "" {
		return fmt.Errorf("invalid inventory payload")
	}
	b, err := j.Buckets.GetByName(ctx, req.BucketName)
	if err != nil {
		return err
	}
	if b == nil || b.Status != "active" {
		return fmt.Errorf("bucket %s not found", req.BucketName)
	}
	if err := j.scanBucket(ctx, *b); err != nil {
		return fmt.Errorf("inventory %s: %w", req.BucketName, err)
	}
	slog.Info("inventory bucket done", "bucket", req.BucketName)
	return nil
}

func (j *Jobs) scanBucket(ctx context.Context, b bucket.Bucket) error {
	started := time.Now().UTC()
	token := ""
	for {
		page, err := j.S3.ListObjects(ctx, b.BucketName, token, 1000)
		if err != nil {
			return err
		}
		for _, obj := range page.Objects {
			if obj.Key == "" {
				continue
			}
			if err := j.Objects.UpsertSeen(ctx, b.ID, b.BucketName, obj.Key, obj.Size, obj.ETag, obj.StorageClass, started); err != nil {
				return err
			}
		}
		if !page.Truncated || page.Token == "" {
			break
		}
		token = page.Token
	}
	return j.Objects.PurgeUnseen(ctx, b.ID, started)
}

func (j *Jobs) Trash(ctx context.Context, _ *asynq.Task) error {
	if skipS3(j.S3, TaskTrash) {
		return nil
	}
	if j.Settings == nil {
		return nil
	}
	days, enabled, err := j.Settings.TrashPolicy(ctx)
	if err != nil {
		return err
	}
	if !shouldCleanupTrash(enabled, days) {
		slog.Info("trash cleanup skipped", "enabled", enabled, "days", days)
		return nil
	}
	items, err := j.Objects.ExpiredTrash(ctx, days)
	if err != nil {
		return err
	}
	var deleted int
	for _, item := range items {
		if err := j.S3.DeleteObject(ctx, item.BucketName, item.Key); err != nil {
			slog.Warn("trash delete storage", "bucket", item.BucketName, "key", item.Key, "err", err)
			continue
		}
		if err := j.Objects.Delete(ctx, item.BucketID, item.Key); err != nil {
			return err
		}
		deleted++
	}
	slog.Info("trash cleanup done", "deleted", deleted)
	return nil
}

func shouldCleanupTrash(enabled bool, days int) bool {
	return enabled && days >= 1
}

func (j *Jobs) CleanVersions(ctx context.Context, _ *asynq.Task) error {
	if skipS3(j.S3, TaskVersions) {
		return nil
	}
	if j.Settings == nil || j.Versions == nil {
		return nil
	}
	days, enabled, err := j.Settings.VersionPolicy(ctx)
	if err != nil {
		return err
	}
	if !shouldCleanupTrash(enabled, days) {
		slog.Info("version cleanup skipped", "enabled", enabled, "days", days)
		return nil
	}
	items, err := j.Versions.Expired(ctx, days)
	if err != nil {
		return err
	}
	var deleted int
	for _, item := range items {
		if err := j.S3.DeleteObject(ctx, item.BucketName, item.StorageKey); err != nil {
			slog.Warn("version delete storage", "bucket", item.BucketName, "key", item.StorageKey, "err", err)
			continue
		}
		if err := j.Versions.Delete(ctx, item.ID); err != nil {
			return err
		}
		deleted++
	}
	slog.Info("version cleanup done", "deleted", deleted)
	return nil
}

func (j *Jobs) CleanMultipart(ctx context.Context, _ *asynq.Task) error {
	if skipS3(j.S3, TaskMultipart) {
		return nil
	}
	if j.Settings == nil || j.Uploads == nil {
		return nil
	}
	days, enabled, err := j.Settings.MultipartPolicy(ctx)
	if err != nil {
		return err
	}
	if !shouldCleanupTrash(enabled, days) {
		slog.Info("multipart cleanup skipped", "enabled", enabled, "days", days)
		return nil
	}
	items, err := j.Uploads.StaleMultipart(ctx, days)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	var aborted int
	for _, item := range items {
		if item.UploadID != nil && *item.UploadID != "" {
			if err := j.S3.AbortMultipart(ctx, item.BucketName, item.ObjectKey, *item.UploadID); err != nil {
				slog.Warn("multipart abort storage", "bucket", item.BucketName, "key", item.ObjectKey, "err", err)
				continue
			}
		}
		if err := j.Uploads.Finish(ctx, item.ID, uploads.StatusAbort, now); err != nil {
			return err
		}
		aborted++
	}
	slog.Info("multipart cleanup done", "aborted", aborted)
	return nil
}

func (j *Jobs) RequestStats(ctx context.Context, _ *asynq.Task) error {
	if j.Stats == nil {
		return nil
	}
	res, err := j.Stats.AggregateRequests(ctx, time.Now())
	if err != nil {
		return err
	}
	slog.Info("request stats done",
		"periods", res.Periods, "accounts", res.Accounts, "buckets", res.Buckets,
		"users", res.Users, "prefixes", res.Prefixes, "daily", res.Daily)
	return nil
}
