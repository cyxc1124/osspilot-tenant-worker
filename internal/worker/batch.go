package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"github.com/cyxc1124/osspilot-tenant-worker/internal/objects"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/queue"
)

func (j *Jobs) BatchDelete(ctx context.Context, t *asynq.Task) error {
	s3 := j.client(ctx)
	if skipS3(s3, queue.TaskBatchDelete) {
		return nil
	}
	var p queue.BatchDelete
	if err := json.Unmarshal(t.Payload(), &p); err != nil || strings.TrimSpace(p.BucketName) == "" || len(p.Keys) == 0 {
		return fmt.Errorf("invalid batch delete payload")
	}
	b, err := j.Buckets.GetVisible(ctx, p.AccountID, p.BucketName)
	if err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("bucket %s not found", p.BucketName)
	}
	resolved, failed := objects.ExpandDeleteKeys(ctx, s3, j.Objects, b, p.Keys)
	now := time.Now()
	var ok int
	for _, f := range failed {
		j.audit(ctx, p.UserID, p.AccountID, p.BucketName, f.Key, "delete", "failure", f.Error, p.SourceIP, p.UserAgent)
	}
	for _, key := range resolved {
		if err := objects.ApplyDelete(ctx, s3, j.Objects, b, p.UserID, key, p.Permanent, now); err != nil {
			j.audit(ctx, p.UserID, p.AccountID, p.BucketName, key, "delete", "failure", objects.CopyErr(err), p.SourceIP, p.UserAgent)
			continue
		}
		j.audit(ctx, p.UserID, p.AccountID, p.BucketName, key, "delete", "success", "", p.SourceIP, p.UserAgent)
		ok++
	}
	slog.Info("batch delete done", "bucket", p.BucketName, "deleted", ok, "failed", len(failed)+len(resolved)-ok)
	return nil
}

func (j *Jobs) BatchCopy(ctx context.Context, t *asynq.Task) error {
	s3 := j.client(ctx)
	if skipS3(s3, queue.TaskBatchCopy) {
		return nil
	}
	var p queue.BatchCopy
	if err := json.Unmarshal(t.Payload(), &p); err != nil || strings.TrimSpace(p.BucketName) == "" || len(p.Items) == 0 {
		return fmt.Errorf("invalid batch copy payload")
	}
	src, err := j.Buckets.GetVisible(ctx, p.AccountID, p.BucketName)
	if err != nil {
		return err
	}
	if src == nil {
		return fmt.Errorf("bucket %s not found", p.BucketName)
	}
	now := time.Now()
	var ok int
	for _, it := range p.Items {
		destName := src.BucketName
		if it.DestBucketName != nil && strings.TrimSpace(*it.DestBucketName) != "" {
			destName = strings.TrimSpace(*it.DestBucketName)
		}
		dest, err := j.Buckets.GetVisible(ctx, p.AccountID, destName)
		if err != nil {
			return err
		}
		if dest == nil {
			j.audit(ctx, p.UserID, p.AccountID, destName, it.SourceKey, "copy", "failure", "Bucket not found", p.SourceIP, p.UserAgent)
			continue
		}
		if err := objects.ApplyCopy(ctx, s3, j.Objects, src, dest, p.UserID, it.SourceKey, it.DestKey, now); err != nil {
			j.audit(ctx, p.UserID, p.AccountID, dest.BucketName, it.DestKey, "copy", "failure", objects.CopyErr(err), p.SourceIP, p.UserAgent)
			continue
		}
		j.audit(ctx, p.UserID, p.AccountID, dest.BucketName, it.DestKey, "copy", "success", "", p.SourceIP, p.UserAgent)
		ok++
	}
	slog.Info("batch copy done", "bucket", p.BucketName, "copied", ok)
	return nil
}

func (j *Jobs) BatchMove(ctx context.Context, t *asynq.Task) error {
	s3 := j.client(ctx)
	if skipS3(s3, queue.TaskBatchMove) {
		return nil
	}
	var p queue.BatchMove
	if err := json.Unmarshal(t.Payload(), &p); err != nil || strings.TrimSpace(p.BucketName) == "" || len(p.Items) == 0 {
		return fmt.Errorf("invalid batch move payload")
	}
	b, err := j.Buckets.GetVisible(ctx, p.AccountID, p.BucketName)
	if err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("bucket %s not found", p.BucketName)
	}
	now := time.Now()
	var ok int
	for _, it := range p.Items {
		if err := objects.ApplyMove(ctx, s3, j.Objects, b, p.UserID, it.SourceKey, it.DestKey, now); err != nil {
			j.audit(ctx, p.UserID, p.AccountID, p.BucketName, it.SourceKey, "move", "failure", objects.CopyErr(err), p.SourceIP, p.UserAgent)
			continue
		}
		j.audit(ctx, p.UserID, p.AccountID, p.BucketName, it.DestKey, "move", "success", "", p.SourceIP, p.UserAgent)
		ok++
	}
	slog.Info("batch move done", "bucket", p.BucketName, "moved", ok)
	return nil
}

func (j *Jobs) audit(ctx context.Context, userID, accountID int64, bucket, key, action, status, errMsg, ip, ua string) {
	if j.Log == nil {
		return
	}
	j.Log.RecordMeta(ctx, userID, accountID, bucket, key, action, status, errMsg, ip, ua)
}
