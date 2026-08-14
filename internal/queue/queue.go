package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"
)

const (
	TaskInventoryBucket = "objects:inventory_bucket"
	TaskBatchDelete     = "objects:batch_delete"
	TaskBatchCopy       = "objects:batch_copy"
	TaskBatchMove       = "objects:batch_move"
)

var ErrUnavailable = errors.New("task queue unavailable")

type CopyItem struct {
	SourceKey      string  `json:"source_key"`
	DestKey        string  `json:"dest_key"`
	DestBucketName *string `json:"dest_bucket_name,omitempty"`
}

type BatchDelete struct {
	AccountID  int64    `json:"account_id"`
	UserID     int64    `json:"user_id"`
	BucketName string   `json:"bucket_name"`
	Keys       []string `json:"keys"`
	Permanent  bool     `json:"permanent"`
	SourceIP   string   `json:"source_ip,omitempty"`
	UserAgent  string   `json:"user_agent,omitempty"`
}

type BatchCopy struct {
	AccountID  int64      `json:"account_id"`
	UserID     int64      `json:"user_id"`
	BucketName string     `json:"bucket_name"`
	Items      []CopyItem `json:"items"`
	SourceIP   string     `json:"source_ip,omitempty"`
	UserAgent  string     `json:"user_agent,omitempty"`
}

type BatchMove struct {
	AccountID  int64      `json:"account_id"`
	UserID     int64      `json:"user_id"`
	BucketName string     `json:"bucket_name"`
	Items      []CopyItem `json:"items"`
	SourceIP   string     `json:"source_ip,omitempty"`
	UserAgent  string     `json:"user_agent,omitempty"`
}

type Client struct {
	asynq *asynq.Client
}

func New(redisURL string) *Client {
	redisURL = strings.TrimSpace(redisURL)
	if redisURL == "" {
		return nil
	}
	opt, err := asynq.ParseRedisURI(redisURL)
	if err != nil {
		return nil
	}
	return &Client{asynq: asynq.NewClient(opt)}
}

func (c *Client) Close() error {
	if c == nil || c.asynq == nil {
		return nil
	}
	return c.asynq.Close()
}

func (c *Client) EnqueueInventory(ctx context.Context, bucketName string) (string, error) {
	if c == nil || c.asynq == nil {
		return "", ErrUnavailable
	}
	bucketName = strings.TrimSpace(bucketName)
	if bucketName == "" {
		return "", fmt.Errorf("bucket_name is required")
	}
	payload, err := json.Marshal(map[string]string{"bucket_name": bucketName})
	if err != nil {
		return "", err
	}
	info, err := c.asynq.EnqueueContext(ctx, asynq.NewTask(TaskInventoryBucket, payload),
		asynq.MaxRetry(3), asynq.Timeout(time.Hour), asynq.Queue("default"))
	if err != nil {
		return "", err
	}
	return info.ID, nil
}

func (c *Client) EnqueueBatchDelete(ctx context.Context, p BatchDelete) (string, error) {
	return c.enqueueJSON(ctx, TaskBatchDelete, p)
}

func (c *Client) EnqueueBatchCopy(ctx context.Context, p BatchCopy) (string, error) {
	return c.enqueueJSON(ctx, TaskBatchCopy, p)
}

func (c *Client) EnqueueBatchMove(ctx context.Context, p BatchMove) (string, error) {
	return c.enqueueJSON(ctx, TaskBatchMove, p)
}

func (c *Client) enqueueJSON(ctx context.Context, typename string, p any) (string, error) {
	if c == nil || c.asynq == nil {
		return "", ErrUnavailable
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	info, err := c.asynq.EnqueueContext(ctx, asynq.NewTask(typename, payload),
		asynq.MaxRetry(1), asynq.Timeout(2*time.Hour), asynq.Queue("default"))
	if err != nil {
		return "", err
	}
	return info.ID, nil
}
