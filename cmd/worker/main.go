package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyxc1124/osspilot-tenant-worker/internal/audit"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/bucket"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/config"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/objects"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/platform"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/queue"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/stats"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/storage"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/uploads"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/versions"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/worker"
)

func main() {
	cfg := config.Load()
	if cfg.RedisURL == "" {
		slog.Error("REDIS_URL is required for the inventory worker")
		os.Exit(1)
	}
	if cfg.DatabaseURL == "" {
		slog.Error("DATABASE_URL is required for the inventory worker")
		os.Exit(1)
	}
	s3cfg := storage.Config{
		Endpoint:  cfg.S3Endpoint,
		AccessKey: cfg.RGWAccessKey,
		SecretKey: cfg.RGWSecretKey,
		Region:    cfg.S3Region,
	}
	var s3 *storage.Client
	if s3cfg.Ready() {
		s3 = storage.New(s3cfg)
	} else {
		slog.Info("S3 not configured; object tasks will skip until S3_ENDPOINT/RGW_ACCESS_KEY/RGW_SECRET_KEY are set")
	}

	redisOpt, err := asynq.ParseRedisURI(cfg.RedisURL)
	if err != nil {
		slog.Error("REDIS_URL", "err", err)
		os.Exit(1)
	}

	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	jobs := &worker.Jobs{
		Buckets:  bucket.NewStore(pool),
		Objects:  objects.NewStore(pool),
		Versions: versions.NewStore(pool),
		Uploads:  uploads.NewStore(pool),
		Settings: platform.NewStore(pool),
		Stats:    stats.NewStore(pool),
		S3:       s3,
		Log:      audit.NewLogger(pool),
	}

	mux := asynq.NewServeMux()
	mux.HandleFunc(worker.TaskInventory, jobs.Inventory)
	mux.HandleFunc(worker.TaskInventoryBucket, jobs.InventoryBucket)
	mux.HandleFunc(worker.TaskTrash, jobs.Trash)
	mux.HandleFunc(worker.TaskVersions, jobs.CleanVersions)
	mux.HandleFunc(worker.TaskMultipart, jobs.CleanMultipart)
	mux.HandleFunc(queue.TaskBatchDelete, jobs.BatchDelete)
	mux.HandleFunc(queue.TaskBatchCopy, jobs.BatchCopy)
	mux.HandleFunc(queue.TaskBatchMove, jobs.BatchMove)
	mux.HandleFunc(worker.TaskRequestStats, jobs.RequestStats)

	srv := asynq.NewServer(redisOpt, asynq.Config{Concurrency: 2})
	scheduler := asynq.NewScheduler(redisOpt, nil)
	if _, err := scheduler.Register("@every 15m", asynq.NewTask(worker.TaskInventory, nil)); err != nil {
		slog.Error("schedule inventory", "err", err)
		os.Exit(1)
	}
	if _, err := scheduler.Register("@every 1h", asynq.NewTask(worker.TaskTrash, nil)); err != nil {
		slog.Error("schedule trash", "err", err)
		os.Exit(1)
	}
	if _, err := scheduler.Register("@every 6h", asynq.NewTask(worker.TaskVersions, nil)); err != nil {
		slog.Error("schedule versions", "err", err)
		os.Exit(1)
	}
	if _, err := scheduler.Register("@every 6h", asynq.NewTask(worker.TaskMultipart, nil)); err != nil {
		slog.Error("schedule multipart", "err", err)
		os.Exit(1)
	}
	if _, err := scheduler.Register("@every 1h", asynq.NewTask(worker.TaskRequestStats, nil)); err != nil {
		slog.Error("schedule request stats", "err", err)
		os.Exit(1)
	}

	go func() {
		if err := scheduler.Run(); err != nil {
			slog.Error("scheduler", "err", err)
			os.Exit(1)
		}
	}()

	client := asynq.NewClient(redisOpt)
	defer client.Close()
	if s3 != nil {
		if _, err := client.Enqueue(asynq.NewTask(worker.TaskInventory, nil), asynq.MaxRetry(3), asynq.Timeout(time.Hour)); err != nil {
			slog.Warn("enqueue startup inventory", "err", err)
		}
	}
	if _, err := client.Enqueue(asynq.NewTask(worker.TaskRequestStats, nil), asynq.MaxRetry(3), asynq.Timeout(10*time.Minute)); err != nil {
		slog.Warn("enqueue startup request stats", "err", err)
	}

	slog.Info("worker listen")
	if err := srv.Run(mux); err != nil {
		slog.Error("worker", "err", err)
		os.Exit(1)
	}
}
