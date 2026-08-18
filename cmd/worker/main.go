package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyxc1124/osspilot-tenant-worker/internal/audit"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/bucket"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/config"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/logx"
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
	logx.Setup("osspilot-tenant-worker")
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
		S3FB:     s3cfg,
		Log:      audit.NewLogger(pool),
	}

	mux := asynq.NewServeMux()
	mux.HandleFunc(worker.TaskInventory, jobs.Inventory)
	mux.HandleFunc(worker.TaskInventoryBucket, jobs.InventoryBucket)
	mux.HandleFunc(queue.TaskTrashBucket, jobs.TrashBucket)
	mux.HandleFunc(worker.TaskTrash, jobs.Trash)
	mux.HandleFunc(queue.TaskVersionsBucket, jobs.VersionsBucket)
	mux.HandleFunc(worker.TaskVersions, jobs.CleanVersions)
	mux.HandleFunc(queue.TaskMultipartBucket, jobs.MultipartBucket)
	mux.HandleFunc(worker.TaskMultipart, jobs.CleanMultipart)
	mux.HandleFunc(queue.TaskBatchDelete, jobs.BatchDelete)
	mux.HandleFunc(queue.TaskBatchCopy, jobs.BatchCopy)
	mux.HandleFunc(queue.TaskBatchMove, jobs.BatchMove)
	mux.HandleFunc(worker.TaskRequestStats, jobs.RequestStats)

	go serveHealthz(cfg.HTTPAddr)
	slog.Info("worker listen", "concurrency", cfg.AsynqConcurrency)
	srv := asynq.NewServer(redisOpt, asynq.Config{Concurrency: cfg.AsynqConcurrency})
	if err := srv.Run(withTaskLog(mux)); err != nil {
		slog.Error("worker", "err", err)
		os.Exit(1)
	}
}

func withTaskLog(next asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		slog.Info("task start", "type", t.Type())
		if err := next.ProcessTask(ctx, t); err != nil {
			slog.Error("task fail", "type", t.Type(), "err", err)
			return err
		}
		return nil
	})
}

func serveHealthz(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	slog.Info("healthz listen", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("healthz", "err", err)
	}
}
