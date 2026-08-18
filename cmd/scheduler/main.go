package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyxc1124/osspilot-tenant-worker/internal/bucket"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/config"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/logx"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/queue"
	"github.com/cyxc1124/osspilot-tenant-worker/internal/sched"
)

func main() {
	logx.Setup("osspilot-tenant-scheduler")
	cfg := config.Load()
	if cfg.RedisURL == "" || cfg.DatabaseURL == "" {
		slog.Error("DATABASE_URL and REDIS_URL are required for the scheduler")
		os.Exit(1)
	}
	redisOpt, err := asynq.ParseRedisURI(cfg.RedisURL)
	if err != nil {
		slog.Error("REDIS_URL", "err", err)
		os.Exit(1)
	}
	group, err := sched.New(cfg.RedisURL, "tenant")
	if err != nil {
		slog.Error("scheduler group", "err", err)
		os.Exit(1)
	}
	defer group.Close()
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	client := asynq.NewClient(redisOpt)
	defer client.Close()
	buckets := bucket.NewStore(pool)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go serveHealthz(cfg.HTTPAddr)
	slog.Info("scheduler listen", "me", group.Me())

	tick := time.NewTicker(sched.BeatEvery)
	defer tick.Stop()
	lastMembers := ""
	run := func() {
		if err := group.Beat(ctx); err != nil {
			slog.Warn("heartbeat", "err", err)
			return
		}
		members, err := group.Members(ctx)
		if err != nil {
			slog.Warn("members", "err", err)
			return
		}
		fp := sched.MembersFingerprint(members)
		if fp != lastMembers {
			slog.Info("scheduler members", "members", members)
			lastMembers = fp
		}
		now := time.Now().UTC()
		items, err := buckets.ListActive(ctx)
		if err != nil {
			slog.Warn("list buckets", "err", err)
		} else {
			enqueueBuckets(ctx, group, client, items, members, now, 15*time.Minute, "inventory", queue.TaskInventoryBucket, time.Hour)
			enqueueBuckets(ctx, group, client, items, members, now, time.Hour, "trash", queue.TaskTrashBucket, time.Hour)
			enqueueBuckets(ctx, group, client, items, members, now, 6*time.Hour, "versions", queue.TaskVersionsBucket, time.Hour)
			enqueueBuckets(ctx, group, client, items, members, now, 6*time.Hour, "multipart", queue.TaskMultipartBucket, time.Hour)
		}
		enqueueStats(ctx, group, client, now)
	}
	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			run()
		}
	}
}

func enqueueBuckets(ctx context.Context, g *sched.Group, client *asynq.Client, items []bucket.Bucket, members []string, now time.Time, interval time.Duration, typ, task string, timeout time.Duration) {
	slot, ttl := sched.Slot(now, interval)
	for _, b := range items {
		if !g.Mine(b.ID, members) {
			continue
		}
		id := sched.IDString(b.ID)
		ok, err := g.Claim(ctx, slot, typ, id, ttl)
		if err != nil {
			slog.Warn("claim", "type", typ, "bucket", b.BucketName, "err", err)
			continue
		}
		if !ok {
			continue
		}
		payload, err := json.Marshal(queue.BucketJob{BucketID: b.ID, BucketName: b.BucketName})
		if err != nil {
			g.DropClaim(ctx, slot, typ, id)
			continue
		}
		if _, err := client.EnqueueContext(ctx, asynq.NewTask(task, payload),
			asynq.TaskID(sched.ClaimKey(slot, typ, id)), asynq.MaxRetry(3), asynq.Timeout(timeout)); err != nil {
			if !errors.Is(err, asynq.ErrTaskIDConflict) {
				g.DropClaim(ctx, slot, typ, id)
				slog.Warn("enqueue", "type", typ, "bucket", b.BucketName, "err", err)
			}
		}
	}
}

func enqueueStats(ctx context.Context, g *sched.Group, client *asynq.Client, now time.Time) {
	slot, ttl := sched.Slot(now, time.Hour)
	ok, err := g.Claim(ctx, slot, "stats", "all", ttl)
	if err != nil {
		slog.Warn("claim stats", "err", err)
		return
	}
	if !ok {
		return
	}
	if _, err := client.EnqueueContext(ctx, asynq.NewTask(queue.TaskRequestStats, nil),
		asynq.TaskID(sched.ClaimKey(slot, "stats", "all")), asynq.MaxRetry(3), asynq.Timeout(10*time.Minute)); err != nil {
		if !errors.Is(err, asynq.ErrTaskIDConflict) {
			g.DropClaim(ctx, slot, "stats", "all")
			slog.Warn("enqueue stats", "err", err)
		}
	}
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
