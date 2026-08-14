package audit

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Logger struct {
	pool *pgxpool.Pool
}

func NewLogger(pool *pgxpool.Pool) *Logger {
	if pool == nil {
		return nil
	}
	return &Logger{pool: pool}
}

func (l *Logger) RecordMeta(ctx context.Context, userID, accountID int64, bucket, key, action, status, errMsg, ip, ua string) {
	l.write(ctx, &userID, &accountID, bucket, key, action, status, errMsg, ip, ua)
}

func (l *Logger) write(ctx context.Context, uid, account *int64, bucket, key, action, status, errMsg, ip, ua string) {
	if l == nil || l.pool == nil || action == "" {
		return
	}
	if status == "" {
		status = "success"
	}
	_, _ = l.pool.Exec(ctx, `
		INSERT INTO audit_logs (
			tenant_user_id, account_id, bucket_name, object_key, action, source_ip, user_agent, status, error_message
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, uid, account, emptyToNil(bucket), emptyToNil(key), action, emptyToNil(ip), emptyToNil(ua), status, emptyToNil(errMsg))
}

func emptyToNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
