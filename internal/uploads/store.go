package uploads

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	TypeSimple    = "simple"
	TypeMultipart = "multipart"
	StatusPending = "pending"
	StatusDone    = "completed"
	StatusAbort   = "aborted"
)

type Task struct {
	ID          int64
	UserID      int64
	BucketName  string
	ObjectKey   string
	UploadType  string
	UploadID    *string
	Size        *int64
	ContentType *string
	Status      string
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Insert(ctx context.Context, t *Task, expiredAt time.Time) error {
	return s.pool.QueryRow(ctx, `
		INSERT INTO upload_tasks (
			user_id, bucket_name, object_key, upload_type, upload_id, size, content_type, status, expired_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING id`,
		t.UserID, t.BucketName, t.ObjectKey, t.UploadType, t.UploadID, t.Size, t.ContentType, StatusPending, expiredAt,
	).Scan(&t.ID)
}

func (s *Store) GetPending(ctx context.Context, id, userID int64, bucket, key, uploadType string) (*Task, error) {
	var t Task
	err := s.pool.QueryRow(ctx, `
		SELECT id, user_id, bucket_name, object_key, upload_type, upload_id, size, content_type, status
		FROM upload_tasks WHERE id = $1`, id,
	).Scan(&t.ID, &t.UserID, &t.BucketName, &t.ObjectKey, &t.UploadType, &t.UploadID, &t.Size, &t.ContentType, &t.Status)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get upload task: %w", err)
	}
	if t.UserID != userID || t.BucketName != bucket || t.ObjectKey != key || t.UploadType != uploadType || t.Status != StatusPending {
		return nil, nil
	}
	return &t, nil
}

func (s *Store) Finish(ctx context.Context, id int64, status string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE upload_tasks SET status = $2, completed_at = $3 WHERE id = $1`, id, status, at)
	return err
}

func (s *Store) CompletedBytesSince(ctx context.Context, accountID int64, since time.Time) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(t.size), 0)
		FROM upload_tasks t
		JOIN tenant_users u ON u.id = t.user_id
		WHERE COALESCE(u.account_id, u.id) = $1
		  AND t.status = $2
		  AND t.completed_at >= $3`, accountID, StatusDone, since).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("daily upload bytes: %w", err)
	}
	return n, nil
}

// StaleMultipart returns pending multipart tasks past age or expired_at.
func (s *Store) StaleMultipart(ctx context.Context, staleDays int) ([]Task, error) {
	return s.staleMultipart(ctx, staleDays, "")
}

func (s *Store) StaleMultipartInBucket(ctx context.Context, staleDays int, bucketName string) ([]Task, error) {
	return s.staleMultipart(ctx, staleDays, bucketName)
}

func (s *Store) staleMultipart(ctx context.Context, staleDays int, bucketName string) ([]Task, error) {
	q := `
		SELECT id, user_id, bucket_name, object_key, upload_type, upload_id, size, content_type, status
		FROM upload_tasks
		WHERE upload_type = $1 AND status = $2
		  AND (expired_at < now() OR created_at < now() - ($3 * interval '1 day'))`
	args := []any{TypeMultipart, StatusPending, staleDays}
	if bucketName != "" {
		q += ` AND bucket_name = $4`
		args = append(args, bucketName)
	}
	q += ` ORDER BY id LIMIT 5000`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("stale multipart: %w", err)
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.UserID, &t.BucketName, &t.ObjectKey, &t.UploadType, &t.UploadID, &t.Size, &t.ContentType, &t.Status); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
