package versions

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Record struct {
	ID                int64
	BucketName        string
	ObjectKey         string
	VersionNo         int
	StorageKey        string
	Size              int64
	ETag              *string
	CreatedBy         int64
	CreatedByUsername *string
	CreatedAt         time.Time
	Source            string
	Remark            *string
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) List(ctx context.Context, bucket, key string, limit, offset int) ([]Record, int, error) {
	var total int
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM file_versions WHERE bucket_name = $1 AND object_key = $2`, bucket, key).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count versions: %w", err)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT v.id, v.bucket_name, v.object_key, v.version_no, v.storage_key, v.size, v.etag,
			v.created_by, u.username, v.created_at, v.source, v.remark
		FROM file_versions v
		LEFT JOIN tenant_users u ON u.id = v.created_by
		WHERE v.bucket_name = $1 AND v.object_key = $2
		ORDER BY v.version_no DESC
		LIMIT $3 OFFSET $4`, bucket, key, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list versions: %w", err)
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	return out, total, rows.Err()
}

func (s *Store) GetByID(ctx context.Context, id int64) (*Record, error) {
	r, err := scan(s.pool.QueryRow(ctx, `
		SELECT v.id, v.bucket_name, v.object_key, v.version_no, v.storage_key, v.size, v.etag,
			v.created_by, u.username, v.created_at, v.source, v.remark
		FROM file_versions v
		LEFT JOIN tenant_users u ON u.id = v.created_by
		WHERE v.id = $1`, id))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get version: %w", err)
	}
	return &r, nil
}

func (s *Store) MaxNo(ctx context.Context, bucket, key string) (int, error) {
	if s == nil || s.pool == nil {
		return 1, nil
	}
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(version_no), 1) FROM file_versions
		WHERE bucket_name = $1 AND object_key = $2`, bucket, key).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("max version: %w", err)
	}
	return n, nil
}

func (s *Store) NextNo(ctx context.Context, bucket, key string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(version_no), 0) + 1 FROM file_versions
		WHERE bucket_name = $1 AND object_key = $2`, bucket, key).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("next version: %w", err)
	}
	return n, nil
}

func (s *Store) Insert(ctx context.Context, r *Record) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO file_versions (
			bucket_name, object_key, version_no, storage_key, size, etag, created_by, created_at, source, remark
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		RETURNING id`, r.BucketName, r.ObjectKey, r.VersionNo, r.StorageKey, r.Size, r.ETag, r.CreatedBy, r.CreatedAt, r.Source, r.Remark,
	).Scan(&r.ID)
	if err != nil {
		return fmt.Errorf("insert version: %w", err)
	}
	return nil
}

func (s *Store) Expired(ctx context.Context, days int) ([]Record, error) {
	return s.expired(ctx, days, "")
}

func (s *Store) ExpiredInBucket(ctx context.Context, days int, bucketName string) ([]Record, error) {
	return s.expired(ctx, days, bucketName)
}

func (s *Store) expired(ctx context.Context, days int, bucketName string) ([]Record, error) {
	q := `
		SELECT v.id, v.bucket_name, v.object_key, v.version_no, v.storage_key, v.size, v.etag,
			v.created_by, u.username, v.created_at, v.source, v.remark
		FROM file_versions v
		LEFT JOIN tenant_users u ON u.id = v.created_by
		WHERE v.created_at < now() - ($1 * interval '1 day')`
	args := []any{days}
	if bucketName != "" {
		q += ` AND v.bucket_name = $2`
		args = append(args, bucketName)
	}
	q += ` ORDER BY v.id LIMIT 5000`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("expired versions: %w", err)
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(
			&r.ID, &r.BucketName, &r.ObjectKey, &r.VersionNo, &r.StorageKey, &r.Size, &r.ETag,
			&r.CreatedBy, &r.CreatedByUsername, &r.CreatedAt, &r.Source, &r.Remark,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Delete(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM file_versions WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete version: %w", err)
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scan(row rowScanner) (Record, error) {
	var r Record
	err := row.Scan(
		&r.ID, &r.BucketName, &r.ObjectKey, &r.VersionNo, &r.StorageKey, &r.Size, &r.ETag,
		&r.CreatedBy, &r.CreatedByUsername, &r.CreatedAt, &r.Source, &r.Remark,
	)
	return r, err
}
