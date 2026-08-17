package bucket

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrConflict = errors.New("bucket exists")

type Bucket struct {
	ID                    int64
	BucketName            string
	DisplayName           *string
	DisplayAliasOnly      bool
	QuotaBytes            *int64
	ObjectLimit           *int64
	VersioningEnabled     bool
	AccessLoggingEnabled  bool
	AccessLogTargetBucket *string
	AccessLogPrefix       *string
	Status                string
	CreatedBy             *int64
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const bucketCols = `id, bucket_name, display_name, display_alias_only, quota_bytes, object_limit,
	versioning_enabled, access_logging_enabled, access_log_target_bucket, access_log_prefix,
	status, created_by, created_at, updated_at`

func (s *Store) List(ctx context.Context, userID int64) ([]Bucket, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+bucketCols+`
		FROM buckets b
		JOIN account_bucket_grants g ON g.bucket_id = b.id
		WHERE g.user_id = $1 AND b.status = 'active'
		ORDER BY b.created_at DESC`, userID)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		b, err := scanBucket(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) GetVisible(ctx context.Context, userID int64, name string) (*Bucket, error) {
	b, err := scanBucket(s.pool.QueryRow(ctx, `
		SELECT `+bucketCols+`
		FROM buckets b
		JOIN account_bucket_grants g ON g.bucket_id = b.id
		WHERE g.user_id = $1 AND b.bucket_name = $2 AND b.status = 'active'`, userID, name))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket: %w", err)
	}
	return &b, nil
}

func (s *Store) VisibleID(ctx context.Context, accountID int64, name string) (*int64, error) {
	b, err := s.GetVisible(ctx, accountID, name)
	if err != nil || b == nil {
		return nil, err
	}
	return &b.ID, nil
}

func (s *Store) GrantLocal(ctx context.Context, userID, bucketID int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO account_bucket_grants (user_id, bucket_id, local) VALUES ($1,$2,true)
		ON CONFLICT (user_id, bucket_id) DO NOTHING`, userID, bucketID)
	if err != nil {
		return fmt.Errorf("grant bucket: %w", err)
	}
	return nil
}

func (s *Store) Ensure(ctx context.Context, name string, display *string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO buckets (bucket_name, display_name, status, created_at, updated_at)
		VALUES ($1,$2,'active',now(),now())
		ON CONFLICT (bucket_name) DO UPDATE SET
			display_name = COALESCE(EXCLUDED.display_name, buckets.display_name),
			updated_at = now()
		RETURNING id`, name, display).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ensure bucket: %w", err)
	}
	return id, nil
}

func (s *Store) ReplaceOpsGrants(ctx context.Context, userID int64, bucketIDs []int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM account_bucket_grants WHERE user_id = $1 AND NOT local`, userID); err != nil {
		return fmt.Errorf("clear ops grants: %w", err)
	}
	for _, id := range bucketIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO account_bucket_grants (user_id, bucket_id, local) VALUES ($1,$2,false)
			ON CONFLICT (user_id, bucket_id) DO NOTHING`, userID, id); err != nil {
			return fmt.Errorf("insert ops grant: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) Insert(ctx context.Context, b *Bucket) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO buckets (
			bucket_name, display_name, display_alias_only, quota_bytes, object_limit,
			versioning_enabled, access_logging_enabled, access_log_target_bucket, access_log_prefix,
			status, created_by, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12)
		RETURNING `+bucketCols, b.BucketName, b.DisplayName, b.DisplayAliasOnly, b.QuotaBytes, b.ObjectLimit,
		b.VersioningEnabled, b.AccessLoggingEnabled, b.AccessLogTargetBucket, b.AccessLogPrefix,
		b.Status, b.CreatedBy, b.CreatedAt,
	).Scan(
		&b.ID, &b.BucketName, &b.DisplayName, &b.DisplayAliasOnly, &b.QuotaBytes, &b.ObjectLimit,
		&b.VersioningEnabled, &b.AccessLoggingEnabled, &b.AccessLogTargetBucket, &b.AccessLogPrefix,
		&b.Status, &b.CreatedBy, &b.CreatedAt, &b.UpdatedAt,
	)
	if isUnique(err) {
		return ErrConflict
	}
	if err != nil {
		return fmt.Errorf("insert bucket: %w", err)
	}
	return nil
}

func (s *Store) Update(ctx context.Context, b *Bucket) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE buckets SET
			display_name = $2, display_alias_only = $3, quota_bytes = $4, object_limit = $5,
			versioning_enabled = $6, access_logging_enabled = $7, access_log_target_bucket = $8,
			access_log_prefix = $9, updated_at = $10
		WHERE id = $1`,
		b.ID, b.DisplayName, b.DisplayAliasOnly, b.QuotaBytes, b.ObjectLimit,
		b.VersioningEnabled, b.AccessLoggingEnabled, b.AccessLogTargetBucket, b.AccessLogPrefix, b.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("update bucket: %w", err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM buckets WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}
	return nil
}

type Usage struct {
	UsedBytes, ObjectCount     int64
	TrashBytes, TrashCount     int64
	VersionBytes, VersionCount int64
}

func (s *Store) UsageByID(ctx context.Context, ids []int64) (map[int64]Usage, error) {
	out := map[int64]Usage{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT bucket_id,
			COALESCE(SUM(size) FILTER (WHERE object_key NOT LIKE '.trash/%' AND object_key <> '.trash/' AND object_key NOT LIKE '.versions/%' AND object_key <> '.versions/'), 0),
			COUNT(*) FILTER (WHERE object_key NOT LIKE '.trash/%' AND object_key <> '.trash/' AND object_key NOT LIKE '.versions/%' AND object_key <> '.versions/'),
			COALESCE(SUM(size) FILTER (WHERE object_key LIKE '.trash/%' OR object_key = '.trash/'), 0),
			COUNT(*) FILTER (WHERE object_key LIKE '.trash/%' OR object_key = '.trash/'),
			COALESCE(SUM(size) FILTER (WHERE object_key LIKE '.versions/%' OR object_key = '.versions/'), 0),
			COUNT(*) FILTER (WHERE object_key LIKE '.versions/%' OR object_key = '.versions/')
		FROM object_records
		WHERE bucket_id = ANY($1)
		GROUP BY bucket_id`, ids)
	if err != nil {
		return nil, fmt.Errorf("bucket usage: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var u Usage
		if err := rows.Scan(&id, &u.UsedBytes, &u.ObjectCount, &u.TrashBytes, &u.TrashCount, &u.VersionBytes, &u.VersionCount); err != nil {
			return nil, err
		}
		out[id] = u
	}
	return out, rows.Err()
}

type PrefixUsage struct {
	Prefix      string
	UsedBytes   int64
	ObjectCount int64
}

func (s *Store) PrefixUsage(ctx context.Context, bucketID int64) ([]PrefixUsage, error) {
	// ponytail: SQL 一级前缀与 firstLevelPrefix 一致（docs/a → docs/，根文件 → ""）。桶大了再物化。
	rows, err := s.pool.Query(ctx, `
		SELECT CASE
				WHEN position('/' IN object_key) = 0 THEN ''
				ELSE left(object_key, position('/' IN object_key))
			END AS prefix,
			COALESCE(SUM(size), 0),
			COUNT(*)
		FROM object_records
		WHERE bucket_id = $1
			AND object_key NOT LIKE '.trash/%' AND object_key <> '.trash/'
			AND object_key NOT LIKE '.versions/%' AND object_key <> '.versions/'
		GROUP BY 1
		ORDER BY 2 DESC, 1`, bucketID)
	if err != nil {
		return nil, fmt.Errorf("prefix usage: %w", err)
	}
	defer rows.Close()
	var out []PrefixUsage
	for rows.Next() {
		var p PrefixUsage
		if err := rows.Scan(&p.Prefix, &p.UsedBytes, &p.ObjectCount); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) ListActive(ctx context.Context) ([]Bucket, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+bucketCols+` FROM buckets WHERE status = 'active' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list active buckets: %w", err)
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		b, err := scanBucket(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) MarkInventoried(ctx context.Context, id int64, at time.Time) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx, `UPDATE buckets SET inventoried_at = $2 WHERE id = $1`, id, at)
	if err != nil {
		return fmt.Errorf("mark inventoried: %w", err)
	}
	return nil
}

func (s *Store) GetByName(ctx context.Context, name string) (*Bucket, error) {
	b, err := scanBucket(s.pool.QueryRow(ctx, `SELECT `+bucketCols+` FROM buckets WHERE bucket_name = $1`, name))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket by name: %w", err)
	}
	return &b, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanBucket(row rowScanner) (Bucket, error) {
	var b Bucket
	err := row.Scan(
		&b.ID, &b.BucketName, &b.DisplayName, &b.DisplayAliasOnly, &b.QuotaBytes, &b.ObjectLimit,
		&b.VersioningEnabled, &b.AccessLoggingEnabled, &b.AccessLogTargetBucket, &b.AccessLogPrefix,
		&b.Status, &b.CreatedBy, &b.CreatedAt, &b.UpdatedAt,
	)
	return b, err
}

func isUnique(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}
