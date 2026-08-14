package objects

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Folded(ctx context.Context, bucketID int64, prefix, after string, maxKeys int) ([]ObjectItem, []string, *string, bool, error) {
	if maxKeys < 1 {
		maxKeys = 1
	}
	if maxKeys > maxListKeys {
		maxKeys = maxListKeys
	}
	recs, err := s.ListPrefix(ctx, bucketID, prefix, after, scanCap)
	if err != nil {
		return nil, nil, nil, false, err
	}
	items, prefixes, token, truncated := foldList(prefix, maxKeys, recs, len(recs) == scanCap)
	out := make([]ObjectItem, 0, len(items))
	for _, rec := range items {
		out = append(out, ObjectItem{Key: rec.Key, Size: rec.Size, ContentType: rec.ContentType, LastModified: rec.LastModified, ETag: rec.ETag})
	}
	return out, prefixes, token, truncated, nil
}

type ObjectItem struct {
	Key          string
	Size         int64
	ContentType  *string
	LastModified *string
	ETag         *string
}

func (s *Store) ListPrefix(ctx context.Context, bucketID int64, prefix, after string, limit int) ([]record, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT object_key, COALESCE(size, 0), content_type, etag, created_by,
			COALESCE(last_seen_at, updated_at)
		FROM object_records
		WHERE bucket_id = $1
		  AND object_key LIKE $2 ESCAPE '\'
		  AND object_key > $3
		  AND object_key NOT LIKE '.trash/%'
		  AND object_key <> '.trash/'
		  AND object_key NOT LIKE '.versions/%'
		  AND object_key <> '.versions/'
		ORDER BY object_key
		LIMIT $4`, bucketID, likePrefix(prefix), after, limit)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}
	defer rows.Close()
	var out []record
	for rows.Next() {
		var rec record
		var last time.Time
		if err := rows.Scan(&rec.Key, &rec.Size, &rec.ContentType, &rec.ETag, &rec.UploadedBy, &last); err != nil {
			return nil, err
		}
		s := last.UTC().Format(time.RFC3339)
		rec.LastModified = &s
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *Store) Exists(ctx context.Context, bucketID int64, key string) (bool, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT 1 FROM object_records WHERE bucket_id = $1 AND object_key = $2`, bucketID, key).Scan(&n)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("object exists: %w", err)
	}
	return true, nil
}

func (s *Store) Get(ctx context.Context, bucketID int64, key string) (*record, error) {
	var rec record
	var last, created, updated time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT r.object_key, COALESCE(r.size, 0), r.content_type, r.etag, r.storage_class, r.created_by,
			COALESCE(r.last_seen_at, r.updated_at), r.created_at, r.updated_at, u.username
		FROM object_records r
		LEFT JOIN tenant_users u ON u.id = r.created_by
		WHERE r.bucket_id = $1 AND r.object_key = $2`, bucketID, key,
	).Scan(
		&rec.Key, &rec.Size, &rec.ContentType, &rec.ETag, &rec.StorageClass, &rec.UploadedBy,
		&last, &created, &updated, &rec.Username,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	ls, cs, us := last.UTC().Format(time.RFC3339), created.UTC().Format(time.RFC3339), updated.UTC().Format(time.RFC3339)
	rec.LastModified, rec.CreatedAt, rec.UpdatedAt = &ls, &cs, &us
	return &rec, nil
}

func (s *Store) Upsert(ctx context.Context, bucketID int64, bucketName, key string, size int64, etag, contentType *string, userID int64, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO object_records (
			bucket_id, bucket_name, object_key, size, etag, content_type,
			created_by, updated_by, last_seen_at, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$7,$8,$8,$8)
		ON CONFLICT (bucket_id, object_key) DO UPDATE SET
			size = EXCLUDED.size,
			etag = EXCLUDED.etag,
			content_type = EXCLUDED.content_type,
			updated_by = EXCLUDED.updated_by,
			last_seen_at = EXCLUDED.last_seen_at,
			updated_at = EXCLUDED.updated_at`,
		bucketID, bucketName, key, size, etag, contentType, userID, at)
	if err != nil {
		return fmt.Errorf("upsert object: %w", err)
	}
	return nil
}

func (s *Store) ListTrash(ctx context.Context, bucketID int64, prefix, after string, limit int) ([]record, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT object_key, COALESCE(size, 0), content_type, COALESCE(last_seen_at, updated_at)
		FROM object_records
		WHERE bucket_id = $1
		  AND object_key LIKE $2 ESCAPE '\'
		  AND object_key > $3
		ORDER BY object_key
		LIMIT $4`, bucketID, likePrefix(TrashPrefix+prefix), after, limit)
	if err != nil {
		return nil, fmt.Errorf("list trash: %w", err)
	}
	defer rows.Close()
	var out []record
	for rows.Next() {
		var rec record
		var last time.Time
		if err := rows.Scan(&rec.Key, &rec.Size, &rec.ContentType, &last); err != nil {
			return nil, err
		}
		ts := last.UTC().Format(time.RFC3339)
		rec.LastModified = &ts
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *Store) ListLiveKeys(ctx context.Context, bucketID int64, prefix string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT object_key
		FROM object_records
		WHERE bucket_id = $1
		  AND object_key LIKE $2 ESCAPE '\'
		  AND object_key NOT LIKE '.trash/%'
		  AND object_key <> '.trash/'
		  AND object_key NOT LIKE '.versions/%'
		  AND object_key <> '.versions/'
		ORDER BY object_key`, bucketID, likePrefix(prefix))
	if err != nil {
		return nil, fmt.Errorf("list live keys: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}

func (s *Store) Delete(ctx context.Context, bucketID int64, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM object_records WHERE bucket_id = $1 AND object_key = $2`, bucketID, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (s *Store) MoveKey(ctx context.Context, bucketID int64, bucketName, from, to string, size int64, etag, contentType *string, userID int64, at time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("move object begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM object_records WHERE bucket_id = $1 AND object_key = $2`, bucketID, to); err != nil {
		return fmt.Errorf("move object drop dest: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE object_records SET
			object_key = $3, size = $4, etag = $5, content_type = $6,
			updated_by = $7, last_seen_at = $8, updated_at = $8
		WHERE bucket_id = $1 AND object_key = $2`,
		bucketID, from, to, size, etag, contentType, userID, at)
	if err != nil {
		return fmt.Errorf("move object: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO object_records (
				bucket_id, bucket_name, object_key, size, etag, content_type,
				created_by, updated_by, last_seen_at, created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$7,$8,$8,$8)`,
			bucketID, bucketName, to, size, etag, contentType, userID, at); err != nil {
			return fmt.Errorf("move object insert: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("move object commit: %w", err)
	}
	return nil
}

func (s *Store) UpsertSeen(ctx context.Context, bucketID int64, bucketName, key string, size int64, etag, storageClass *string, seenAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO object_records (
			bucket_id, bucket_name, object_key, size, etag, storage_class, last_seen_at, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$7,$7)
		ON CONFLICT (bucket_id, object_key) DO UPDATE SET
			size = EXCLUDED.size,
			etag = EXCLUDED.etag,
			storage_class = EXCLUDED.storage_class,
			last_seen_at = EXCLUDED.last_seen_at,
			updated_at = EXCLUDED.updated_at`,
		bucketID, bucketName, key, size, etag, storageClass, seenAt)
	if err != nil {
		return fmt.Errorf("upsert seen: %w", err)
	}
	return nil
}

func (s *Store) PurgeUnseen(ctx context.Context, bucketID int64, seenAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM object_records
		WHERE bucket_id = $1 AND (last_seen_at IS NULL OR last_seen_at < $2)`, bucketID, seenAt)
	if err != nil {
		return fmt.Errorf("purge unseen: %w", err)
	}
	return nil
}

type TrashObject struct {
	BucketID   int64
	BucketName string
	Key        string
}

func (s *Store) ExpiredTrash(ctx context.Context, days int) ([]TrashObject, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT bucket_id, bucket_name, object_key
		FROM object_records
		WHERE object_key LIKE '.trash/%' AND object_key <> '.trash/'
		  AND COALESCE(last_seen_at, updated_at) < now() - ($1 * interval '1 day')`, days)
	if err != nil {
		return nil, fmt.Errorf("expired trash: %w", err)
	}
	defer rows.Close()
	var out []TrashObject
	for rows.Next() {
		var item TrashObject
		if err := rows.Scan(&item.BucketID, &item.BucketName, &item.Key); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
