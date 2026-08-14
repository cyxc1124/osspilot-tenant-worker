package platform

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Map(ctx context.Context) (map[string]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT key, value FROM platform_settings`)
	if err != nil {
		return nil, fmt.Errorf("list settings: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func pick(rows map[string]string, key, fallback string) string {
	if v := strings.TrimSpace(rows[key]); v != "" {
		return v
	}
	return fallback
}

func pickPtr(rows map[string]string, key, fallback string) *string {
	v := pick(rows, key, fallback)
	if v == "" {
		return nil
	}
	return &v
}

func pickInt(rows map[string]string, key string, fallback int) int {
	v := strings.TrimSpace(rows[key])
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func pickBool(rows map[string]string, key string, fallback bool) bool {
	v := strings.ToLower(strings.TrimSpace(rows[key]))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return fallback
}

func (s *Store) Upsert(ctx context.Context, key, value string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO platform_settings (key, value, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, key, value)
	if err != nil {
		return fmt.Errorf("upsert setting %s: %w", key, err)
	}
	return nil
}

func (s *Store) TrashPolicy(ctx context.Context) (days int, enabled bool, err error) {
	return s.CleanupPolicy(ctx, "trash_cleanup_enabled", "trash_retention_days", 0)
}

func (s *Store) VersionPolicy(ctx context.Context) (days int, enabled bool, err error) {
	return s.CleanupPolicy(ctx, "version_cleanup_enabled", "version_retention_days", 0)
}

func (s *Store) MultipartPolicy(ctx context.Context) (staleDays int, enabled bool, err error) {
	return s.CleanupPolicy(ctx, "multipart_cleanup_enabled", "multipart_stale_days", 7)
}

func (s *Store) CleanupPolicy(ctx context.Context, enabledKey, daysKey string, defaultDays int) (days int, enabled bool, err error) {
	rows, err := s.Map(ctx)
	if err != nil {
		return 0, false, err
	}
	return pickInt(rows, daysKey, defaultDays), pickBool(rows, enabledKey, false), nil
}
