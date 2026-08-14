package stats

import (
	"context"
	"fmt"
	"time"
)

type AggregateResult struct {
	Periods, Accounts, Buckets, Users, Prefixes, Daily int
	CollectedAt                                        time.Time
}

type auditRow struct {
	userID, accountID *int64
	bucket, key       *string
	action, status    string
	at                time.Time
	size              int64
}

func (s *Store) AggregateRequests(ctx context.Context, now time.Time) (AggregateResult, error) {
	var out AggregateResult
	if s == nil || s.pool == nil {
		return out, nil
	}
	now = now.UTC()
	since := now.Add(-time.Duration(periodHours["30d"]) * time.Hour)

	logs, err := s.loadLogs(ctx, since)
	if err != nil {
		return out, err
	}
	buckets, err := s.loadBucketNames(ctx)
	if err != nil {
		return out, err
	}
	names, err := s.loadUsernames(ctx)
	if err != nil {
		return out, err
	}

	platform := map[string]*counter{}
	for p := range periodHours {
		platform[p] = &counter{}
	}
	daily := map[time.Time]*counter{}
	type accKey struct {
		id     int64
		period string
	}
	type bktKey struct {
		id     int64
		period string
	}
	type userKey struct {
		userID, accountID int64
		period            string
	}
	type prefixKey struct {
		bucketID int64
		prefix   string
		period   string
	}
	accounts := map[accKey]*counter{}
	bucketStats := map[bktKey]*counter{}
	users := map[userKey]*userCounter{}
	prefixes := map[prefixKey]int64{}
	bucketAccount := map[int64]*int64{}
	nameToID := map[string]int64{}
	for id, name := range buckets {
		nameToID[name] = id
	}

	dayCut := now.AddDate(0, 0, -dailyRollupDays)
	dayCut = time.Date(dayCut.Year(), dayCut.Month(), dayCut.Day(), 0, 0, 0, 0, time.UTC)

	for _, log := range logs {
		if !isObjectRequest(log.action) {
			continue
		}
		var bid *int64
		if log.bucket != nil {
			if id, ok := nameToID[*log.bucket]; ok {
				bid = &id
			}
		}
		var actor *int64
		if log.userID != nil {
			actor = log.userID
		}
		for period, hours := range periodHours {
			if !inWindow(log.at, now, hours) {
				continue
			}
			platform[period].add(log.action, log.status, log.size, actor)
			if log.accountID != nil {
				k := accKey{*log.accountID, period}
				if accounts[k] == nil {
					accounts[k] = &counter{}
				}
				accounts[k].add(log.action, log.status, log.size, actor)
			}
			if bid != nil {
				k := bktKey{*bid, period}
				if bucketStats[k] == nil {
					bucketStats[k] = &counter{}
				}
				bucketStats[k].add(log.action, log.status, log.size, actor)
			}
			if log.accountID != nil && actor != nil {
				uk := userKey{*actor, *log.accountID, period}
				if users[uk] == nil {
					uc := &userCounter{}
					if n, ok := names[*actor]; ok {
						uc.username = &n
					}
					users[uk] = uc
				}
				key := ""
				if log.key != nil {
					key = *log.key
				}
				users[uk].add(log.action, log.size, key)
			}
			if bid != nil && log.key != nil {
				if _, ok := putActions[log.action]; ok {
					prefixes[prefixKey{*bid, firstLevelPrefix(*log.key), period}]++
					setBucketAccount(bucketAccount, *bid, log.accountID)
				} else if _, ok := getActions[log.action]; ok {
					prefixes[prefixKey{*bid, firstLevelPrefix(*log.key), period}]++
					setBucketAccount(bucketAccount, *bid, log.accountID)
				}
			}
		}
		day := time.Date(log.at.Year(), log.at.Month(), log.at.Day(), 0, 0, 0, 0, time.UTC)
		if !day.Before(dayCut) {
			if daily[day] == nil {
				daily[day] = &counter{}
			}
			daily[day].add(log.action, log.status, log.size, actor)
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	for _, q := range []string{
		`DELETE FROM prefix_request_stats`,
		`DELETE FROM user_request_stats`,
		`DELETE FROM bucket_request_stats`,
		`DELETE FROM account_request_stats`,
		`DELETE FROM daily_platform_request_stats`,
		`DELETE FROM platform_request_stats`,
	} {
		if _, err := tx.Exec(ctx, q); err != nil {
			return out, fmt.Errorf("clear request stats: %w", err)
		}
	}
	for period, c := range platform {
		if _, err := tx.Exec(ctx, `
			INSERT INTO platform_request_stats (
				period, upload_bytes, download_bytes, request_count, get_count, put_count, delete_count, error_count, active_users, collected_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			period, c.uploadBytes, c.downloadBytes, c.requestCount, c.getCount, c.putCount, c.deleteCount, c.errorCount, int64(len(c.active)), now); err != nil {
			return out, fmt.Errorf("platform stats: %w", err)
		}
	}
	for day, c := range daily {
		if _, err := tx.Exec(ctx, `
			INSERT INTO daily_platform_request_stats (
				stat_date, upload_bytes, download_bytes, request_count, get_count, put_count, delete_count, error_count, collected_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			day, c.uploadBytes, c.downloadBytes, c.requestCount, c.getCount, c.putCount, c.deleteCount, c.errorCount, now); err != nil {
			return out, fmt.Errorf("daily stats: %w", err)
		}
	}
	seenAcc := map[int64]struct{}{}
	for k, c := range accounts {
		seenAcc[k.id] = struct{}{}
		if _, err := tx.Exec(ctx, `
			INSERT INTO account_request_stats (
				account_id, period, upload_bytes, download_bytes, request_count, get_count, put_count, delete_count, error_count, active_users, collected_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			k.id, k.period, c.uploadBytes, c.downloadBytes, c.requestCount, c.getCount, c.putCount, c.deleteCount, c.errorCount, int64(len(c.active)), now); err != nil {
			return out, fmt.Errorf("account stats: %w", err)
		}
	}
	seenBkt := map[int64]struct{}{}
	for k, c := range bucketStats {
		name, ok := buckets[k.id]
		if !ok {
			continue
		}
		seenBkt[k.id] = struct{}{}
		if _, err := tx.Exec(ctx, `
			INSERT INTO bucket_request_stats (
				bucket_id, period, account_id, bucket_name, request_count, upload_bytes, download_bytes, get_count, put_count, delete_count, collected_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			k.id, k.period, bucketAccount[k.id], name, c.requestCount, c.uploadBytes, c.downloadBytes, c.getCount, c.putCount, c.deleteCount, now); err != nil {
			return out, fmt.Errorf("bucket stats: %w", err)
		}
	}
	for k, c := range users {
		if _, err := tx.Exec(ctx, `
			INSERT INTO user_request_stats (
				user_id, account_id, period, username, upload_count, download_count, delete_count, access_count, upload_bytes, download_bytes, collected_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			k.userID, k.accountID, k.period, c.username, c.uploadCount, c.downloadCount, c.deleteCount, c.accessCount, c.uploadBytes, c.downloadBytes, now); err != nil {
			return out, fmt.Errorf("user stats: %w", err)
		}
	}
	for k, n := range prefixes {
		if _, ok := buckets[k.bucketID]; !ok {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO prefix_request_stats (bucket_id, prefix, period, account_id, access_count, collected_at)
			VALUES ($1,$2,$3,$4,$5,$6)`,
			k.bucketID, k.prefix, k.period, bucketAccount[k.bucketID], n, now); err != nil {
			return out, fmt.Errorf("prefix stats: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return out, err
	}
	return AggregateResult{
		Periods: len(periodHours), Accounts: len(seenAcc), Buckets: len(seenBkt),
		Users: len(users), Prefixes: len(prefixes), Daily: len(daily), CollectedAt: now,
	}, nil
}

func setBucketAccount(m map[int64]*int64, bucketID int64, accountID *int64) {
	if accountID == nil {
		return
	}
	cur, ok := m[bucketID]
	if !ok {
		id := *accountID
		m[bucketID] = &id
		return
	}
	if cur != nil && *cur != *accountID {
		m[bucketID] = nil
	}
}

func (s *Store) loadLogs(ctx context.Context, since time.Time) ([]auditRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT a.tenant_user_id, a.account_id, a.bucket_name, a.object_key, a.action, a.status, a.created_at,
			COALESCE(r.size, 0)
		FROM audit_logs a
		LEFT JOIN object_records r ON r.bucket_name = a.bucket_name AND r.object_key = a.object_key
		WHERE a.created_at >= $1`, since)
	if err != nil {
		return nil, fmt.Errorf("load audit logs: %w", err)
	}
	defer rows.Close()
	var out []auditRow
	for rows.Next() {
		var r auditRow
		if err := rows.Scan(&r.userID, &r.accountID, &r.bucket, &r.key, &r.action, &r.status, &r.at, &r.size); err != nil {
			return nil, err
		}
		r.at = r.at.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) loadBucketNames(ctx context.Context) (map[int64]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, bucket_name FROM buckets`)
	if err != nil {
		return nil, fmt.Errorf("load buckets: %w", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		out[id] = name
	}
	return out, rows.Err()
}

func (s *Store) loadUsernames(ctx context.Context) (map[int64]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, username FROM tenant_users`)
	if err != nil {
		return nil, fmt.Errorf("load users: %w", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		out[id] = name
	}
	return out, rows.Err()
}
