package sched

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	BeatEvery = 5 * time.Second
	BeatTTL   = 15 * time.Second
)

type Group struct {
	rdb  *redis.Client
	role string
	me   string
}

func New(redisURL, role string) (*Group, error) {
	opt, err := redis.ParseURL(strings.TrimSpace(redisURL))
	if err != nil {
		return nil, err
	}
	me := strings.TrimSpace(os.Getenv("HOSTNAME"))
	if me == "" {
		me, err = os.Hostname()
		if err != nil || me == "" {
			me = "scheduler"
		}
	}
	return &Group{rdb: redis.NewClient(opt), role: role, me: me}, nil
}

func (g *Group) Close() error {
	if g == nil || g.rdb == nil {
		return nil
	}
	return g.rdb.Close()
}

func (g *Group) Me() string { return g.me }

func (g *Group) Beat(ctx context.Context) error {
	return g.rdb.Set(ctx, g.beatKey(), "1", BeatTTL).Err()
}

func (g *Group) Members(ctx context.Context) ([]string, error) {
	pat := g.prefix() + "*"
	var keys []string
	var cursor uint64
	for {
		batch, next, err := g.rdb.Scan(ctx, cursor, pat, 50).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	out := make([]string, 0, len(keys)+1)
	pref := g.prefix()
	seen := map[string]struct{}{}
	for _, k := range keys {
		name := strings.TrimPrefix(k, pref)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	if _, ok := seen[g.me]; !ok && g.me != "" {
		out = append(out, g.me)
	}
	sort.Strings(out)
	return out, nil
}

func (g *Group) Mine(id int64, members []string) bool {
	return Mine(id, members, g.me)
}

func (g *Group) Claim(ctx context.Context, slot, typ, id string, ttl time.Duration) (bool, error) {
	if ttl < time.Second {
		ttl = time.Second
	}
	ok, err := g.rdb.SetNX(ctx, ClaimKey(slot, typ, id), "1", ttl).Result()
	return ok, err
}

func (g *Group) DropClaim(ctx context.Context, slot, typ, id string) {
	c, cancel := dropCtx(ctx)
	defer cancel()
	_ = g.rdb.Del(c, ClaimKey(slot, typ, id)).Err()
}

func dropCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
}

func (g *Group) prefix() string {
	return "osspilot:sched:" + g.role + ":"
}

func (g *Group) beatKey() string {
	return g.prefix() + g.me
}

func Mine(id int64, members []string, me string) bool {
	n := len(members)
	if n == 0 {
		return true
	}
	i := sort.SearchStrings(members, me)
	if i >= n || members[i] != me {
		return false
	}
	return int(uint64(id)%uint64(n)) == i
}

func ClaimKey(slot, typ, id string) string {
	return "osspilot:claim:" + slot + ":" + typ + ":" + id
}

func Slot(now time.Time, interval time.Duration) (slot string, ttl time.Duration) {
	now = now.UTC()
	start := now.Truncate(interval)
	slot = start.Format(time.RFC3339)
	ttl = start.Add(interval).Sub(now)
	if ttl < time.Second {
		ttl = time.Second
	}
	return slot, ttl
}

func IDString(id int64) string {
	return strconv.FormatInt(id, 10)
}

func MembersFingerprint(members []string) string {
	return strings.Join(members, ",")
}
