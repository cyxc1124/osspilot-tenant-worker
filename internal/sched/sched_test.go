package sched

import (
	"context"
	"testing"
	"time"
)

func TestMine(t *testing.T) {
	members := []string{"a", "b"}
	if !Mine(0, members, "a") || Mine(0, members, "b") {
		t.Fatal("id 0 belongs to a")
	}
	if Mine(1, members, "a") || !Mine(1, members, "b") {
		t.Fatal("id 1 belongs to b")
	}
	if !Mine(3, nil, "a") {
		t.Fatal("empty members take all")
	}
	if Mine(1, members, "c") {
		t.Fatal("unknown member owns nothing")
	}
}

func TestSlot(t *testing.T) {
	now := time.Date(2026, 8, 18, 4, 7, 0, 0, time.UTC)
	slot, ttl := Slot(now, 15*time.Minute)
	if slot != "2026-08-18T04:00:00Z" {
		t.Fatalf("slot %s", slot)
	}
	if ttl != 8*time.Minute {
		t.Fatalf("ttl %s", ttl)
	}
}

func TestClaimKey(t *testing.T) {
	if got := ClaimKey("2026-08-18T04:00:00Z", "inventory", "9"); got != "osspilot:claim:2026-08-18T04:00:00Z:inventory:9" {
		t.Fatalf("key %s", got)
	}
}

func TestDropCtxIgnoresCancel(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	c, stop := dropCtx(parent)
	defer stop()
	if err := c.Err(); err != nil {
		t.Fatalf("canceled parent leaked: %v", err)
	}
}
