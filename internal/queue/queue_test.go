package queue

import "testing"

func TestNewNilWithoutRedis(t *testing.T) {
	if New("") != nil {
		t.Fatal("expected nil client without REDIS_URL")
	}
	if _, err := (*Client)(nil).EnqueueInventory(t.Context(), "b"); err != ErrUnavailable {
		t.Fatalf("got %v", err)
	}
}
