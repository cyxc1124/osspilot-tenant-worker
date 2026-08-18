package config

import "testing"

func TestConcurrency(t *testing.T) {
	t.Setenv("ASYNQ_CONCURRENCY", "")
	if concurrency() != 4 {
		t.Fatal("empty")
	}
	t.Setenv("ASYNQ_CONCURRENCY", "0")
	if concurrency() != 4 {
		t.Fatal("zero")
	}
	t.Setenv("ASYNQ_CONCURRENCY", "8")
	if concurrency() != 8 {
		t.Fatal("eight")
	}
}
