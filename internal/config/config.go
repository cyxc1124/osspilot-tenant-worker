package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	RedisURL         string
	S3Endpoint       string
	RGWAccessKey     string
	RGWSecretKey     string
	S3Region         string
	AsynqConcurrency int
}

func Load() Config {
	return Config{
		HTTPAddr:         getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		RedisURL:         os.Getenv("REDIS_URL"),
		S3Endpoint:       os.Getenv("S3_ENDPOINT"),
		RGWAccessKey:     os.Getenv("RGW_ACCESS_KEY"),
		RGWSecretKey:     os.Getenv("RGW_SECRET_KEY"),
		S3Region:         getenv("S3_REGION", "us-east-1"),
		AsynqConcurrency: concurrency(),
	}
}

func concurrency() int {
	n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("ASYNQ_CONCURRENCY")))
	if err != nil || n < 1 {
		return 4
	}
	return n
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
