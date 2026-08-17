package config

import "os"

type Config struct {
	HTTPAddr     string
	DatabaseURL  string
	RedisURL     string
	S3Endpoint   string
	RGWAccessKey string
	RGWSecretKey string
	S3Region     string
}

func Load() Config {
	return Config{
		HTTPAddr:     getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		RedisURL:     os.Getenv("REDIS_URL"),
		S3Endpoint:   os.Getenv("S3_ENDPOINT"),
		RGWAccessKey: os.Getenv("RGW_ACCESS_KEY"),
		RGWSecretKey: os.Getenv("RGW_SECRET_KEY"),
		S3Region:     getenv("S3_REGION", "us-east-1"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
