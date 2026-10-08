package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL, RedisAddr, RedisPassword, HTTPAddr, Stream, Group, Consumer string
	WorkerConcurrency, BatchSize, LeaseSeconds, MaxJobSeconds                int
	PollInterval                                                             time.Duration
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func integer(key string, def int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil || n <= 0 {
		return def
	}
	return n
}
func Load() Config {
	h, _ := os.Hostname()
	return Config{
		DatabaseURL: env("DATABASE_URL", "postgres://compute:compute@localhost:5432/compute?sslmode=disable"),
		RedisAddr:   env("REDIS_ADDR", "localhost:6379"), RedisPassword: os.Getenv("REDIS_PASSWORD"),
		HTTPAddr: env("HTTP_ADDR", ":8080"), Stream: env("REDIS_STREAM", "compute:jobs"),
		Group: env("REDIS_GROUP", "compute-workers"), Consumer: env("WORKER_ID", h+"-"+strconv.Itoa(os.Getpid())),
		WorkerConcurrency: integer("WORKER_CONCURRENCY", 16), BatchSize: integer("DISPATCH_BATCH_SIZE", 100),
		LeaseSeconds: integer("JOB_LEASE_SECONDS", 120), MaxJobSeconds: integer("MAX_JOB_SECONDS", 30),
		PollInterval: time.Duration(integer("DISPATCH_POLL_MS", 100)) * time.Millisecond,
	}
}
