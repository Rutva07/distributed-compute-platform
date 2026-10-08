package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/example/distributed-compute-platform/internal/config"
	"github.com/example/distributed-compute-platform/internal/runner"
	"github.com/example/distributed-compute-platform/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	cfg := config.Load()
	db, err := config.Database(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	q := config.Redis(cfg)
	defer q.Client.Close()
	log.Printf("worker %s concurrency=%d", cfg.Consumer, cfg.WorkerConcurrency)
	if err := (&runner.Worker{Config: cfg, Store: store.New(db), Queue: q}).Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
