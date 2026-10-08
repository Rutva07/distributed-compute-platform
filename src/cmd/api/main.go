package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/distributed-compute-platform/internal/config"
	"github.com/example/distributed-compute-platform/internal/httpapi"
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
	redis := config.Redis(cfg)
	defer redis.Client.Close()
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: (&httpapi.Server{Store: store.New(db), Queue: redis}).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		graceCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = server.Shutdown(graceCtx)
	}()
	log.Printf("API listening on %s", cfg.HTTPAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
