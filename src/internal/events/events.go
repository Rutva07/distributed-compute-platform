package events

import (
	"context"
	"log"
	"time"

	"github.com/example/distributed-compute-platform/internal/config"
	"github.com/example/distributed-compute-platform/internal/queue"
	"github.com/example/distributed-compute-platform/internal/store"
)

type Dispatcher struct {
	Config config.Config
	Store  *store.Store
	Queue  *queue.Queue
}

func (d *Dispatcher) Run(ctx context.Context) error {
	if err := d.Queue.Setup(ctx); err != nil {
		return err
	}
	tick := time.NewTicker(d.Config.PollInterval)
	defer tick.Stop()
	owner := d.Config.Consumer
	sweep := time.NewTicker(10 * time.Second)
	defer sweep.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		entries, err := d.Store.TakeOutbox(ctx, owner, d.Config.BatchSize)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := d.Queue.Publish(ctx, e.JobID); err != nil {
				log.Printf("publish %d: %v", e.ID, err)
				break
			}
			if err := d.Store.MarkPublished(ctx, e.ID, owner); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sweep.C:
			if _, err := d.Store.RecoverTerminal(ctx); err != nil {
				log.Printf("sweep: %v", err)
			}
		case <-tick.C:
		}
	}
}
