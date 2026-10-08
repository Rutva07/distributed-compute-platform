package runner

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/example/distributed-compute-platform/internal/config"
	"github.com/example/distributed-compute-platform/internal/engine"
	"github.com/example/distributed-compute-platform/internal/queue"
	"github.com/example/distributed-compute-platform/internal/store"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	Config config.Config
	Store  *store.Store
	Queue  *queue.Queue
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.Queue.Setup(ctx); err != nil {
		return err
	}
	messages := make(chan redis.XMessage, w.Config.WorkerConcurrency*4)
	var wg sync.WaitGroup
	for i := 0; i < w.Config.WorkerConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range messages {
				w.handle(ctx, msg)
			}
		}()
	}
	defer func() { close(messages); wg.Wait() }()
	recoverTick := time.NewTicker(5 * time.Second)
	defer recoverTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		select {
		case <-recoverTick.C:
			msgs, err := w.Queue.Reclaim(ctx, w.Config.Consumer, time.Duration(w.Config.LeaseSeconds+5)*time.Second)
			if err != nil {
				log.Printf("reclaim: %v", err)
			} else {
				for _, msg := range msgs {
					select {
					case messages <- msg:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		default:
		}
		msgs, err := w.Queue.Read(ctx, w.Config.Consumer)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("read: %v", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		for _, msg := range msgs {
			select {
			case messages <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
func (w *Worker) handle(parent context.Context, msg redis.XMessage) {
	id, ok := msg.Values["job_id"].(string)
	if !ok {
		log.Printf("invalid message %s", msg.ID)
		_ = w.Queue.Ack(context.Background(), msg.ID)
		return
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	job, claimed, err := w.Store.Claim(ctx, id, w.Config.Consumer, w.Config.LeaseSeconds)
	cancel()
	if err != nil {
		log.Printf("claim %s: %v", id, err)
		return
	}
	if !claimed {
		ctx, cancel = context.WithTimeout(parent, 10*time.Second)
		j, e := w.Store.Get(ctx, id)
		cancel()
		if e == nil && (j.Status == "running" || j.Status == "queued") {
			return
		}
		if e != nil && !store.IsNotFound(e) {
			log.Printf("inspect %s: %v", id, e)
			return
		}
		w.ack(msg.ID)
		return
	}
	w.notify(id)
	execCtx, stop := context.WithTimeout(parent, time.Duration(w.Config.MaxJobSeconds)*time.Second)
	defer stop()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Duration(w.Config.LeaseSeconds) * time.Second / 3)
		defer ticker.Stop()
		defer close(done)
		for {
			select {
			case <-execCtx.Done():
				return
			case <-ticker.C:
				heartbeatCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
				canceled, e := w.Store.Heartbeat(heartbeatCtx, id, w.Config.Consumer, w.Config.LeaseSeconds)
				c()
				if e != nil {
					log.Printf("heartbeat %s: %v", id, e)
					stop()
					return
				}
				if canceled {
					stop()
					return
				}
			}
		}
	}()
	result, runErr := engine.Run(execCtx, job.Type, job.Payload)
	stop()
	<-done
	writeCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	if runErr == nil {
		ok, err := w.Store.Succeed(writeCtx, id, w.Config.Consumer, result)
		if err != nil {
			log.Printf("save success %s: %v", id, err)
			return
		}
		if !ok {
			log.Printf("stale completion %s", id)
			return
		}
	}
	if runErr != nil {
		status, err := w.Store.Fail(writeCtx, id, w.Config.Consumer, fmt.Sprint(runErr))
		if err != nil {
			log.Printf("save failure %s: %v", id, err)
			return
		}
		if status == "" {
			return
		}
	}
	w.notify(id)
	w.ack(msg.ID)
}
func (w *Worker) ack(id string) {
	ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	if err := w.Queue.Ack(ctx, id); err != nil {
		log.Printf("ack %s: %v", id, err)
	}
}
func (w *Worker) notify(id string) {
	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()
	w.Queue.Notify(ctx, id)
}
