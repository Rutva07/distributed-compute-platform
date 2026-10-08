package queue

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	Client        *redis.Client
	Stream, Group string
}

func New(addr, password, stream, group string) *Queue {
	return &Queue{Client: redis.NewClient(&redis.Options{Addr: addr, Password: password, PoolSize: 40}), Stream: stream, Group: group}
}
func (q *Queue) Setup(ctx context.Context) error {
	err := q.Client.XGroupCreateMkStream(ctx, q.Stream, q.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}
func (q *Queue) Publish(ctx context.Context, id string) error {
	return q.Client.XAdd(ctx, &redis.XAddArgs{Stream: q.Stream, Values: map[string]any{"job_id": id}}).Err()
}
func (q *Queue) Read(ctx context.Context, consumer string) ([]redis.XMessage, error) {
	streams, err := q.Client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: q.Group, Consumer: consumer, Streams: []string{q.Stream, ">"}, Count: 16, Block: time.Second}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var msgs []redis.XMessage
	for _, s := range streams {
		msgs = append(msgs, s.Messages...)
	}
	return msgs, nil
}
func (q *Queue) Reclaim(ctx context.Context, consumer string, minIdle time.Duration) ([]redis.XMessage, error) {
	msgs, _, err := q.Client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: q.Stream, Group: q.Group, Consumer: consumer, MinIdle: minIdle, Start: "0-0", Count: 16}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	return msgs, err
}
func (q *Queue) Ack(ctx context.Context, id string) error {
	return q.Client.XAck(ctx, q.Stream, q.Group, id).Err()
}
func (q *Queue) Notify(ctx context.Context, id string) { q.Client.Publish(ctx, "jobs:"+id, "changed") }
