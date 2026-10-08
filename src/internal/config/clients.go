package config

import (
	"context"
	"time"

	"github.com/example/distributed-compute-platform/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Database(ctx context.Context, c Config) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(integer("DB_MAX_CONNS", 12))
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
func Redis(c Config) *queue.Queue { return queue.New(c.RedisAddr, c.RedisPassword, c.Stream, c.Group) }
