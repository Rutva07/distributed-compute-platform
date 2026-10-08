package main

import (
	"context"
	"log"
	"time"

	"github.com/example/distributed-compute-platform/internal/config"
	"github.com/example/distributed-compute-platform/internal/migrations"
)

func main() {
	ctx, c := context.WithTimeout(context.Background(), 60*time.Second)
	defer c()
	db, err := config.Database(ctx, config.Load())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := migrations.Apply(ctx, db); err != nil {
		log.Fatal(err)
	}
	log.Println("migrations complete")
}
