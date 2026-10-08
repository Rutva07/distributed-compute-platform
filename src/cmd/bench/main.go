package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/distributed-compute-platform/internal/model"
)

type stats struct{ Total, Queued, Running, Succeeded, Failed, Canceled int64 }

func payload(i int) (string, any) {
	switch i % 100 {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34:
		return "json_transform", map[string]any{"object": map[string]any{"index": i, "value": i % 11}, "prefix": "item_"}
	case 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59:
		return "text", map[string]any{"text": "distributed compute task processing example"}
	case 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79:
		return "numeric", map[string]any{"n": 1000 + i%100}
	case 80, 81, 82, 83, 84, 85, 86, 87, 88, 89:
		return "aggregate", map[string]any{"values": []float64{1, 2, 3, 4, float64(i % 10)}}
	default:
		return "simulated_io", map[string]any{"delay_ms": 5}
	}
}
func main() {
	base := flag.String("url", "http://localhost:8080", "API URL")
	count := flag.Int("n", 10000, "jobs")
	conc := flag.Int("concurrency", 64, "concurrent submitters")
	timeout := flag.Duration("timeout", 3*time.Minute, "overall timeout")
	flag.Parse()
	if *count < 1 || *conc < 1 {
		log.Fatal("n and concurrency must be positive")
	}
	id, err := model.ID()
	if err != nil {
		log.Fatal(err)
	}
	batch := "bench-" + id
	transport := &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 512, MaxConnsPerHost: 512, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext}
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	started := time.Now()
	tasks := make(chan int)
	var wg sync.WaitGroup
	var accepted, errorsCount atomic.Int64
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range tasks {
				kind, p := payload(idx)
				reqBody, _ := json.Marshal(map[string]any{"type": kind, "payload": p, "batch_id": batch, "idempotency_key": fmt.Sprintf("%s-%d", batch, idx)})
				req, e := http.NewRequestWithContext(ctx, http.MethodPost, *base+"/v1/jobs", bytes.NewReader(reqBody))
				if e != nil {
					errorsCount.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				resp, e := client.Do(req)
				if e != nil {
					errorsCount.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 202 {
					errorsCount.Add(1)
				} else {
					accepted.Add(1)
				}
			}
		}()
	}
produce:
	for i := 0; i < *count; i++ {
		select {
		case tasks <- i:
		case <-ctx.Done():
			break produce
		}
	}
	close(tasks)
	wg.Wait()
	submitDuration := time.Since(started)
	fmt.Printf("batch=%s submitted=%d errors=%d submit_seconds=%.3f submit_jobs_per_sec=%.1f\n", batch, accepted.Load(), errorsCount.Load(), submitDuration.Seconds(), float64(accepted.Load())/submitDuration.Seconds())
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var final stats
	for {
		select {
		case <-ctx.Done():
			log.Printf("timeout: %v", ctx.Err())
			report(final, accepted.Load(), time.Since(started))
			return
		case <-ticker.C:
			req, e := http.NewRequestWithContext(ctx, http.MethodGet, *base+"/v1/batches/"+batch+"/stats", nil)
			if e != nil {
				continue
			}
			resp, e := client.Do(req)
			if e != nil {
				continue
			}
			if resp.StatusCode == 200 {
				e = json.NewDecoder(resp.Body).Decode(&final)
			} else {
				e = fmt.Errorf("HTTP %d", resp.StatusCode)
			}
			resp.Body.Close()
			if e != nil {
				continue
			}
			if final.Succeeded+final.Failed+final.Canceled >= accepted.Load() {
				report(final, accepted.Load(), time.Since(started))
				return
			}
		}
	}
}
func report(s stats, accepted int64, d time.Duration) {
	rate := 0.0
	throughput := 0.0
	if accepted > 0 {
		rate = float64(s.Succeeded) / float64(accepted) * 100
	}
	if d.Seconds() > 0 {
		throughput = float64(s.Succeeded) / d.Seconds()
	}
	fmt.Printf("completed=%d failed=%d canceled=%d outstanding=%d elapsed_sec=%.3f effective_jobs_per_sec=%.1f completion_rate=%.3f%%\n", s.Succeeded, s.Failed, s.Canceled, accepted-s.Succeeded-s.Failed-s.Canceled, d.Seconds(), throughput, rate)
}
