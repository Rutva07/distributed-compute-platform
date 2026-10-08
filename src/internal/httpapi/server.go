package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/example/distributed-compute-platform/internal/model"
	"github.com/example/distributed-compute-platform/internal/queue"
	"github.com/example/distributed-compute-platform/internal/store"
	"github.com/gorilla/websocket"
)

type Server struct {
	Store *store.Store
	Queue *queue.Queue
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.HandleFunc("POST /v1/jobs", s.submit)
	mux.HandleFunc("GET /v1/jobs", s.list)
	mux.HandleFunc("GET /v1/jobs/{id}", s.get)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("GET /v1/batches/{batch}/stats", s.stats)
	mux.HandleFunc("GET /v1/ws", s.watch)
	return logging(mux)
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode: %v", err)
	}
}
func bad(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
func validID(w http.ResponseWriter, id string) bool {
	if err := store.ValidateUUID(id); err != nil {
		bad(w, 400, "invalid UUID")
		return false
	}
	return true
}
func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	var req model.SubmitRequest
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil {
		bad(w, 400, "invalid request: "+err.Error())
		return
	}
	if err := d.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		bad(w, 400, "only one JSON object is accepted")
		return
	}
	if err := req.Validate(); err != nil {
		bad(w, 400, err.Error())
		return
	}
	j, err := s.Store.Create(r.Context(), req)
	if err != nil {
		log.Printf("submit: %v", err)
		bad(w, 500, "database error")
		return
	}
	writeJSON(w, http.StatusAccepted, j)
}
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(w, id) {
		return
	}
	j, err := s.Store.Get(r.Context(), id)
	if store.IsNotFound(err) {
		bad(w, 404, "job not found")
		return
	}
	if err != nil {
		bad(w, 500, "database error")
		return
	}
	writeJSON(w, 200, j)
}
func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(w, id) {
		return
	}
	j, err := s.Store.Cancel(r.Context(), id)
	if store.IsNotFound(err) {
		bad(w, 404, "job not found")
		return
	}
	if err != nil {
		bad(w, 500, "database error")
		return
	}
	s.Queue.Notify(r.Context(), id)
	writeJSON(w, 200, j)
}
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 1 || n > 500 {
			bad(w, 400, "limit must be 1-500")
			return
		}
		limit = n
	}
	jobs, err := s.Store.List(r.Context(), limit)
	if err != nil {
		bad(w, 500, "database error")
		return
	}
	writeJSON(w, 200, map[string]any{"jobs": jobs})
}
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	batch := r.PathValue("batch")
	if len(batch) > 128 {
		bad(w, 400, "batch too long")
		return
	}
	stats, err := s.Store.Stats(r.Context(), batch)
	if err != nil {
		bad(w, 500, "database error")
		return
	}
	writeJSON(w, 200, stats)
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, c := context.WithTimeout(r.Context(), 2*time.Second)
	defer c()
	if s.Store.DB.Ping(ctx) != nil || s.Queue.Client.Ping(ctx).Err() != nil {
		bad(w, 503, "dependencies unavailable")
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}

var upgrader = websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize: 2048}

func (s *Server) watch(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("job_id")
	if !validID(w, id) {
		return
	}
	if _, err := s.Store.Get(r.Context(), id); err != nil {
		if store.IsNotFound(err) {
			bad(w, 404, "job not found")
		} else {
			bad(w, 500, "database error")
		}
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	sub := s.Queue.Client.Subscribe(ctx, "jobs:"+id)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		return
	}
	var last string
	send := func() error {
		qctx, c := context.WithTimeout(ctx, 5*time.Second)
		defer c()
		j, e := s.Store.Get(qctx, id)
		if e != nil {
			return e
		}
		b, e := json.Marshal(j)
		if e != nil {
			return e
		}
		if string(b) == last {
			return nil
		}
		last = string(b)
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return conn.WriteMessage(websocket.TextMessage, b)
	}
	if send() != nil {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sub.Channel():
			if !ok {
				return
			}
			if send() != nil {
				return
			}
		case <-ticker.C:
			if send() != nil {
				return
			}
		}
	}
}
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path != "/health/live" && r.URL.Path != "/health/ready" && time.Since(started) > 2*time.Second {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(started))
		}
	})
}
