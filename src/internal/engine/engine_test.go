package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestWorkloads(t *testing.T) {
	cases := []struct{ kind, payload, contains string }{
		{"json_transform", `{"object":{"x":1},"prefix":"k_"}`, `"k_x":1`},
		{"text", `{"text":"one two"}`, `"word_count":2`},
		{"numeric", `{"n":10}`, `"sum":45`},
		{"aggregate", `{"values":[1,2,3]}`, `"mean":2`},
		{"simulated_io", `{"delay_ms":1}`, `"ok":true`},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			res, err := Run(context.Background(), tc.kind, json.RawMessage(tc.payload))
			if err != nil {
				t.Fatal(err)
			}
			if !json.Valid(res) {
				t.Fatal("invalid result")
			}
			if !strings.Contains(string(res), tc.contains) {
				t.Fatalf("got %s", res)
			}
		})
	}
}
func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := Run(ctx, "simulated_io", json.RawMessage(`{"delay_ms":500}`))
	if err == nil {
		t.Fatal("expected cancellation")
	}
}
