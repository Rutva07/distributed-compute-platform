package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var api = os.Getenv("INTEGRATION_URL")

func submit(t *testing.T, kind, payload, key string) map[string]any {
	t.Helper()
	data, _ := json.Marshal(map[string]any{"type": kind, "payload": json.RawMessage(payload), "idempotency_key": key})
	resp, err := http.Post(api+"/v1/jobs", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Fatalf("submit returned HTTP %d", resp.StatusCode)
	}
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}
func get(t *testing.T, id string) map[string]any {
	t.Helper()
	resp, err := http.Get(api + "/v1/jobs/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET returned %d", resp.StatusCode)
	}
	var job map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	return job
}
func wait(t *testing.T, id, status string) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		j := get(t, id)
		if j["status"] == status {
			return
		}
		if j["status"] == "failed" || j["status"] == "canceled" || j["status"] == "succeeded" {
			t.Fatalf("expected %s, got %v", status, j)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out awaiting %s", status)
}
func TestIntegration(t *testing.T) {
	if api == "" {
		t.Skip("set INTEGRATION_URL to run")
	}
	t.Run("complete and idempotency", func(t *testing.T) {
		key := fmt.Sprintf("itest-%d", time.Now().UnixNano())
		a := submit(t, "text", `{"text":"hello world"}`, key)
		b := submit(t, "text", `{"text":"hello world"}`, key)
		if a["id"] != b["id"] {
			t.Fatal("duplicate job created")
		}
		wait(t, a["id"].(string), "succeeded")
		j := get(t, a["id"].(string))
		if j["result"] == nil {
			t.Fatal("missing result")
		}
	})
	t.Run("retry eventually fails", func(t *testing.T) {
		j := submit(t, "numeric", `{"n":-1}`, "")
		wait(t, j["id"].(string), "failed")
		last := get(t, j["id"].(string))
		if last["attempts"] != float64(3) {
			t.Fatalf("expected 3 attempts; got %v", last["attempts"])
		}
	})
	t.Run("websocket snapshot", func(t *testing.T) {
		j := submit(t, "simulated_io", `{"delay_ms":50}`, "")
		url := strings.Replace(api, "http://", "ws://", 1)
		url = strings.Replace(url, "https://", "wss://", 1) + "/v1/ws?job_id=" + j["id"].(string)
		c, resp, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			t.Fatalf("websocket %v (response %v)", err, resp)
		}
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		var snapshot map[string]any
		if err := c.ReadJSON(&snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot["id"] != j["id"] {
			t.Fatalf("wrong snapshot: %v", snapshot)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		j := submit(t, "simulated_io", `{"delay_ms":5000}`, "")
		req, _ := http.NewRequest(http.MethodPost, api+"/v1/jobs/"+j["id"].(string)+"/cancel", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("cancel HTTP %d", resp.StatusCode)
		}
		wait(t, j["id"].(string), "canceled")
	})
}
