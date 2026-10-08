package model

import (
	"encoding/json"
	"testing"
)

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		kind string
		ok   bool
	}{{"text", true}, {"nonsense", false}} {
		r := SubmitRequest{Type: tc.kind, Payload: json.RawMessage(`{}`)}
		err := r.Validate()
		if (err == nil) != tc.ok {
			t.Fatalf("%q: %v", tc.kind, err)
		}
		if err == nil && r.MaxAttempts != 3 {
			t.Fatal("default attempts")
		}
	}
	a, _ := ID()
	b, _ := ID()
	if a == b || len(a) != 36 {
		t.Fatal("invalid IDs")
	}
}
