package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type inputMap map[string]json.RawMessage

func field[T any](m inputMap, name string, dest *T) error {
	v, ok := m[name]
	if !ok {
		return fmt.Errorf("missing %s", name)
	}
	if err := json.Unmarshal(v, dest); err != nil {
		return fmt.Errorf("invalid %s: %w", name, err)
	}
	return nil
}
func Run(ctx context.Context, kind string, payload json.RawMessage) (json.RawMessage, error) {
	var m inputMap
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, errors.New("payload must be an object")
	}
	var result any
	switch kind {
	case "json_transform":
		var obj map[string]any
		if err := field(m, "object", &obj); err != nil {
			return nil, err
		}
		var prefix string
		_ = json.Unmarshal(m["prefix"], &prefix)
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(obj))
		for _, k := range keys {
			out[prefix+k] = obj[k]
		}
		result = map[string]any{"object": out, "count": len(out)}
	case "text":
		var s string
		if err := field(m, "text", &s); err != nil {
			return nil, err
		}
		result = map[string]any{"uppercase": strings.ToUpper(s), "word_count": len(strings.Fields(s)), "bytes": len(s)}
	case "numeric":
		var n int
		if err := field(m, "n", &n); err != nil {
			return nil, err
		}
		if n < 0 || n > 1000000 {
			return nil, errors.New("n must be between 0 and 1000000")
		}
		var sum int64
		for i := 0; i < n; i++ {
			if i%4096 == 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				default:
				}
			}
			sum += int64(i)
		}
		result = map[string]any{"n": n, "sum": sum}
	case "aggregate":
		var values []float64
		if err := field(m, "values", &values); err != nil {
			return nil, err
		}
		if len(values) > 100000 {
			return nil, errors.New("too many values")
		}
		var sum float64
		for _, x := range values {
			if math.IsNaN(x) || math.IsInf(x, 0) {
				return nil, errors.New("non-finite value")
			}
			sum += x
		}
		mean := 0.0
		if len(values) > 0 {
			mean = sum / float64(len(values))
		}
		result = map[string]any{"count": len(values), "sum": sum, "mean": mean}
	case "simulated_io":
		var ms int
		if err := field(m, "delay_ms", &ms); err != nil {
			return nil, err
		}
		if ms < 0 || ms > 5000 {
			return nil, errors.New("delay_ms must be between 0 and 5000")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(ms) * time.Millisecond):
		}
		result = map[string]any{"delay_ms": ms, "ok": true}
	default:
		return nil, errors.New("unsupported job type")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return json.Marshal(result)
}
