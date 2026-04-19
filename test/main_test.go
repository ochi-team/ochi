package test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	httpClient := &http.Client{}
	api := NewClient("http://localhost:9014", httpClient)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := api.Ready(ctx); err != nil {
		t.Fatalf("ready failed: %v", err)
	}

	insertPayload := InsertRequest{
		Streams: []InsertStream{
			{
				Stream: map[string]string{
					"tag1": "alpha",
					"tag2": "beta",
				},
				Values: [][]any{
					{"1710000000000000001", "same message", map[string]any{"field1": "x", "field2": "x"}},
				},
			},
		},
	}

	if err := api.InsertLokiJSON(ctx, insertPayload); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	if err := api.Flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	queryPayload := QueryRequest{
		Start: 1710000000000000000,
		End:   1710000000000000002,
		Tags: []Field{
			{Key: "tag1", Value: "alpha"},
			{Key: "tag2", Value: "beta"},
		},
		Fields: []Field{
			{Key: "field1", Value: "x"},
			{Key: "field2", Value: "x"},
		},
	}

	lines, err := api.Query(ctx, queryPayload)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 line, got %d", len(lines))
	}

	if lines[0].TimestampNs != 1710000000000000001 {
		t.Fatalf("unexpected timestamp: got %d", lines[0].TimestampNs)
	}

	actual := make(map[string]string, len(lines[0].Fields))
	for _, f := range lines[0].Fields {
		actual[f.Key] = f.Value
	}

	expected := map[string]string{
		"tag1":   "alpha",
		"tag2":   "beta",
		"field1": "x",
		"field2": "x",
		"":       "same message",
	}

	for k, v := range expected {
		got, ok := actual[k]
		if !ok {
			t.Fatalf("missing field key %q in %#v", k, lines[0].Fields)
		}
		if got != v {
			t.Fatalf("unexpected value for key %q: got %q want %q", k, got, v)
		}
	}

	if len(actual) != len(expected) {
		t.Fatalf("unexpected field set: got %s", fmt.Sprint(actual))
	}
}
