package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/golang/snappy"
)

type Field struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type QueryRequest struct {
	Start  uint64  `json:"start"`
	End    uint64  `json:"end"`
	Tags   []Field `json:"tags"`
	Fields []Field `json:"fields"`
}

type QueryLine struct {
	TimestampNs uint64  `json:"timestampNs"`
	Fields      []Field `json:"fields"`
}

type InsertRequest struct {
	Streams []InsertStream `json:"streams"`
}

type InsertStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]any           `json:"values"`
}

type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string, httpClient *http.Client) *Client {
	return &Client{baseURL: baseURL, http: httpClient}
}

func (c *Client) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/insert/loki/ready", nil)
	if err != nil {
		return fmt.Errorf("create ready request: %w", err)
	}

	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("execute ready request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("ready status: %d body: %q", res.StatusCode, string(body))
	}

	return nil
}

func (c *Client) InsertLokiJSON(ctx context.Context, payload InsertRequest) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal insert payload: %w", err)
	}

	snappyBody := snappy.Encode(nil, encoded)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/insert/loki/api/v1/push", bytes.NewReader(snappyBody))
	if err != nil {
		return fmt.Errorf("create insert request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("content-encoding", "snappy")

	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("execute insert request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("insert status: %d body: %q", res.StatusCode, string(body))
	}

	return nil
}

func (c *Client) Flush(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/flush", nil)
	if err != nil {
		return fmt.Errorf("create flush request: %w", err)
	}
	req.Header.Set("content-type", "application/json")

	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("execute flush request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("flush status: %d body: %q", res.StatusCode, string(responseBody))
	}

	return nil
}

func (c *Client) Query(ctx context.Context, payload QueryRequest) ([]QueryLine, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal query payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/query", bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("create query request: %w", err)
	}
	req.Header.Set("content-type", "application/json")

	res, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute query request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("query status: %d body: %q", res.StatusCode, string(responseBody))
	}

	responseBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("read query response: %w", err)
	}

	var out []QueryLine
	if err := json.Unmarshal(responseBody, &out); err != nil {
		return nil, fmt.Errorf("unmarshal query response: %w", err)
	}

	return out, nil
}
