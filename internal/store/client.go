// Package store talks to the ymonitor-api Cloudflare Worker: authenticated
// ingest of batches and public reads of the current peer state.
package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"ymonitor/internal/collector"
)

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

func NewClient(baseURL, ingestToken string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   ingestToken,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Ingest POSTs a batch with retries. 4xx (except 429) are permanent errors —
// retrying the same payload would not help.
func (c *Client) Ingest(batch collector.Batch) error {
	body, err := batch.MarshalJSONLine()
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*2) * time.Second)
		}
		req, err := http.NewRequest(http.MethodPost, c.baseURL+"/v1/ingest", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusOK:
			return nil
		case resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
			return fmt.Errorf("ingest: HTTP %d: %s", resp.StatusCode, string(data))
		default:
			lastErr = fmt.Errorf("ingest: HTTP %d: %s", resp.StatusCode, string(data))
		}
	}
	return lastErr
}

// FetchPeers reads the public /v1/peers state.
func (c *Client) FetchPeers() ([]collector.PeerRow, error) {
	resp, err := c.http.Get(c.baseURL + "/v1/peers")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch peers: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Peers []collector.PeerRow `json:"peers"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&out); err != nil {
		return nil, err
	}
	return out.Peers, nil
}
