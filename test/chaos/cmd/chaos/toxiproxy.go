package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// toxiproxy drives the toxiproxy HTTP API (deploy/toxiproxy.json defines the proxies
// broker<N>_internal and broker<N>_raft; every broker reaches its peers through them).
type toxiproxy struct {
	base string
	http *http.Client
}

func newToxiproxy(base string) *toxiproxy {
	return &toxiproxy{base: base, http: &http.Client{Timeout: 5 * time.Second}}
}

// proxyNames returns the internal and Raft proxies of a broker: the inbound paths to it.
func proxyNames(broker int32) []string {
	return []string{fmt.Sprintf("broker%d_internal", broker), fmt.Sprintf("broker%d_raft", broker)}
}

// allProxies returns every proxy.
func allProxies() []string {
	var out []string
	for b := int32(1); b <= 3; b++ {
		out = append(out, proxyNames(b)...)
	}
	return out
}

func (t *toxiproxy) do(ctx context.Context, method, path string, body any) error {
	var r io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.base+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.http.Do(req)
	if err != nil {
		return fmt.Errorf("toxiproxy %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("toxiproxy %s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(msg))
	}
	return nil
}

// ready reports whether the API answers.
func (t *toxiproxy) ready(ctx context.Context) error {
	return t.do(ctx, http.MethodGet, "/proxies", nil)
}

// setEnabled enables or disables a proxy. Disabling closes its existing connections and refuses
// new ones.
func (t *toxiproxy) setEnabled(ctx context.Context, proxy string, enabled bool) error {
	return t.do(ctx, http.MethodPost, "/proxies/"+proxy, map[string]any{"enabled": enabled})
}

// toxic is one toxic definition.
type toxic struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Stream     string         `json:"stream"`
	Toxicity   float64        `json:"toxicity"`
	Attributes map[string]int `json:"attributes"`
}

// addToxic adds a toxic to a proxy.
func (t *toxiproxy) addToxic(ctx context.Context, proxy string, tx toxic) error {
	return t.do(ctx, http.MethodPost, "/proxies/"+proxy+"/toxics", tx)
}

// reset re-enables every proxy and removes every toxic.
func (t *toxiproxy) reset(ctx context.Context) error {
	return t.do(ctx, http.MethodPost, "/reset", nil)
}
