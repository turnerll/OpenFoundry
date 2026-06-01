// Package world_monitor is the connector adapter for BransonOS / WorldMonitor.
//
// WorldMonitor provides macro intelligence feeds: geopolitical events,
// market signals, economic indicators, and news sentiment. This adapter
// syncs those feeds into OpenFoundry for correlation with personal
// portfolio and relationship data.
package world_monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/adapters"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

const (
	// ConnectorType is the registry key for WorldMonitor/BransonOS.
	ConnectorType = "world_monitor"
)

// Adapter implements [adapters.ConnectorAdapter] for WorldMonitor.
type Adapter struct {
	httpClient *http.Client
}

// New returns a ready-to-use WorldMonitor adapter.
func New() *Adapter {
	return &Adapter{httpClient: &http.Client{Timeout: 30 * time.Second}}
}

// Factory returns an [adapters.Factory] for the registry.
func Factory() adapters.Factory {
	return adapters.FactoryFunc(func() adapters.ConnectorAdapter { return New() })
}

// SetHTTPClient overrides the HTTP client for testing.
func (a *Adapter) SetHTTPClient(client *http.Client) {
	if client != nil {
		a.httpClient = client
	}
}

type monitorConfig struct {
	BaseURL string `json:"base_url"`
	APIKey  string `json:"api_key"`
	Feeds   []string `json:"feeds,omitempty"`
}

func parseConfig(raw json.RawMessage) (*monitorConfig, error) {
	cfg := &monitorConfig{}
	if len(raw) == 0 {
		return nil, errors.New("world_monitor: empty config")
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("world_monitor: invalid config: %w", err)
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("world_monitor: config missing 'base_url'")
	}
	return cfg, nil
}

var monitorSources = []adapters.Source{
	{Selector: "/api/events", DisplayName: "World Events", SourceKind: "world_events", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "/api/markets", DisplayName: "Market Signals", SourceKind: "market_signals", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "/api/economic-indicators", DisplayName: "Economic Indicators", SourceKind: "economic_indicators", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "/api/news-sentiment", DisplayName: "News Sentiment", SourceKind: "news_sentiment", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "/api/geopolitical", DisplayName: "Geopolitical Risk", SourceKind: "geopolitical_risk", SupportsSync: true, SupportsZeroCopy: false},
	{Selector: "/api/sector-signals", DisplayName: "Sector Signals", SourceKind: "sector_signals", SupportsSync: true, SupportsZeroCopy: true},
}

// DiscoverSources returns WorldMonitor feed types.
func (a *Adapter) DiscoverSources(_ context.Context, _ *models.Connection, _ string) ([]adapters.Source, error) {
	return monitorSources, nil
}

// QueryVirtualTable fetches a preview from WorldMonitor.
func (a *Adapter) QueryVirtualTable(ctx context.Context, c *models.Connection, q *adapters.Query, _ string) (*adapters.Result, error) {
	if q == nil {
		return nil, errors.New("world_monitor: query is nil")
	}
	cfg, err := parseConfig(c.Config)
	if err != nil {
		return nil, err
	}

	limit := 50
	if q.Limit != nil && *q.Limit > 0 && *q.Limit <= 500 {
		limit = *q.Limit
	}

	url := strings.TrimRight(cfg.BaseURL, "/") + q.Selector + fmt.Sprintf("?limit=%d", limit)
	body, err := a.fetch(ctx, cfg, url)
	if err != nil {
		return nil, err
	}

	rows := normalizeResponse(body)
	rawRows := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		buf, _ := json.Marshal(row)
		rawRows = append(rawRows, buf)
	}

	return &adapters.Result{
		Selector: q.Selector,
		Mode:     "zero_copy",
		Columns:  columnsFromRows(rawRows),
		RowCount: len(rawRows),
		Rows:     rawRows,
	}, nil
}

// StreamArrow is not supported.
func (a *Adapter) StreamArrow(_ context.Context, _ *models.Connection, _ *adapters.Query, _ string) (adapters.ArrowStream, error) {
	return nil, fmt.Errorf("%w: world_monitor arrow streaming", adapters.ErrNotImplemented)
}

// BuildIngestSpec produces the spec for ingestion-replication-service.
func (a *Adapter) BuildIngestSpec(_ context.Context, c *models.Connection, src *adapters.Source) (*adapters.IngestSpec, error) {
	if c == nil || src == nil {
		return nil, errors.New("world_monitor: connection or source is nil")
	}
	cfg, err := parseConfig(c.Config)
	if err != nil {
		return nil, err
	}
	specCfg, _ := json.Marshal(map[string]any{
		"base_url": cfg.BaseURL,
		"endpoint": src.Selector,
		"feeds":    cfg.Feeds,
	})
	return &adapters.IngestSpec{
		Name:      fmt.Sprintf("worldmonitor-%s", strings.TrimPrefix(src.Selector, "/api/")),
		Namespace: "personal",
		Source:    ConnectorType,
		Config:    specCfg,
	}, nil
}

func (a *Adapter) fetch(ctx context.Context, cfg *monitorConfig, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("world_monitor: build request: %w", err)
	}
	if cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("world_monitor: transport error: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("world_monitor: API returned HTTP %d", resp.StatusCode)
	}
	return body, nil
}

func normalizeResponse(body []byte) []any {
	var arr []any
	if json.Unmarshal(body, &arr) == nil {
		return arr
	}
	var obj map[string]any
	if json.Unmarshal(body, &obj) == nil {
		for _, key := range []string{"data", "items", "results", "events", "signals"} {
			if list, ok := obj[key].([]any); ok {
				return list
			}
		}
		return []any{obj}
	}
	return nil
}

func columnsFromRows(rows []json.RawMessage) []string {
	for _, row := range rows {
		var obj map[string]any
		if json.Unmarshal(row, &obj) == nil {
			cols := make([]string, 0, len(obj))
			for k := range obj {
				cols = append(cols, k)
			}
			return cols
		}
	}
	return nil
}
