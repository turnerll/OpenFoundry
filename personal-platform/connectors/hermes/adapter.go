// Package hermes is the connector adapter for Hermes CRM/Agent Dashboard.
//
// Hermes provides AI agent-powered CRM with outreach automation, content
// management, and analytics. This adapter connects to its REST API to sync
// contacts, outreach campaigns, content, and analytics data into the
// OpenFoundry relationship ontology.
package hermes

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
	// ConnectorType is the registry key for Hermes CRM.
	ConnectorType = "hermes_crm"
)

// Adapter implements [adapters.ConnectorAdapter] for Hermes CRM.
type Adapter struct {
	httpClient *http.Client
}

// New returns a ready-to-use Hermes adapter.
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

type hermesConfig struct {
	BaseURL string `json:"base_url"`
	APIKey  string `json:"api_key"`
}

func parseConfig(raw json.RawMessage) (*hermesConfig, error) {
	cfg := &hermesConfig{}
	if len(raw) == 0 {
		return nil, errors.New("hermes_crm: empty config")
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("hermes_crm: invalid config: %w", err)
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("hermes_crm: config missing 'base_url'")
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, errors.New("hermes_crm: config missing 'api_key'")
	}
	return cfg, nil
}

var hermesSources = []adapters.Source{
	{Selector: "/api/contacts", DisplayName: "Contacts", SourceKind: "hermes_contacts", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "/api/campaigns", DisplayName: "Outreach Campaigns", SourceKind: "hermes_campaigns", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "/api/interactions", DisplayName: "Interactions", SourceKind: "hermes_interactions", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "/api/content", DisplayName: "Content", SourceKind: "hermes_content", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "/api/analytics", DisplayName: "Analytics", SourceKind: "hermes_analytics", SupportsSync: true, SupportsZeroCopy: false},
}

// DiscoverSources returns Hermes CRM object types.
func (a *Adapter) DiscoverSources(_ context.Context, _ *models.Connection, _ string) ([]adapters.Source, error) {
	return hermesSources, nil
}

// QueryVirtualTable fetches a preview from a Hermes endpoint.
func (a *Adapter) QueryVirtualTable(ctx context.Context, c *models.Connection, q *adapters.Query, _ string) (*adapters.Result, error) {
	if q == nil {
		return nil, errors.New("hermes_crm: query is nil")
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
	return nil, fmt.Errorf("%w: hermes_crm arrow streaming", adapters.ErrNotImplemented)
}

// BuildIngestSpec produces the spec for ingestion-replication-service.
func (a *Adapter) BuildIngestSpec(_ context.Context, c *models.Connection, src *adapters.Source) (*adapters.IngestSpec, error) {
	if c == nil || src == nil {
		return nil, errors.New("hermes_crm: connection or source is nil")
	}
	cfg, err := parseConfig(c.Config)
	if err != nil {
		return nil, err
	}
	specCfg, _ := json.Marshal(map[string]any{
		"base_url": cfg.BaseURL,
		"endpoint": src.Selector,
	})
	return &adapters.IngestSpec{
		Name:      fmt.Sprintf("hermes-%s", strings.TrimPrefix(src.Selector, "/api/")),
		Namespace: "personal",
		Source:    ConnectorType,
		Config:    specCfg,
	}, nil
}

func (a *Adapter) fetch(ctx context.Context, cfg *hermesConfig, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("hermes_crm: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hermes_crm: transport error: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("hermes_crm: API returned HTTP %d", resp.StatusCode)
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
		for _, key := range []string{"data", "items", "results", "contacts", "campaigns"} {
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
