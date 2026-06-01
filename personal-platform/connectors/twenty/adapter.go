// Package twenty is the connector adapter for Twenty CRM (twentyhq/twenty).
//
// Twenty exposes a GraphQL API at /api/graphql with token-based auth.
// This adapter maps Twenty's core objects (people, companies, opportunities,
// activities) into OpenFoundry's connector surface for ingestion into the
// relationship ontology.
//
// Capabilities:
//   - DiscoverSources — returns people, companies, opportunities, activities
//   - QueryVirtualTable — executes a GraphQL query for preview
//   - StreamArrow — not supported (GraphQL source)
//   - BuildIngestSpec — produces spec for ingestion-replication-service
package twenty

import (
	"bytes"
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
	// ConnectorType is the registry key for Twenty CRM.
	ConnectorType = "twenty_crm"
)

// Adapter implements [adapters.ConnectorAdapter] for Twenty CRM.
type Adapter struct {
	httpClient *http.Client
}

// New returns a ready-to-use Twenty CRM adapter.
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

type twentyConfig struct {
	BaseURL   string `json:"base_url"`
	APIToken  string `json:"api_token"`
	Workspace string `json:"workspace,omitempty"`
}

func parseConfig(raw json.RawMessage) (*twentyConfig, error) {
	cfg := &twentyConfig{}
	if len(raw) == 0 {
		return nil, errors.New("twenty_crm: empty config")
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("twenty_crm: invalid config: %w", err)
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("twenty_crm: config missing 'base_url'")
	}
	if strings.TrimSpace(cfg.APIToken) == "" {
		return nil, errors.New("twenty_crm: config missing 'api_token'")
	}
	return cfg, nil
}

// discoverable objects in Twenty CRM
var twentySources = []adapters.Source{
	{Selector: "people", DisplayName: "People", SourceKind: "twenty_people", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "companies", DisplayName: "Companies", SourceKind: "twenty_companies", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "opportunities", DisplayName: "Opportunities", SourceKind: "twenty_opportunities", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "activities", DisplayName: "Activities", SourceKind: "twenty_activities", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "notes", DisplayName: "Notes", SourceKind: "twenty_notes", SupportsSync: true, SupportsZeroCopy: true},
	{Selector: "tasks", DisplayName: "Tasks", SourceKind: "twenty_tasks", SupportsSync: true, SupportsZeroCopy: true},
}

// GraphQL queries for each Twenty CRM object type.
var graphqlQueries = map[string]string{
	"people": `query($limit: Int) {
		people(first: $limit) {
			edges { node { id firstName lastName email phone company { name } city jobTitle createdAt updatedAt } }
		}
	}`,
	"companies": `query($limit: Int) {
		companies(first: $limit) {
			edges { node { id name domainName address employees linkedinLink createdAt updatedAt } }
		}
	}`,
	"opportunities": `query($limit: Int) {
		opportunities(first: $limit) {
			edges { node { id name amount closeDate stage probability company { name } person { firstName lastName } createdAt } }
		}
	}`,
	"activities": `query($limit: Int) {
		activities(first: $limit) {
			edges { node { id title body type dueAt completedAt assignee { firstName lastName } createdAt } }
		}
	}`,
	"notes": `query($limit: Int) {
		notes(first: $limit) {
			edges { node { id title body createdAt updatedAt } }
		}
	}`,
	"tasks": `query($limit: Int) {
		tasks(first: $limit) {
			edges { node { id title body status dueAt assignee { firstName lastName } createdAt } }
		}
	}`,
}

// DiscoverSources returns the fixed catalog of Twenty CRM objects.
func (a *Adapter) DiscoverSources(_ context.Context, _ *models.Connection, _ string) ([]adapters.Source, error) {
	return twentySources, nil
}

// QueryVirtualTable executes a GraphQL query against Twenty CRM.
func (a *Adapter) QueryVirtualTable(ctx context.Context, c *models.Connection, q *adapters.Query, _ string) (*adapters.Result, error) {
	if q == nil {
		return nil, errors.New("twenty_crm: query is nil")
	}
	cfg, err := parseConfig(c.Config)
	if err != nil {
		return nil, err
	}

	query, ok := graphqlQueries[q.Selector]
	if !ok {
		return nil, fmt.Errorf("twenty_crm: unknown selector %q", q.Selector)
	}

	limit := 50
	if q.Limit != nil && *q.Limit > 0 && *q.Limit <= 100 {
		limit = *q.Limit
	}

	body, err := a.executeGraphQL(ctx, cfg, query, map[string]any{"limit": limit})
	if err != nil {
		return nil, err
	}

	rows := extractEdgeNodes(body, q.Selector)
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

// StreamArrow is not supported for GraphQL sources.
func (a *Adapter) StreamArrow(_ context.Context, _ *models.Connection, _ *adapters.Query, _ string) (adapters.ArrowStream, error) {
	return nil, fmt.Errorf("%w: twenty_crm arrow streaming", adapters.ErrNotImplemented)
}

// BuildIngestSpec produces the spec for ingestion-replication-service.
func (a *Adapter) BuildIngestSpec(_ context.Context, c *models.Connection, src *adapters.Source) (*adapters.IngestSpec, error) {
	if c == nil || src == nil {
		return nil, errors.New("twenty_crm: connection or source is nil")
	}
	cfg, err := parseConfig(c.Config)
	if err != nil {
		return nil, err
	}
	specCfg, _ := json.Marshal(map[string]any{
		"base_url":  cfg.BaseURL,
		"workspace": cfg.Workspace,
		"selector":  src.Selector,
		"query":     graphqlQueries[src.Selector],
	})
	return &adapters.IngestSpec{
		Name:      fmt.Sprintf("twenty-%s-%s", cfg.Workspace, src.Selector),
		Namespace: "personal",
		Source:    ConnectorType,
		Config:    specCfg,
	}, nil
}

// executeGraphQL sends a GraphQL request to Twenty CRM.
func (a *Adapter) executeGraphQL(ctx context.Context, cfg *twentyConfig, query string, variables map[string]any) (json.RawMessage, error) {
	payload, _ := json.Marshal(map[string]any{
		"query":     query,
		"variables": variables,
	})

	url := strings.TrimRight(cfg.BaseURL, "/") + "/api/graphql"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("twenty_crm: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.APIToken)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("twenty_crm: transport error: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("twenty_crm: API returned HTTP %d: %s", resp.StatusCode, string(body[:min(200, len(body))]))
	}
	return body, nil
}

// extractEdgeNodes pulls nodes from a GraphQL edges response.
func extractEdgeNodes(body json.RawMessage, selector string) []any {
	var resp struct {
		Data map[string]struct {
			Edges []struct {
				Node json.RawMessage `json:"node"`
			} `json:"edges"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil
	}
	edges, ok := resp.Data[selector]
	if !ok {
		return nil
	}
	nodes := make([]any, 0, len(edges.Edges))
	for _, edge := range edges.Edges {
		var node any
		if json.Unmarshal(edge.Node, &node) == nil {
			nodes = append(nodes, node)
		}
	}
	return nodes
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
