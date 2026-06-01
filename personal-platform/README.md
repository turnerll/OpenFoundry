# Personal Operational Intelligence Platform

OpenFoundry configured as a personal Palantir for wealth, power, influence, and impact.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Personal Copilot (AI Agent)                       │
│  "What's my net worth?" · "Who should I reach out to?" · "ROI?"    │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
    ┌─────────────────┼─────────────────┐
    │                 │                 │
    ▼                 ▼                 ▼
┌─────────┐   ┌─────────────┐   ┌──────────────┐
│ Wealth  │   │ Influence & │   │   Impact &   │
│ Layer   │   │ Relationship│   │  Community   │
│         │   │   Layer     │   │    Layer     │
└────┬────┘   └──────┬──────┘   └──────┬───────┘
     │               │                  │
     ▼               ▼                  ▼
┌─────────┐   ┌─────────────┐   ┌──────────────┐
│Brokerage│   │ Twenty CRM  │   │  Paperclip   │
│  APIs   │   │ Hermes CRM  │   │  (AI Agents) │
│  FRED   │   │ Google/Cal  │   │  Workflows   │
│  SEC    │   │ LinkedIn    │   │  Initiatives │
└─────────┘   └─────────────┘   └──────────────┘
```

## External Integrations

| System | Purpose | Connector Type |
|--------|---------|----------------|
| **Twenty CRM** | Contact/company/deal management, activity tracking | GraphQL API (`twenty_crm`) |
| **Hermes** | AI agent CRM, outreach automation, content management | REST API (`hermes_crm`) |
| **Paperclip** | AI agent orchestration, zero-human workflows | REST API (`paperclip_agents`) |
| **BransonOS / WorldMonitor** | Macro intelligence, world event monitoring | REST API (`world_monitor`) |

## Quick Start

```bash
# 1. Deploy the full stack
cd /path/to/OpenFoundry
docker compose --profile foundation up -d

# 2. Seed the ontology
curl -X POST http://localhost:50103/api/v1/object-types \
  -H "Content-Type: application/json" \
  -d @personal-platform/ontology/financial-ontology.json

# 3. Create connectors
curl -X POST http://localhost:50088/api/v1/connections \
  -H "Content-Type: application/json" \
  -d @personal-platform/connectors/twenty-crm-connection.json

# 4. Register AI agents
curl -X POST http://localhost:50095/api/v1/agents \
  -H "Content-Type: application/json" \
  -d @personal-platform/agents/wealth-advisor.json

# 5. Deploy workflows
curl -X POST http://localhost:50076/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d @personal-platform/workflows/opportunity-tracker.json
```

## Directory Layout

```
personal-platform/
├── ontology/           # Object type definitions (financial, relationship, community)
├── connectors/         # External system connector configs (Twenty, Hermes, Paperclip)
├── pipelines/          # DAG transform definitions (net worth, influence, matching)
├── agents/             # AI agent definitions (wealth advisor, relationship AI, copilot)
├── workflows/          # Automation workflows (opportunity tracking, impact alerts)
├── apps/               # Workshop app definitions (dashboards, CRM view, impact app)
└── deploy/             # Deployment config (env vars, secrets, compose overlay)
```
