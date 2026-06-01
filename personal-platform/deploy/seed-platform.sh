#!/usr/bin/env bash
# seed-platform.sh — Seeds the personal platform ontology, connections, agents, and workflows.
#
# Prerequisites:
#   - OpenFoundry stack running (docker compose up -d)
#   - .env file configured (cp deploy/.env.example deploy/.env && edit)
#
# Usage:
#   cd personal-platform
#   ./deploy/seed-platform.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(dirname "$SCRIPT_DIR")"

# Load env if present
if [ -f "$SCRIPT_DIR/.env" ]; then
  set -a
  # shellcheck source=/dev/null
  source "$SCRIPT_DIR/.env"
  set +a
fi

# Service endpoints (default to local Docker)
ONTOLOGY_URL="${ONTOLOGY_URL:-http://localhost:50103}"
CONNECTOR_URL="${CONNECTOR_URL:-http://localhost:50088}"
AGENT_URL="${AGENT_URL:-http://localhost:50095}"
WORKFLOW_URL="${WORKFLOW_URL:-http://localhost:50076}"

echo "═══════════════════════════════════════════════════════════"
echo "  OpenFoundry Personal Platform — Seeding"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ─── 1. Seed Ontology Types ───
echo "📦 Seeding financial ontology..."
for file in "$PLATFORM_DIR"/ontology/*.json; do
  ontology_data=$(cat "$file")
  object_types=$(echo "$ontology_data" | jq -c '.object_types[]')
  while IFS= read -r obj_type; do
    name=$(echo "$obj_type" | jq -r '.name')
    # Build CreateObjectTypeRequest
    request=$(echo "$obj_type" | jq '{
      name: .name,
      display_name: .display_name,
      plural_display_name: .plural_display_name,
      description: .description,
      icon: .icon,
      color: .color,
      primary_key_property: .primary_key_property,
      editable: .editable
    }')
    
    echo "  → Creating object type: $name"
    response=$(curl -sf -X POST "$ONTOLOGY_URL/api/v1/object-types" \
      -H "Content-Type: application/json" \
      -d "$request" 2>/dev/null || echo '{"error":"already exists or failed"}')
    
    # Get the object type ID for properties
    obj_id=$(echo "$response" | jq -r '.id // empty')
    if [ -n "$obj_id" ]; then
      # Create properties
      properties=$(echo "$obj_type" | jq -c '.properties[]')
      while IFS= read -r prop; do
        prop_request=$(echo "$prop" | jq '{
          name: .name,
          display_name: .display_name,
          description: (.description // ""),
          property_type: .property_type,
          required: (.required // false),
          unique_constraint: (.unique_constraint // false)
        }')
        curl -sf -X POST "$ONTOLOGY_URL/api/v1/object-types/$obj_id/properties" \
          -H "Content-Type: application/json" \
          -d "$prop_request" > /dev/null 2>&1 || true
      done <<< "$properties"
    fi
  done <<< "$object_types"
  
  echo "  ✓ $(basename "$file") processed"
done

# ─── 2. Create Connections ───
echo ""
echo "🔌 Creating external connections..."
connections=$(jq -c '.connections[]' "$PLATFORM_DIR/connectors/connections.json")
while IFS= read -r conn; do
  name=$(echo "$conn" | jq -r '.name')
  echo "  → Connection: $name"
  
  # Substitute env vars in config
  config=$(echo "$conn" | envsubst)
  
  curl -sf -X POST "$CONNECTOR_URL/api/v1/connections" \
    -H "Content-Type: application/json" \
    -d "$config" > /dev/null 2>&1 || echo "    (may already exist)"
done <<< "$connections"
echo "  ✓ Connections configured"

# ─── 3. Register AI Agents ───
echo ""
echo "🤖 Registering AI agents..."
agents=$(jq -c '.agents[]' "$PLATFORM_DIR/agents/agents.json")
while IFS= read -r agent; do
  slug=$(echo "$agent" | jq -r '.slug')
  echo "  → Agent: $slug"
  
  curl -sf -X POST "$AGENT_URL/api/v1/agents" \
    -H "Content-Type: application/json" \
    -d "$agent" > /dev/null 2>&1 || echo "    (may already exist)"
done <<< "$agents"
echo "  ✓ Agents registered"

# ─── 4. Deploy Workflows ───
echo ""
echo "⚡ Deploying workflows..."
workflows=$(jq -c '.workflows[]' "$PLATFORM_DIR/workflows/workflows.json")
while IFS= read -r wf; do
  name=$(echo "$wf" | jq -r '.name')
  echo "  → Workflow: $name"
  
  # Substitute env vars
  wf_config=$(echo "$wf" | envsubst)
  
  curl -sf -X POST "$WORKFLOW_URL/api/v1/workflows" \
    -H "Content-Type: application/json" \
    -d "$wf_config" > /dev/null 2>&1 || echo "    (may already exist)"
done <<< "$workflows"
echo "  ✓ Workflows deployed"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  ✅ Personal platform seeded successfully!"
echo ""
echo "  Next steps:"
echo "    1. Configure .env with your API tokens"
echo "    2. Trigger initial sync: curl -X POST $CONNECTOR_URL/api/v1/connections/sync-all"
echo "    3. Open the UI: http://localhost:8080"
echo "    4. Ask the copilot: 'What is my net worth today?'"
echo "═══════════════════════════════════════════════════════════"
