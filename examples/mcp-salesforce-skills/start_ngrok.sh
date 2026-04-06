#/bin/zsh
# ---------------------------------------------------------------------------
# start_ngrok.sh — Run the Salesforce CMS Skills MCP server and expose it
#                  publicly via ngrok so Salesforce Agentforce can reach it.
#
# Prerequisites:
#   brew install ngrok          (or: https://ngrok.com/download)
#   ngrok config add-authtoken <YOUR_NGROK_TOKEN>
#
# Usage:
#   ./start_ngrok.sh                         # default port 2034
#   ./start_ngrok.sh --port 9000             # custom port
#   ./start_ngrok.sh --token mysecrettoken   # require Bearer auth
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"   # OpenViking repo root
PORT=2034
BEARER_TOKEN=""

# --- parse args ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)   PORT="$2";         shift 2 ;;
    --token)  BEARER_TOKEN="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# --- verify dependencies ---
if ! command -v ngrok &>/dev/null; then
  echo "ERROR: ngrok not found."
  echo "Install: brew install ngrok  OR  https://ngrok.com/download"
  echo "Then:    ngrok config add-authtoken <YOUR_NGROK_TOKEN>"
  exit 1
fi

# Pick Python 3.11+ (repo requires >=3.10 for union-type syntax; system 3.9 is too old)
PYTHON=""
for candidate in python3.13 python3.12 python3.11 python3.10 python3; do
  if command -v "$candidate" &>/dev/null; then
    _ver=$("$candidate" -c "import sys; print(sys.version_info[:2])" 2>/dev/null)
    if [[ "$_ver" > "(3, 9)" ]]; then   # string compare works for tuples here
      PYTHON="$candidate"
      break
    fi
  fi
done

if [[ -z "$PYTHON" ]]; then
  echo "ERROR: Python 3.10+ is required but not found."
  echo "Install: brew install python@3.11"
  exit 1
fi
echo "Using $($PYTHON --version)"

# --- install Python deps if needed ---
# Install the local openviking package (editable) so the cms_adapter is available
# without needing it on PyPI.  Then install mcp.
if ! "$PYTHON" -c "from openviking.storage.cms_adapter import SalesforceCMSFS" 2>/dev/null; then
  echo "Installing openviking from repo..."
  "$PYTHON" -m pip install --quiet -e "${REPO_ROOT}"
fi

if ! "$PYTHON" -c "import mcp" 2>/dev/null; then
  echo "Installing mcp..."
  "$PYTHON" -m pip install --quiet "mcp>=1.8.0"
fi

# --- start MCP server ---
echo "Starting MCP server on port ${PORT}..."
cd "$SCRIPT_DIR"

SERVER_CMD=("$PYTHON" server.py --port "$PORT")
[[ -n "$BEARER_TOKEN" ]] && SERVER_CMD+=(--token "$BEARER_TOKEN")

"${SERVER_CMD[@]}" &
SERVER_PID=$!

# give the server a moment to bind
sleep 2

# verify it started
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "ERROR: MCP server failed to start."
  exit 1
fi

# --- start ngrok ---
echo "Starting ngrok tunnel for port ${PORT}..."
NGROK_ARGS=(http "$PORT" --log stderr)

# If a bearer token is set, add ngrok's traffic policy so only requests
# with the correct Authorization header reach the server.
if [[ -n "$BEARER_TOKEN" ]]; then
  POLICY=$(cat <<EOF
{
  "inbound": [{
    "expressions": ["req.headers['authorization'] != 'Bearer ${BEARER_TOKEN}'"],
    "actions": [{"type": "deny", "config": {"status_code": 401}}]
  }]
}
EOF
)
  NGROK_ARGS+=(--traffic-policy "$POLICY")
fi

ngrok "${NGROK_ARGS[@]}" &
NGROK_PID=$!

# wait for ngrok to register with the API
sleep 3

# --- extract public URL ---
PUBLIC_URL=$(
  curl -s http://localhost:4040/api/tunnels 2>/dev/null \
  | "$PYTHON" -c "
import sys, json
data = json.load(sys.stdin)
tunnels = data.get('tunnels', [])
https = [t for t in tunnels if t.get('proto') == 'https']
print((https or tunnels)[0]['public_url'])
" 2>/dev/null
) || true

if [[ -z "$PUBLIC_URL" ]]; then
  echo "WARNING: Could not auto-detect ngrok URL — check http://localhost:4040"
  PUBLIC_URL="https://<your-ngrok-subdomain>.ngrok-free.app"
fi

MCP_URL="${PUBLIC_URL}/mcp"

# --- print Agentforce setup instructions ---
echo ""
echo "============================================================"
echo "  MCP endpoint ready"
echo "============================================================"
echo ""
echo "  Local  : http://localhost:${PORT}/mcp"
echo "  Public : ${MCP_URL}"
echo ""
if [[ -n "$BEARER_TOKEN" ]]; then
echo "  Auth   : Bearer ${BEARER_TOKEN}"
echo "           (set as Named Credential password in Salesforce)"
echo ""
fi
echo "------------------------------------------------------------"
echo "  Salesforce Agentforce — add this MCP server:"
echo ""
echo "  1. Setup → Agents → Agent Actions → New"
echo "     Action Type : MCP Server"
echo "     Server URL  : ${MCP_URL}"
if [[ -n "$BEARER_TOKEN" ]]; then
echo "     Auth Type   : Bearer Token  →  ${BEARER_TOKEN}"
fi
echo ""
echo "  2. Agentforce will discover these tools automatically:"
echo "       • list_skills   — browse all skills in CMS"
echo "       • get_skill     — read a specific skill"
echo "       • get_skill_raw — raw markdown body"
echo "       • reload_skills — refresh cache from Salesforce CMS"
echo ""
echo "  3. Assign the actions to your Agent in Agent Builder"
echo "     and activate the Agent."
echo "------------------------------------------------------------"
echo ""
echo "  ngrok inspector : http://localhost:4040"
echo "  Press Ctrl+C to stop both processes."
echo "============================================================"
echo ""

# --- wait and clean up ---
_cleanup() {
  echo ""
  echo "Shutting down..."
  kill "$SERVER_PID" "$NGROK_PID" 2>/dev/null || true
  wait "$SERVER_PID" "$NGROK_PID" 2>/dev/null || true
  echo "Done."
}
trap _cleanup INT TERM

wait
