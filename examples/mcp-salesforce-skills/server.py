#!/usr/bin/env python3
"""
Salesforce CMS Skills MCP Server

Loads SKILL.md files from the Salesforce CMS `agent/skills` folder and
exposes them to MCP clients as tools and resources.

Credential resolution order:
  1. OV_CONFIG_JSON env var  — full ov.conf JSON string (recommended for Heroku)
  2. SF_* env vars           — individual Salesforce fields (alternative for Heroku)
  3. --config file           — local ov.conf file (default: ~/.openviking/ov.conf)

Usage (local):
  uv run server.py
  uv run server.py --config ~/.openviking/ov.conf --port 2034
  uv run server.py --transport stdio

Usage (Heroku):
  heroku config:set OV_CONFIG_JSON="$(cat ~/.openviking/ov.conf)"
  git push heroku main

Claude CLI:
  claude mcp add --transport http salesforce-skills https://<your-app>.herokuapp.com/mcp
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from openviking.core.skill_loader import SkillLoader
from openviking.storage.cms_adapter import CMSClient, SalesforceCMSConfig, SalesforceCMSFS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("salesforce-cms-skills")

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
_config_path: str = str(Path("~/.openviking/ov.conf").expanduser())
_bearer_token: str = ""
_fs: Optional[SalesforceCMSFS] = None
_skills_cache: Optional[Dict[str, Any]] = None  # name → skill_dict

_SKILLS_ROOT = "/viking/agent/skills/"


# ---------------------------------------------------------------------------
# Config loading (file, OV_CONFIG_JSON, or individual SF_* env vars)
# ---------------------------------------------------------------------------

def _load_ov_conf() -> Dict[str, Any]:
    """Load ov.conf from the first available source."""
    # 1. Full JSON blob in env var (ideal for Heroku / 12-factor)
    config_json = os.getenv("OV_CONFIG_JSON")
    if config_json:
        logger.info("Loading config from OV_CONFIG_JSON")
        return json.loads(config_json)

    # 2. Individual Salesforce env vars (alternative Heroku approach)
    if os.getenv("SF_INSTANCE_URL"):
        logger.info("Loading config from SF_* environment variables")
        folder_map_raw = os.getenv("SF_FOLDER_MAP", "{}")
        return {
            "storage": {
                "cms": {
                    "instance_url":  os.environ["SF_INSTANCE_URL"],
                    "client_id":     os.environ["SF_CLIENT_ID"],
                    "client_secret": os.environ["SF_CLIENT_SECRET"],
                    "space_id":      os.environ["SF_SPACE_ID"],
                    "api_version":   os.getenv("SF_API_VERSION", "v66.0"),
                    "bu_context":    os.getenv("SF_BU_CONTEXT", "default"),
                    "content_type":  os.getenv("SF_CONTENT_TYPE", "ov_node"),
                    "folder_map":    json.loads(folder_map_raw),
                }
            }
        }

    # 3. File-based config (local development)
    config_file = Path(_config_path).expanduser()
    if not config_file.exists():
        raise FileNotFoundError(
            f"No config found. Set OV_CONFIG_JSON, SF_INSTANCE_URL, "
            f"or provide a config file at: {config_file}"
        )
    logger.info("Loading config from file: %s", config_file)
    with config_file.open() as fh:
        raw = os.path.expandvars(fh.read())
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Adapter bootstrap
# ---------------------------------------------------------------------------

def _get_fs() -> SalesforceCMSFS:
    global _fs
    if _fs is None:
        ov_conf = _load_ov_conf()
        cms_dict = ov_conf.get("storage", {}).get("cms")
        if not cms_dict:
            raise ValueError("Config is missing storage.cms section")
        cfg = SalesforceCMSConfig(**cms_dict)
        client = CMSClient(cfg)
        _fs = SalesforceCMSFS(client, cfg.space_id, cfg.bu_context)
        logger.info("Connected to Salesforce CMS: %s", cfg.instance_url)
    return _fs


# ---------------------------------------------------------------------------
# Skill discovery
# ---------------------------------------------------------------------------

def _load_all_skills() -> Dict[str, Any]:
    """Fetch and parse all skill files directly under /viking/agent/skills/."""
    fs = _get_fs()
    skills: Dict[str, Any] = {}

    try:
        entries = fs.ls(_SKILLS_ROOT)
    except FileNotFoundError:
        logger.warning("Skills root not found in CMS: %s", _SKILLS_ROOT)
        return skills

    for entry in entries:
        if entry.get("isDir"):
            continue
        skill_name = entry["name"]
        skill_path = f"{_SKILLS_ROOT.rstrip('/')}/{skill_name}"
        try:
            raw_bytes = fs.read(skill_path)
            content = raw_bytes.decode("utf-8", errors="replace")
            try:
                skill_dict = SkillLoader.parse(content, source_path=skill_path)
            except ValueError:
                # No frontmatter — derive name/description from filename and first heading
                first_line = next(
                    (l.lstrip("# ").strip() for l in content.splitlines() if l.strip()),
                    skill_name,
                )
                skill_dict = {
                    "name": skill_name,
                    "description": first_line,
                    "content": content.strip(),
                    "source_path": skill_path,
                    "allowed_tools": [],
                    "tags": [],
                }
                logger.info("Loaded skill (no frontmatter): %s", skill_name)
            else:
                logger.info("Loaded skill: %s", skill_name)
            skills[skill_name] = skill_dict
        except FileNotFoundError:
            logger.debug("Skill file missing: %s, skipping", skill_name)
        except Exception as exc:
            logger.warning("Failed to parse skill %s: %s", skill_name, exc)

    return skills


def _get_skills() -> Dict[str, Any]:
    global _skills_cache
    if _skills_cache is None:
        _skills_cache = _load_all_skills()
    return _skills_cache


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

class _BearerAuthMiddleware(BaseHTTPMiddleware):
    """Reject requests that don't carry the expected Bearer token."""

    def __init__(self, app, token: str) -> None:
        super().__init__(app)
        self._token = token

    async def dispatch(self, request: Request, call_next):
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {self._token}":
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        return await call_next(request)


def create_server(host: str = "127.0.0.1", port: int = 2034) -> FastMCP:
    mcp = FastMCP(
        name="salesforce-cms-skills",
        instructions=(
            "Exposes OpenViking agent skills stored in Salesforce CMS. "
            "Use 'list_skills' to browse available skills, 'get_skill' to read a specific skill, "
            "and 'reload_skills' to refresh the cache from Salesforce."
        ),
        host=host,
        port=port,
        stateless_http=True,
        json_response=True,
    )

    if _bearer_token:
        mcp._middleware = [(_BearerAuthMiddleware, {"token": _bearer_token})]
        logger.info("Bearer token auth enabled")

    @mcp.tool()
    async def list_skills() -> str:
        """
        List all skills loaded from the Salesforce CMS agent/skills folder.

        Returns a summary of each skill (name, description, tags).
        """
        def _list():
            skills = _get_skills()
            if not skills:
                return "No skills found in Salesforce CMS."
            lines = [f"Found {len(skills)} skill(s):\n"]
            for name, s in sorted(skills.items()):
                tags = ", ".join(s.get("tags", [])) or "—"
                lines.append(f"• **{name}**\n  {s['description']}\n  tags: {tags}\n")
            return "\n".join(lines)

        return await asyncio.to_thread(_list)

    @mcp.tool()
    async def get_skill(name: str) -> str:
        """
        Return the parsed content of a specific skill from Salesforce CMS.

        Args:
            name: Skill folder name (as returned by list_skills).
        """
        def _get():
            skills = _get_skills()
            skill = skills.get(name)
            if skill is None:
                available = ", ".join(sorted(skills)) or "none"
                return f"Skill '{name}' not found. Available: {available}"
            return SkillLoader.to_skill_md(skill)

        return await asyncio.to_thread(_get)

    @mcp.tool()
    async def get_skill_raw(name: str) -> str:
        """
        Return the raw markdown body of a skill (without frontmatter).

        Useful for displaying skill instructions or debugging.

        Args:
            name: Skill folder name.
        """
        def _raw():
            skills = _get_skills()
            skill = skills.get(name)
            if skill is None:
                available = ", ".join(sorted(skills)) or "none"
                return f"Skill '{name}' not found. Available: {available}"
            return skill.get("content", "")

        return await asyncio.to_thread(_raw)

    @mcp.tool()
    async def reload_skills() -> str:
        """
        Clear the in-memory skill cache and re-fetch all skills from Salesforce CMS.

        Use this after uploading new or updated skills to CMS.
        """
        def _reload():
            global _skills_cache
            _skills_cache = None
            skills = _get_skills()
            return f"Reloaded {len(skills)} skill(s) from Salesforce CMS."

        return await asyncio.to_thread(_reload)

    @mcp.resource("salesforce-cms://skills")
    def skills_resource() -> str:
        """JSON summary of all skills in the CMS agent/skills folder."""
        skills = _get_skills()
        summaries: List[Dict[str, Any]] = [
            {
                "name": s["name"],
                "description": s["description"],
                "tags": s.get("tags", []),
                "allowed_tools": s.get("allowed_tools", []),
                "source_path": s.get("source_path", ""),
            }
            for s in skills.values()
        ]
        return json.dumps(summaries, indent=2, ensure_ascii=False)

    return mcp


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_ON_HEROKU = bool(os.getenv("DYNO"))  # Heroku sets DYNO on every dyno


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Salesforce CMS Skills MCP Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run server.py
  uv run server.py --config ~/.openviking/ov.conf --port 2034
  uv run server.py --transport stdio

Environment variables (all platforms):
  OV_CONFIG        Path to ov.conf file (default: ~/.openviking/ov.conf)
  OV_PORT          Server port (default: 2034; overridden by PORT on Heroku)
  OV_DEBUG         Enable debug logging (set to 1)

Heroku config vars (credential injection without a file):
  OV_CONFIG_JSON   Full ov.conf JSON as a single string  [recommended]
  SF_INSTANCE_URL  Salesforce org URL
  SF_CLIENT_ID     Connected App consumer key
  SF_CLIENT_SECRET Connected App consumer secret
  SF_SPACE_ID      CMS Workspace ID
  SF_API_VERSION   API version (default: v66.0)
  SF_BU_CONTEXT    Business Unit context (default: default)
  SF_FOLDER_MAP    JSON object mapping path substrings to folder IDs
        """,
    )
    parser.add_argument(
        "--config",
        default=os.getenv("OV_CONFIG", str(Path("~/.openviking/ov.conf").expanduser())),
        help="Path to ov.conf (default: ~/.openviking/ov.conf)",
    )
    # Heroku injects PORT; fall back to OV_PORT, then 2034
    _default_port = int(os.getenv("PORT") or os.getenv("OV_PORT", "2034"))
    # On Heroku we must bind 0.0.0.0; locally default to loopback
    _default_host = "0.0.0.0" if _ON_HEROKU else "127.0.0.1"
    parser.add_argument("--host", default=_default_host, help="Bind host")
    parser.add_argument("--port", type=int, default=_default_port, help="Listen port")
    parser.add_argument(
        "--transport",
        choices=["streamable-http", "stdio"],
        default="streamable-http",
        help="MCP transport (default: streamable-http)",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("MCP_BEARER_TOKEN", ""),
        help="Require this Bearer token on all requests (default: disabled)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    global _config_path, _bearer_token
    _config_path = args.config
    _bearer_token = args.token

    if os.getenv("OV_DEBUG") == "1":
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Salesforce CMS Skills MCP Server starting")
    logger.info("  config:    %s", _config_path)
    logger.info("  transport: %s", args.transport)

    mcp = create_server(host=args.host, port=args.port)

    if args.transport == "streamable-http":
        logger.info("  endpoint:  http://%s:%d/mcp", args.host, args.port)
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
