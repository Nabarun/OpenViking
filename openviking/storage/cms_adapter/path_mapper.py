# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Maps AGFS internal paths to Salesforce CMS identifiers.

AGFS paths look like: /local/{account_id}/{scope}/...
viking:// URIs are converted to these by VikingFS before reaching us.

Mapping rules:
  /local/acc/agent/bot/skills/triage/       → url_name: local-acc-agent-bot-skills-triage
  /local/acc/agent/bot/skills/triage/SKILL.md → url_name: local-acc-agent-bot-skills-triage-skill-md
"""

import re


_SANITIZE_RE = re.compile(r"[^a-z0-9\-]")
_MULTI_DASH_RE = re.compile(r"-{2,}")

# Files that map to named fields on the parent directory's CMS content item
# instead of being stored as individual content items.
_FIELD_MAP = {
    ".abstract.md": "abstract",
    ".overview.md": "overview",
}

# The set of content filenames that are stored as the ``body`` field.
_BODY_FILENAMES = {"content.md", "SKILL.md", "profile.md"}


def path_to_url_name(path: str) -> str:
    """Convert an AGFS path to a CMS urlName (max 255 chars, alphanumeric + dash).

    Example::
        /local/acc/agent/bot/skills/triage/ → local-acc-agent-bot-skills-triage
    """
    cleaned = path.strip("/").replace("/", "-").replace(".", "-").lower()
    cleaned = _SANITIZE_RE.sub("-", cleaned)
    cleaned = _MULTI_DASH_RE.sub("-", cleaned).strip("-")
    return cleaned[:255] if cleaned else "root"


def path_to_title(path: str) -> str:
    """Return the last path segment as the human-readable title."""
    return path.rstrip("/").rsplit("/", 1)[-1] or "root"


def parent_path(path: str) -> str:
    """Return the parent directory path."""
    stripped = path.rstrip("/")
    if "/" not in stripped:
        return "/"
    return stripped.rsplit("/", 1)[0] + "/"


def is_dir_path(path: str) -> bool:
    """Return True if path represents a directory (ends with /)."""
    return path.endswith("/")


def metadata_field(path: str):
    """If *path* is a special metadata file (.abstract.md / .overview.md),
    return ``(field_name, parent_path)``; otherwise return ``(None, None)``.
    """
    filename = path_to_title(path)
    if filename in _FIELD_MAP:
        return _FIELD_MAP[filename], parent_path(path)
    return None, None


def is_body_file(path: str) -> bool:
    """Return True if *path* is a content body file (SKILL.md, content.md, etc.)."""
    return path_to_title(path) in _BODY_FILENAMES
