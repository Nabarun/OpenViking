# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""AGFS-compatible filesystem adapter backed by Salesforce CMS.

Implements the 6-method interface VikingFS calls:  read · write · mkdir · stat · ls · rm

Each AGFS path maps to one CMS content item of type ``ov_node``.
Fields are stored natively in contentBody (no JSON envelope).

Write semantics
---------------
Salesforce CMS enhanced spaces do not support PATCH or DELETE via the Connect
REST API, so this adapter is **write-once**: a second write to the same path
is a no-op (first-write wins).  This is acceptable for skills and memories
which are written once at agent setup time.

Workspace initialisation
------------------------
Call ``initialize_workspace()`` once at startup to create the base folder
hierarchy under ``/viking/``::

    /viking/
    ├── agent/
    │   └── skills/
    ├── resources/
    ├── session/
    ├── temp/
    └── user/

Subsequent writes from agents nest under the appropriate subtree.
"""

import threading
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Tuple, Union

from openviking_cli.utils.logger import get_logger

from . import path_mapper as pm
from .cms_client import CMSClient, CMSNotFoundError

logger = get_logger(__name__)

# Native ov_node contentBody field names
_F_TITLE      = "title"
_F_BODY       = "body"
_F_ABSTRACT   = "abstract"
_F_OVERVIEW   = "overview"
_F_URI        = "viking_uri"
_F_NODE_TYPE  = "node_type"
_F_IS_DIR     = "is_directory"
_F_PARENT_URI = "parent_uri"

# .abstract.md / .overview.md → native field name
_META_FIELD = {".abstract.md": _F_ABSTRACT, ".overview.md": _F_OVERVIEW}

# Base folder hierarchy created at workspace init
_BASE_DIRS = [
    "/viking/",
    "/viking/agent/",
    "/viking/agent/skills/",
    "/viking/resources/",
    "/viking/session/",
    "/viking/temp/",
    "/viking/user/",
]


def _decode(data: Union[bytes, str]) -> str:
    if isinstance(data, (bytes, bytearray)):
        return data.decode("utf-8", errors="replace")
    return data


def _encode(text: str) -> bytes:
    return text.encode("utf-8")


def _node_type(path: str) -> str:
    parts = [p for p in path.strip("/").split("/") if p]
    for marker, label in (("skills", "skill"), ("memories", "memory"),
                          ("session", "session"), ("resources", "resource")):
        if marker in parts:
            return label
    return "file"


def _raw_field(content_body: Dict[str, Any], field: str) -> str:
    val = content_body.get(field, "")
    if isinstance(val, dict):
        val = val.get("value") or val.get("text") or ""
    return val or ""


class SalesforceCMSFS:
    """AGFS-compatible adapter backed by Salesforce CMS (ov_node content type).

    Parameters
    ----------
    client      : authenticated CMSClient
    space_id    : Salesforce CMS workspace ID
    bu_context  : Business Unit identifier (reserved for future variant support)
    """

    def __init__(self, client: CMSClient, space_id: str, bu_context: str = "default") -> None:
        self._client     = client
        self._space_id   = space_id
        self._bu_context = bu_context
        self._cache: Dict[str, str] = {}      # url_name → managedContentId
        self._lock = threading.Lock()
        # Sorted by key length descending so longest pattern wins on lookup
        self._folder_map: List[tuple] = sorted(
            (getattr(client._cfg, "folder_map", None) or {}).items(),
            key=lambda kv: len(kv[0]),
            reverse=True,
        )

    # ------------------------------------------------------------------
    # Public: workspace initialisation
    # ------------------------------------------------------------------

    def initialize_workspace(self) -> Dict[str, int]:
        """Idempotently create the base /viking/ folder hierarchy.

        Returns a dict ``{created: N, existed: M}`` summarising what happened.
        Safe to call on every startup — existing folders are detected and
        skipped without any API write.
        """
        created = existed = 0
        for path in _BASE_DIRS:
            result = self.mkdir(path)
            if result.get("message") == "created":
                created += 1
                logger.info("[CMSFS] init: created %s", path)
            else:
                existed += 1
                logger.debug("[CMSFS] init: exists  %s", path)
        return {"created": created, "existed": existed}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _url_name(self, path: str) -> str:
        return pm.path_to_url_name(path)

    def _get_content_id(self, path: str) -> Optional[str]:
        url_name = self._url_name(path)
        with self._lock:
            if url_name in self._cache:
                return self._cache[url_name]
        item = self._client.find_by_url_name(url_name)
        if item is None:
            return None
        cid = item.get("managedContentId") or item.get("id")
        if cid:
            with self._lock:
                self._cache[url_name] = cid
        return cid

    def _cache_set(self, path: str, cid: str) -> None:
        with self._lock:
            self._cache[self._url_name(path)] = cid

    def _cache_del(self, path: str) -> None:
        with self._lock:
            self._cache.pop(self._url_name(path), None)

    def _resolve_folder_id(self, path: str) -> str:
        """Return the CMS folder ID for *path* using longest-match on folder_map.

        Falls back to the configured space ID when no pattern matches.
        The path may be a AGFS internal path like
        ``/local/{account}/agent/{agent_id}/skills/{name}/SKILL.md``.
        """
        normalized = path.replace("\\", "/")
        for pattern, folder_id in self._folder_map:
            if pattern in normalized:
                logger.debug("[CMSFS] folder_map match '%s' → %s", pattern, folder_id)
                return folder_id
        return self._space_id

    def _body_of(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return record.get("contentBody") or {}

    def _create(self, path: str, is_dir: bool, extra: Optional[Dict[str, str]] = None) -> str:
        """Create a new ov_node content item.  Returns managedContentId."""
        title    = pm.path_to_title(path) or "root"
        url_name = self._url_name(path)
        logger.warning("[CMSFS] _create called: path=%s is_dir=%s url_name=%s", path, is_dir, url_name)
        body: Dict[str, Any] = {
            _F_TITLE:      title,
            _F_BODY:       "",
            _F_ABSTRACT:   "",
            _F_OVERVIEW:   "",
            _F_URI:        path,
            _F_NODE_TYPE:  "dir" if is_dir else _node_type(path),
            _F_IS_DIR:     "true" if is_dir else "false",
            _F_PARENT_URI: pm.parent_path(path),
        }
        if extra:
            body.update(extra)
        folder_id = self._resolve_folder_id(path)
        result = self._client.create_content(
            title=title, url_name=url_name, content_body=body, folder_id=folder_id
        )
        cid = result.get("managedContentId") or result.get("id") or ""
        if cid:
            self._cache_set(path, cid)
        return cid

    # ------------------------------------------------------------------
    # AGFS interface
    # ------------------------------------------------------------------

    def read(self, path: str, offset: int = 0, size: int = -1) -> bytes:
        filename = pm.path_to_title(path)
        field    = _META_FIELD.get(filename, _F_BODY)

        cid = self._get_content_id(path)
        if cid is None:
            raise FileNotFoundError(f"CMS content not found: {path}")
        try:
            record = self._client.get_content(cid)
        except CMSNotFoundError:
            raise FileNotFoundError(f"CMS content not found: {path}")

        text = _raw_field(self._body_of(record), field)
        raw  = _encode(text)
        if offset > 0 or size >= 0:
            raw = raw[offset: offset + size if size >= 0 else len(raw)]
        return raw

    def write(
        self,
        path: str,
        data: Union[bytes, Iterator[bytes], BinaryIO],
        max_retries: int = 3,
    ) -> str:
        """Write file content to CMS (write-once: skips if item already exists)."""
        if isinstance(data, (bytes, bytearray)):
            raw = bytes(data)
        elif hasattr(data, "read"):
            raw = data.read()
        else:
            raw = b"".join(data)
        text = _decode(raw)

        filename = pm.path_to_title(path)
        field    = _META_FIELD.get(filename, _F_BODY)

        if self._get_content_id(path) is not None:
            logger.warning("[CMSFS] write %s — exists, skipping (write-once)", path)
            return "OK"

        cid = self._create(path, is_dir=False, extra={field: text})
        logger.warning("[CMSFS] write %s → id=%s field=%s", path, cid, field)
        return "OK"

    def mkdir(self, path: str, mode: str = "755") -> Dict[str, Any]:
        """Create a directory node.  Idempotent."""
        dir_path = path if path.endswith("/") else path + "/"
        cid = self._get_content_id(dir_path)
        if cid:
            return {"message": "exists", "id": cid}
        try:
            cid = self._create(dir_path, is_dir=True)
            logger.debug("[CMSFS] mkdir %s → id=%s", dir_path, cid)
            return {"message": "created", "id": cid}
        except Exception as exc:
            if "already" in str(exc).lower() or "duplicate" in str(exc).lower():
                return {"message": "exists"}
            raise

    def stat(self, path: str) -> Dict[str, Any]:
        cid = self._get_content_id(path)
        if cid is None:
            raise FileNotFoundError(f"No such file or directory: {path}")
        try:
            record = self._client.get_content(cid)
        except CMSNotFoundError:
            raise FileNotFoundError(f"No such file or directory: {path}")

        cb     = self._body_of(record)
        is_dir = _raw_field(cb, _F_IS_DIR).lower() == "true"
        fname  = pm.path_to_title(path)
        field  = _META_FIELD.get(fname, _F_BODY)
        size   = len(_raw_field(cb, field).encode("utf-8"))

        return {
            "name":    fname or path,
            "size":    size,
            "isDir":   is_dir,
            "modTime": record.get("lastModifiedDate") or "",
            "mode":    "755" if is_dir else "644",
            "meta": {
                "managedContentId": cid,
                "contentType":      self._client._cfg.content_type,
                "bu_context":       self._bu_context,
            },
        }

    def ls(self, path: str) -> List[Dict[str, Any]]:
        dir_path  = path if path.endswith("/") else path + "/"
        all_items = self._client.list_contents()
        children  = []
        for item in all_items:
            cb = self._body_of(item)
            if _raw_field(cb, _F_PARENT_URI) != dir_path:
                continue
            is_dir = _raw_field(cb, _F_IS_DIR).lower() == "true"
            size   = len(_raw_field(cb, _F_BODY).encode("utf-8"))
            children.append({
                "name":    pm.path_to_title(_raw_field(cb, _F_URI) or ""),
                "size":    size,
                "isDir":   is_dir,
                "modTime": item.get("lastModifiedDate") or "",
            })
        return children

    def rm(self, path: str, recursive: bool = False, force: bool = True) -> Dict[str, Any]:
        """Delete.  Note: enhanced CMS spaces block DELETE; logs warning, returns OK."""
        if recursive:
            try:
                for child in self.ls(path):
                    cp = path.rstrip("/") + "/" + child["name"]
                    if child.get("isDir"):
                        cp += "/"
                    self.rm(cp, recursive=True, force=force)
            except Exception:
                pass
        cid = self._get_content_id(path)
        if cid is None:
            return {"message": "deleted"}
        result = self._client.delete_content(cid)
        self._cache_del(path)
        return result
