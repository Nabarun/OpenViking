# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Synchronous HTTP client for the Salesforce CMS Connect API.

Authentication uses the OAuth 2.0 client_credentials flow (Connected App).
Tokens are cached in-process and refreshed proactively before expiry.

CMS API endpoints used:
  POST   /connect/cms/spaces/{spaceId}/contents          create base content
  POST   /connect/cms/contents/variants                  create BU variant
  GET    /connect/cms/contents/{managedContentId}        get content (with ?language=)
  GET    /connect/cms/spaces/{spaceId}/contents          list / search by urlName
  PATCH  /connect/cms/contents/{managedContentId}        update base content
  DELETE /connect/cms/contents/{managedContentId}        delete content
"""

import time
from typing import Any, Dict, List, Optional

import requests

from openviking_cli.utils.logger import get_logger

from .config import SalesforceCMSConfig

logger = get_logger(__name__)


class CMSAuthError(Exception):
    """Raised when OAuth token acquisition fails."""


class CMSNotFoundError(Exception):
    """Raised when a CMS content item is not found."""


class CMSClient:
    """Synchronous Salesforce CMS Connect API client."""

    def __init__(self, config: SalesforceCMSConfig) -> None:
        self._cfg = config
        self._session = requests.Session()
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _ensure_token(self) -> str:
        """Return a valid Bearer token, refreshing if needed."""
        if self._token and time.time() < self._token_expires_at:
            return self._token

        resp = self._session.post(
            self._cfg.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._cfg.client_id,
                "client_secret": self._cfg.client_secret,
            },
            timeout=self._cfg.timeout,
        )
        if not resp.ok:
            raise CMSAuthError(
                f"OAuth token request failed [{resp.status_code}]: {resp.text[:300]}"
            )
        payload = resp.json()
        self._token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 7200))
        self._token_expires_at = time.time() + expires_in - self._cfg.token_cache_buffer_secs
        logger.debug("[CMSClient] OAuth token refreshed, expires in %ds", expires_in)
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._ensure_token()}",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        return f"{self._cfg.cms_base_url}/{path.lstrip('/')}"

    def _raise_for_status(self, resp: requests.Response, context: str) -> None:
        if resp.status_code == 404:
            raise CMSNotFoundError(context)
        if not resp.ok:
            raise requests.HTTPError(
                f"CMS API error [{resp.status_code}] {context}: {resp.text[:400]}",
                response=resp,
            )

    # ------------------------------------------------------------------
    # Content CRUD
    # ------------------------------------------------------------------

    def create_content(
        self,
        title: str,
        url_name: str,
        content_body: Dict[str, Any],
        folder_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new managed content item in the configured CMS space.

        Uses ``POST /connect/cms/contents`` with ``contentSpaceOrFolderId`` to
        target the workspace.  Pass *folder_id* to place the item inside a
        specific CMS folder (9Pu... ID); defaults to the configured space.

        Returns the full content record including ``managedContentId``.
        """
        payload = {
            "contentSpaceOrFolderId": folder_id or self._cfg.space_id,
            "contentType": self._cfg.content_type,
            "title": title,
            "urlName": url_name,
            "contentBody": content_body,
        }
        resp = self._session.post(
            self._url("contents"),
            json=payload,
            headers=self._headers(),
            timeout=self._cfg.timeout,
        )
        self._raise_for_status(resp, f"create_content urlName={url_name}")
        result = resp.json()
        logger.debug("[CMSClient] Created content id=%s urlName=%s", result.get("managedContentId"), url_name)
        return result

    def create_variant(
        self,
        managed_content_id: str,
        language: str,
        title: str,
        url_name: str,
        content_body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create a BU-specific variant of an existing content item.

        Uses the confirmed endpoint POST /connect/cms/contents/variants with
        ``language`` as the BU identifier (e.g. ``health_cloud``, ``ecommerce``).
        """
        payload = {
            "managedContentKeyorId": managed_content_id,
            "language": language,
            "title": title,
            "urlName": url_name,
            "contentBody": content_body,
        }
        resp = self._session.post(
            self._url("contents/variants"),
            json=payload,
            headers=self._headers(),
            timeout=self._cfg.timeout,
        )
        self._raise_for_status(resp, f"create_variant id={managed_content_id} lang={language}")
        result = resp.json()
        logger.debug("[CMSClient] Created variant lang=%s for content id=%s", language, managed_content_id)
        return result

    def get_content(
        self,
        managed_content_id: str,
        language: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch a content item by ID.

        If *language* is set, the response will include that variant's
        ``contentBody`` fields; otherwise the default/base variant is returned.

        Raises ``CMSNotFoundError`` on 404.
        """
        params: Dict[str, str] = {}
        if language and language != "default":
            params["language"] = language

        resp = self._session.get(
            self._url(f"contents/{managed_content_id}"),
            params=params,
            headers=self._headers(),
            timeout=self._cfg.timeout,
        )
        if resp.status_code == 404:
            # Try fallback to default variant
            if language and language != "default":
                logger.debug(
                    "[CMSClient] Variant lang=%s not found for %s, falling back to default",
                    language,
                    managed_content_id,
                )
                return self.get_content(managed_content_id, language=None)
            raise CMSNotFoundError(f"content id={managed_content_id}")
        self._raise_for_status(resp, f"get_content id={managed_content_id}")
        return resp.json()

    def find_by_url_name(self, url_name: str) -> Optional[Dict[str, Any]]:
        """Search for a content item by its urlName via SOQL on ManagedContentVariant.

        ``ManagedContentVariant.UrlName`` stores the urlName we supply at
        creation time.  ``ManagedContent.ApiName`` is an auto-generated key
        (``MC...``) and must NOT be used for this lookup.

        Returns the full content record or ``None`` if not found.
        """
        soql = (
            f"SELECT UrlName, ManagedContentKey FROM ManagedContentVariant "
            f"WHERE UrlName = '{url_name}' "
            f"AND ManagedContent.AuthoredManagedContentSpaceId = '{self._cfg.space_id}' "
            f"LIMIT 1"
        )
        query_url = (
            f"{self._cfg.instance_url.rstrip('/')}/services/data/{self._cfg.api_version}/query"
        )
        resp = self._session.get(
            query_url,
            params={"q": soql},
            headers=self._headers(),
            timeout=self._cfg.timeout,
        )
        if resp.status_code == 404:
            return None
        self._raise_for_status(resp, f"find_by_url_name urlName={url_name}")
        data = resp.json()
        records = data.get("records") or []
        if not records:
            return None

        content_key = records[0].get("ManagedContentKey")
        if not content_key:
            return None

        try:
            return self.get_content(content_key)
        except CMSNotFoundError:
            return None

    def update_content(
        self,
        managed_content_id: str,
        content_body: Dict[str, Any],
        language: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Patch content body fields on an existing item or variant."""
        payload: Dict[str, Any] = {"contentBody": content_body}
        if language and language != "default":
            payload["language"] = language

        resp = self._session.patch(
            self._url(f"contents/{managed_content_id}"),
            json=payload,
            headers=self._headers(),
            timeout=self._cfg.timeout,
        )
        self._raise_for_status(resp, f"update_content id={managed_content_id}")
        return resp.json()

    def list_contents(self, page_size: int = 100) -> List[Dict[str, Any]]:
        """Return all content items in the configured space via SOQL + per-item fetch.

        Queries ManagedContent for all items in the space, then fetches each
        full record via the CMS Connect API so that contentBody fields are
        included.
        """
        soql = (
            f"SELECT Id, ContentKey, ApiName FROM ManagedContent "
            f"WHERE AuthoredManagedContentSpaceId = '{self._cfg.space_id}' "
            f"ORDER BY ApiName LIMIT {page_size}"
        )
        query_url = (
            f"{self._cfg.instance_url.rstrip('/')}/services/data/{self._cfg.api_version}/query"
        )
        results: List[Dict[str, Any]] = []
        next_url: Optional[str] = None

        while True:
            if next_url:
                resp = self._session.get(
                    f"{self._cfg.instance_url.rstrip('/')}{next_url}",
                    headers=self._headers(),
                    timeout=self._cfg.timeout,
                )
            else:
                resp = self._session.get(
                    query_url,
                    params={"q": soql},
                    headers=self._headers(),
                    timeout=self._cfg.timeout,
                )
            self._raise_for_status(resp, "list_contents")
            data = resp.json()
            records = data.get("records") or []

            for rec in records:
                content_key = rec.get("ContentKey")
                if not content_key:
                    continue
                try:
                    full = self.get_content(content_key)
                    results.append(full)
                except CMSNotFoundError:
                    pass

            if data.get("done", True):
                break
            next_url = data.get("nextRecordsUrl")
            if not next_url:
                break

        return results

    def delete_content(self, managed_content_id: str) -> Dict[str, Any]:
        """Delete a content item (and all its variants) by ID."""
        resp = self._session.delete(
            self._url(f"contents/{managed_content_id}"),
            headers=self._headers(),
            timeout=self._cfg.timeout,
        )
        if resp.status_code == 404:
            return {"message": "deleted"}
        self._raise_for_status(resp, f"delete_content id={managed_content_id}")
        return {"message": "deleted"}
