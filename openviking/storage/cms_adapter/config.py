# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Configuration for the Salesforce CMS adapter."""

from typing import Dict, Optional

from pydantic import BaseModel, Field


class SalesforceCMSConfig(BaseModel):
    """Configuration for Salesforce CMS as OpenViking storage backend.

    The ``bu_context`` field identifies which Business Unit variant to read/write.
    It maps directly to the ``language`` field in Salesforce CMS variant API.

    Example ov.conf::

        [storage.cms]
        instance_url = "https://myorg.my.salesforce.com"
        client_id = "..."
        client_secret = "..."
        space_id = "0ZuXXXXXXXXXXXXXXX"
        bu_context = "health_cloud"
    """

    instance_url: str = Field(description="Salesforce org URL, e.g. https://myorg.my.salesforce.com")
    client_id: str = Field(description="Connected App consumer key")
    client_secret: str = Field(description="Connected App consumer secret")
    space_id: str = Field(description="CMS Workspace (ManagedContentSpace) ID")
    api_version: str = Field(default="v66.0", description="Salesforce API version")
    bu_context: str = Field(
        default="default",
        description=(
            "Active Business Unit context. Used as the CMS variant 'language' key. "
            "Examples: health_cloud, ecommerce, financial, default"
        ),
    )
    content_type: str = Field(
        default="ov_node",
        description="CMS managed content type API name for OpenViking nodes.",
    )
    timeout: int = Field(default=30, description="HTTP request timeout in seconds")
    token_cache_buffer_secs: int = Field(
        default=60,
        description="Seconds before token expiry to proactively refresh",
    )
    folder_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Maps AGFS path substrings to CMS folder IDs (9Pu... IDs). "
            "Longest matching key wins. Falls back to space_id if no match. "
            "Example: {\"agent/skills\": \"9PuXXX\", \"memories/cases\": \"9PuYYY\"}"
        ),
    )

    model_config = {"extra": "forbid"}

    @property
    def cms_base_url(self) -> str:
        return f"{self.instance_url.rstrip('/')}/services/data/{self.api_version}/connect/cms"

    @property
    def token_url(self) -> str:
        return f"{self.instance_url.rstrip('/')}/services/oauth2/token"
