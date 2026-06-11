"""
Configuration loader for multi-site SharePoint crawling.
All configuration is loaded from environment variables (e.g. from a .env file).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class SiteConfig:
    """Configuration for a single SharePoint site."""
    key: str
    name: str
    url: str
    group_key: str  # Parent group key for nested output
    page_paths: list[str] | None = None  # Optional filter for specific pages


@dataclass
class SiteGroup:
    """A named group of SharePoint sites."""
    key: str
    name: str
    kb_name: str = ""  # Knowledge Base name in Open WebUI
    sites: list[SiteConfig] = field(default_factory=list)


@dataclass
class SharePointConfig:
    """Full configuration for multi-site SharePoint crawling."""
    tenant_id: str
    client_id: str
    client_secret: str
    output_dir: Path
    file_extensions: set[str]
    site_groups: dict[str, SiteGroup] = field(default_factory=dict)

    def get_all_sites(self) -> list[SiteConfig]:
        """Return a flat list of all sites across all groups."""
        sites = []
        for group in self.site_groups.values():
            sites.extend(group.sites)
        return sites

    def get_sites_by_groups(self, group_keys: list[str]) -> list[SiteConfig]:
        """Return sites belonging to the specified groups."""
        if not group_keys or "all" in [k.lower() for k in group_keys]:
            return self.get_all_sites()

        sites = []
        for key in group_keys:
            key_lower = key.lower()
            if key_lower in self.site_groups:
                sites.extend(self.site_groups[key_lower].sites)
            else:
                logger.warning("Unknown site group: %s", key)
        return sites


def load_config() -> SharePointConfig:
    """Load SharePoint configuration entirely from environment variables.

    Required variables:
      - SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET
      - SITES  (comma-separated group keys)
      - SITE_<KEY>_URLS  (comma-separated URLs for each group)

    Optional per-group variables:
      - SITE_<KEY>_NAME        (display name, defaults to key)
      - SITE_<KEY>_KB_NAME     (Open WebUI knowledge base name)
      - SITE_<KEY>_PAGE_PATHS  (comma-separated server-relative page paths to crawl;
                                leave unset to crawl all pages)

    When a group has multiple URLs (SITE_<KEY>_URLS=url1,url2), you can supply
    per-URL page-path filters using a 0-based index suffix:
      - SITE_<KEY>_0_PAGE_PATHS  (page paths for the first URL)
      - SITE_<KEY>_1_PAGE_PATHS  (page paths for the second URL)
    The indexed variable takes precedence over the group-level one.

    Optional global variables:
      - OUTPUT_DIR         (default: "output")
      - FILE_EXTENSIONS    (comma-separated, default: ".pdf,.docx,.pptx,.xlsx")
    """
    # Load credentials
    tenant_id = os.environ.get("SHAREPOINT_TENANT_ID", "")
    client_id = os.environ.get("SHAREPOINT_CLIENT_ID", "")
    client_secret = os.environ.get("SHAREPOINT_CLIENT_SECRET", "")

    missing = []
    if not tenant_id:
        missing.append("SHAREPOINT_TENANT_ID")
    if not client_id:
        missing.append("SHAREPOINT_CLIENT_ID")
    if not client_secret:
        missing.append("SHAREPOINT_CLIENT_SECRET")
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    # Global options
    output_dir = Path(os.environ.get("OUTPUT_DIR", "output"))

    raw_extensions = os.environ.get("FILE_EXTENSIONS", ".pdf,.docx,.pptx,.xlsx")
    file_extensions = {
        ext.strip().lower() if ext.strip().startswith(".") else f".{ext.strip().lower()}"
        for ext in raw_extensions.split(",")
        if ext.strip()
    }

    # Parse site groups from env vars
    sites_raw = os.environ.get("SITES", "").strip()
    if not sites_raw:
        raise ValueError(
            "Missing required environment variable: SITES "
            "(comma-separated list of site group keys, e.g. SITES=group1,group2)"
        )

    group_keys = [k.strip() for k in sites_raw.split(",") if k.strip()]
    site_groups: dict[str, SiteGroup] = {}

    for group_key in group_keys:
        group_key_lower = group_key.lower()
        prefix = f"SITE_{group_key.upper()}"

        name = os.environ.get(f"{prefix}_NAME", group_key)
        kb_name = os.environ.get(f"{prefix}_KB_NAME", "")
        urls_raw = os.environ.get(f"{prefix}_URLS", "").strip()

        if not urls_raw:
            logger.warning("No URLs configured for site group '%s' (%s_URLS). Skipping.", group_key, prefix)
            continue

        url_list = [u.strip() for u in urls_raw.split(",") if u.strip()]

        # Group-level page paths (fallback when no per-URL override exists)
        group_page_paths_raw = os.environ.get(f"{prefix}_PAGE_PATHS", "").strip()
        group_page_paths: list[str] | None = (
            [p.strip() for p in group_page_paths_raw.split(",") if p.strip()]
            if group_page_paths_raw else None
        )

        sites = []
        for i, url in enumerate(url_list):
            site_key = f"{group_key_lower}_{i}" if len(url_list) > 1 else group_key_lower
            site_name = url.rstrip("/").split("/")[-1]

            # Per-URL page paths (indexed variable takes precedence)
            per_url_raw = os.environ.get(f"{prefix}_{i}_PAGE_PATHS", "").strip()
            if per_url_raw:
                page_paths: list[str] | None = [
                    p.strip() for p in per_url_raw.split(",") if p.strip()
                ]
            else:
                page_paths = group_page_paths

            sites.append(SiteConfig(
                key=site_key,
                name=site_name,
                url=url,
                group_key=group_key_lower,
                page_paths=page_paths,
            ))

        site_groups[group_key_lower] = SiteGroup(
            key=group_key_lower, name=name, kb_name=kb_name, sites=sites
        )

    if not site_groups:
        raise ValueError(
            "No sites configured. Set SITES and corresponding SITE_<KEY>_URLS variables."
        )

    total_sites = sum(len(g.sites) for g in site_groups.values())
    logger.info("Loaded %d site group(s) with %d total site(s).", len(site_groups), total_sites)

    return SharePointConfig(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        output_dir=output_dir,
        file_extensions=file_extensions,
        site_groups=site_groups,
    )


def list_available_sites(config: SharePointConfig) -> None:
    """Print a summary of all configured site groups and sites."""
    print("\nConfigured SharePoint Sites:")
    print("-" * 50)
    for group in config.site_groups.values():
        print(f"\n  {group.key}: {group.name}")
        for site in group.sites:
            print(f"    - {site.name}: {site.url}")
            if site.page_paths:
                print(f"      Page filter: {len(site.page_paths)} path(s)")
    print()

