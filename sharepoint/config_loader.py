"""
Configuration loader for multi-site SharePoint crawling.
Loads site definitions from YAML, credentials from environment variables.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILE = Path(__file__).parent / "sites_config.yaml"


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


def load_config(config_path: Path | str | None = None) -> SharePointConfig:
    """Load SharePoint configuration from YAML file and environment variables.
    
    Credentials are loaded exclusively from environment variables:
      - SHAREPOINT_TENANT_ID
      - SHAREPOINT_CLIENT_ID
      - SHAREPOINT_CLIENT_SECRET
    
    Site definitions are loaded from the YAML config file.
    """
    if config_path is None:
        config_path = DEFAULT_CONFIG_FILE
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    logger.info("Loading configuration from: %s", config_path)

    # Load credentials from environment
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

    # Load site config from YAML
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    output_dir = Path(config.get("output_dir", "output"))

    raw_extensions = config.get("file_extensions", [".pdf", ".docx", ".pptx", ".xlsx"])
    file_extensions = {
        ext.lower() if ext.startswith(".") else f".{ext.lower()}"
        for ext in raw_extensions
    }

    # Parse site groups
    site_groups: dict[str, SiteGroup] = {}
    sites_config = config.get("sites", {})

    for group_key, group_data in sites_config.items():
        group_key_lower = group_key.lower()
        group_name = group_data.get("name", group_key)
        urls = group_data.get("urls", [])

        sites = []
        for i, url_entry in enumerate(urls):
            # Support both string URLs and dict with url + page_paths
            if isinstance(url_entry, str):
                url = url_entry
                page_paths = None
            else:
                url = url_entry.get("url", "")
                page_paths = url_entry.get("page_paths")
            
            if not url:
                logger.warning("Empty URL in group %s, skipping", group_key)
                continue
                
            site_key = f"{group_key_lower}_{i}" if len(urls) > 1 else group_key_lower
            site_name = url.rstrip("/").split("/")[-1]
            sites.append(SiteConfig(
                key=site_key,
                name=site_name,
                url=url,
                group_key=group_key_lower,
                page_paths=page_paths,
            ))

        kb_name = group_data.get("kb_name", "")
        site_groups[group_key_lower] = SiteGroup(
            key=group_key_lower, name=group_name, kb_name=kb_name, sites=sites
        )

    if not site_groups:
        raise ValueError("No sites configured in YAML. Add at least one site group.")

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

