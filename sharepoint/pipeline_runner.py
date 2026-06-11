"""
Pipeline orchestration entry point.

Runs all three phases in sequence:
  1. SharePoint crawl  → writes .md + .json pairs to OUTPUT_DIR/<group>/<site>/
  2. Open WebUI import → uploads files to Knowledge Bases, deduplicates/updates
  3. Cleanup           → removes all crawl output, resets OUTPUT_DIR mount point

Exits non-zero if the crawl or import phase fails entirely, so OpenShift can
detect and report failures. Individual file errors are logged but do not abort
the run.

Usage:
    python pipeline_runner.py [--sites <group> ...] [--config <path>]

Environment variables (all loaded from .env or injected from the OpenShift Secret):
    SHAREPOINT_TENANT_ID      - Azure AD tenant
    SHAREPOINT_CLIENT_ID      - Azure AD app client ID
    SHAREPOINT_CLIENT_SECRET  - Azure AD app client secret
    OPEN_WEB_UI_BASE_URL      - Open WebUI base URL
    OPEN_WEB_UI_API_KEY       - Open WebUI API key
    SITES                     - Comma-separated site group keys
    SITE_<KEY>_NAME           - Display name for a site group
    SITE_<KEY>_KB_NAME        - Open WebUI Knowledge Base name for a site group
    SITE_<KEY>_URLS           - Comma-separated SharePoint URLs for a site group

Optional:
    OUTPUT_DIR                - Base path for crawl output (default: /data/output)
    FILE_EXTENSIONS           - Comma-separated extensions (default: .pdf,.docx,.pptx,.xlsx)
    PIPELINE_SITES            - Comma-separated site group filter (default: all)
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

from config_loader import load_config, SiteConfig
from sharepoint_client import SharePointClient
from sharepoint_crawler import SharePointCrawler
from document_converter import MarkItDownDocumentConverter
from owui_importer import OpenWebUIImporter

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def crawl_site(
    site: SiteConfig,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    output_dir: Path,
    file_extensions: set[str],
) -> dict[str, int]:
    """Crawl a single SharePoint site using the MarkItDown converter."""
    logger.info("=" * 60)
    logger.info("Crawling site: %s (%s)", site.name, site.url)
    if site.page_paths:
        logger.info("Page filter: %d path(s)", len(site.page_paths))
    logger.info("=" * 60)

    sp_client = SharePointClient(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        site_url=site.url,
    )

    # Each site gets its own subdirectory: OUTPUT_DIR/<group>/<site>
    site_output_dir = output_dir / site.group_key / site.name
    crawler = SharePointCrawler(
        client=sp_client,
        output_dir=site_output_dir,
        file_extensions=file_extensions,
        page_paths=site.page_paths,
        converter=MarkItDownDocumentConverter(),
    )
    return crawler.crawl_all()


def get_or_create_kb(importer: OpenWebUIImporter, kb_name: str) -> str:
    """Return the ID of an existing KB or create a new one."""
    kb_id = importer.get_knowledge_base_by_name(kb_name)
    if kb_id:
        logger.info("Found existing Knowledge Base '%s' (id=%s)", kb_name, kb_id)
        return kb_id
    logger.info("Knowledge Base '%s' not found. Creating...", kb_name)
    return importer.create_knowledge_base(kb_name, f"SharePoint content — {kb_name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="SharePoint → Open WebUI pipeline runner."
    )
    parser.add_argument(
        "--sites",
        nargs="+",
        default=[],
        help="Site groups to crawl. Omit for all.",
    )
    args = parser.parse_args()

    # Env var override for sites filter (comma-separated)
    pipeline_sites_env = os.getenv("PIPELINE_SITES", "").strip()
    if pipeline_sites_env and not args.sites:
        args.sites = [s.strip() for s in pipeline_sites_env.split(",") if s.strip()]

    output_dir = Path(os.getenv("OUTPUT_DIR", "/data/output"))
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load config
    try:
        config = load_config()
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)

    sites = config.get_sites_by_groups(args.sites)
    if not sites:
        logger.error("No sites to crawl. Check --sites argument or config file.")
        sys.exit(1)

    logger.info("Pipeline starting. Will crawl %d site(s) into %s.", len(sites), output_dir)

    # ── Phase 1: Crawl ──────────────────────────────────────────────────────
    logger.info("\n%s", "=" * 60)
    logger.info("Phase 1: SharePoint Crawl")
    logger.info("=" * 60)

    crawl_errors: list[str] = []
    crawled_sites: list[str] = []
    for site in sites:
        try:
            results = crawl_site(
                site=site,
                tenant_id=config.tenant_id,
                client_id=config.client_id,
                client_secret=config.client_secret,
                output_dir=output_dir,
                file_extensions=config.file_extensions,
            )
            crawled_sites.append(site.group_key)
            logger.info(
                "Site '%s': crawled %d page(s), %d file(s).",
                site.name,
                results.get("pages", 0),
                results.get("files", 0),
            )
        except Exception:
            logger.exception("Failed to crawl site: %s", site.url)
            crawl_errors.append(site.url)

    if crawl_errors:
        logger.error(
            "Phase 1 failed: %d site(s) could not be crawled: %s",
            len(crawl_errors),
            ", ".join(crawl_errors),
        )
        sys.exit(1)

    logger.info("Phase 1 complete.")

    # ── Phase 2: Import ─────────────────────────────────────────────────────
    logger.info("\n%s", "=" * 60)
    logger.info("Phase 2: Open WebUI Import")
    logger.info("=" * 60)

    base_url = os.getenv("OPEN_WEB_UI_BASE_URL", "").strip()
    api_key = os.getenv("OPEN_WEB_UI_API_KEY", "").strip()
    if not base_url or not api_key:
        logger.error(
            "Missing required env vars: OPEN_WEB_UI_BASE_URL and OPEN_WEB_UI_API_KEY must be set."
        )
        sys.exit(1)

    importer = OpenWebUIImporter(base_url=base_url, api_key=api_key)
    import_errors: list[str] = []

    # Only process groups that produced output in this run
    for group_key, group in config.site_groups.items():
        if group_key not in crawled_sites:
            logger.info("Group '%s' did not produce any output, skipping import.", group_key)
            continue

        group_dir = output_dir / group_key
        if not group_dir.exists():
            logger.info("No output directory for group '%s', skipping import.", group_key)
            continue

        if not group.kb_name:
            logger.error(
                "Group '%s' has no kb_name in sites_config.yaml. Skipping import for this group.",
                group_key,
            )
            import_errors.append(group_key)
            continue

        try:
            kb_id = get_or_create_kb(importer, group.kb_name)
            importer.process_directory(str(group_dir), kb_id)
        except Exception:
            logger.exception("Import failed for group '%s'.", group_key)
            import_errors.append(group_key)

    if import_errors:
        logger.error(
            "Phase 2 failed: %d group(s) could not be imported: %s",
            len(import_errors),
            ", ".join(import_errors),
        )
        sys.exit(1)

    logger.info("Phase 2 complete.")

    # ── Phase 3: Cleanup ────────────────────────────────────────────────────
    logger.info("\n%s", "=" * 60)
    logger.info("Phase 3: Cleanup")
    logger.info("=" * 60)

    try:
        resolved_output_dir = output_dir.resolve()
        if resolved_output_dir in [Path("/"), Path("/data"), Path("/app")]:
            raise ValueError("Refusing to delete unsafe directory: %s", resolved_output_dir)
        
        for child in output_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
                
        logger.info("Cleanup complete. Output directory reset at %s.", output_dir)
    except Exception:
        # Non-fatal: PVC content is overwritten on the next run anyway.
        logger.warning("Cleanup failed; PVC will be overwritten on next run.", exc_info=True)

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
