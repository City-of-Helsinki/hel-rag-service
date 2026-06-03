"""
SharePoint client using Microsoft Graph API with MSAL client-credentials (app-only) auth.
"""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

import msal
import requests

logger = logging.getLogger(__name__)


class SharePointClient:
    """Authenticates against Azure AD and fetches SharePoint page content via Graph API."""

    GRAPH_BASE = "https://graph.microsoft.com/v1.0"
    SCOPES = ["https://graph.microsoft.com/.default"]

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        site_url: str,
    ) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.site_url = site_url.rstrip("/")

        self._app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
        )
        self._token: str | None = None
        self._site_id: str | None = None

    def _get_token(self) -> str:
        """Acquire and cache an OAuth2 access token using client credentials flow.

        Returns the cached token on subsequent calls without re-authenticating.

        Raises:
            RuntimeError: If MSAL fails to acquire a token.
        """
        if self._token:
            return self._token

        result = self._app.acquire_token_for_client(scopes=self.SCOPES)
        if "access_token" not in result:
            error = result.get("error_description", result.get("error", "unknown"))
            raise RuntimeError(f"Failed to acquire access token: {error}")

        self._token = result["access_token"]
        logger.debug("Access token acquired.")
        return self._token

    def _headers(self) -> dict[str, str]:
        """Return HTTP headers required for authenticated Graph API requests."""
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

    def _get_site_id(self) -> str:
        """Resolve and cache the Graph API site ID from the configured site URL.

        Parses the hostname and path from ``self.site_url``, queries
        ``/sites/{hostname}:/{path}`` and caches the returned site ID so that
        subsequent calls skip the network round-trip.

        Returns:
            The opaque site ID string used in all subsequent Graph API calls.

        Raises:
            requests.HTTPError: If the site cannot be found or access is denied.
        """
        if self._site_id:
            return self._site_id

        parsed = urlparse(self.site_url)
        hostname = parsed.netloc  # e.g. organization.sharepoint.com
        # site path without leading slash, e.g. sites/mysite
        site_path = parsed.path.lstrip("/")

        url = f"{self.GRAPH_BASE}/sites/{hostname}:/{site_path}"
        response = requests.get(url, headers=self._headers(), timeout=60)
        response.raise_for_status()

        self._site_id = response.json()["id"]
        logger.debug("Resolved site ID: %s", self._site_id)
        return self._site_id

    def get_all_pages(self) -> list[dict]:
        """Return a list of all SitePages in the site with their metadata.

        Follows ``@odata.nextLink`` pagination so all pages are returned
        regardless of the collection size.

        Returns:
            A list of page metadata dicts as returned by the Graph API
            ``/sites/{site-id}/pages/microsoft.graph.sitePage`` endpoint.

        Raises:
            requests.HTTPError: If the Graph API request fails.
        """
        site_id = self._get_site_id()
        url = f"{self.GRAPH_BASE}/sites/{site_id}/pages/microsoft.graph.sitePage"
        pages: list[dict] = []

        while url:
            response = requests.get(url, headers=self._headers(), timeout=60)
            response.raise_for_status()
            data = response.json()
            pages.extend(data.get("value", []))
            url = data.get("@odata.nextLink")

        logger.info("Found %d page(s) in site.", len(pages))
        return pages

    def get_page_content_html(self, page_id: str) -> str | None:
        """Return the raw HTML content of a single SitePage.

        Fetches the page with its ``canvasLayout`` expanded, then walks every
        horizontal section column and the optional vertical section, collecting
        the ``innerHtml`` of each web part into a single HTML document.  The
        page title is prepended as an ``<h1>`` tag so converters downstream
        (e.g. :class:`~content_processor.HtmlToMarkdownConverter`) can produce
        a proper heading.

        Args:
            page_id: The Graph API page ID (GUID) to retrieve.

        Returns:
            A single HTML string assembled from all web part fragments, or
            ``None`` if the page has no ``canvasLayout`` content.

        Raises:
            requests.HTTPError: If the Graph API request fails.
        """
        site_id = self._get_site_id()
        url = (
            f"{self.GRAPH_BASE}/sites/{site_id}/pages/{page_id}"
            "/microsoft.graph.sitePage?$expand=canvasLayout"
        )
        response = requests.get(url, headers=self._headers(), timeout=60)
        response.raise_for_status()
        page = response.json()

        def collect_webpart_html(webparts: list) -> list[str]:
            # Return each web part's innerHtml fragment as-is
            return [wp["innerHtml"] for wp in webparts if wp.get("innerHtml")]

        html_parts: list[str] = []

        title = page.get("title")
        if title:
            html_parts.append(f"<h1>{title}</h1>")

        canvas = page.get("canvasLayout") or {}

        # Horizontal sections
        for section in canvas.get("horizontalSections", []):
            for column in section.get("columns", []):
                html_parts.extend(collect_webpart_html(column.get("webparts", [])))

        # Vertical section is a single object, not a list
        vertical = canvas.get("verticalSection") or {}
        html_parts.extend(collect_webpart_html(vertical.get("webparts", [])))

        if not html_parts:
            logger.warning("No canvasLayout content found for page %s (%s).", page_id, page.get("title", "?"))
            return None

        return "\n".join(html_parts)

    def get_pages_by_paths(self, page_paths: list[str]) -> list[dict]:
        """Filter all site pages to those whose URL or name matches a given list.

        Fetches all pages via :meth:`get_all_pages` and returns only those
        whose ``webUrl`` (relative to the site URL) or ``name`` matches one of
        the provided paths. Matching is case-insensitive.

        Args:
            page_paths: A list of relative URL paths or page names to match
                against, e.g. ``["/sites/mysite/SitePages/Home.aspx"]``.

        Returns:
            A filtered list of page metadata dicts.
        """
        normalized = {p.lower().strip() for p in page_paths}
        all_pages = self.get_all_pages()
        matched = [
            p
            for p in all_pages
            if (p.get("webUrl", "").lower().split(self.site_url.lower(), 1)[-1] in normalized)
            or (p.get("name", "").lower() in normalized)
        ]
        logger.info(
            "Matched %d / %d page(s) for the requested paths.",
            len(matched),
            len(all_pages),
        )
        return matched

    def get_site_drives(self) -> list[dict]:
        """Return all document libraries (drives) in the site.

        Queries ``/sites/{site-id}/drives`` and returns the full list of drive
        metadata dicts, each containing at minimum ``id`` and ``name``.

        Returns:
            A list of drive metadata dicts as returned by the Graph API.

        Raises:
            requests.HTTPError: If the Graph API request fails.
        """
        site_id = self._get_site_id()
        url = f"{self.GRAPH_BASE}/sites/{site_id}/drives"
        response = requests.get(url, headers=self._headers(), timeout=60)
        response.raise_for_status()
        drives = response.json().get("value", [])
        logger.info("Found %d drive(s) in site.", len(drives))
        return drives

    def get_drive_items(
        self,
        drive_id: str,
        folder_id: str = "root",
        extensions: set[str] | None = None,
    ) -> list[dict]:
        """Recursively list all files in a drive folder.

        Traverses the folder hierarchy starting from ``folder_id``, following
        ``@odata.nextLink`` pagination at each level. Sub-folders are visited
        depth-first. File items are optionally filtered by extension.

        Args:
            drive_id: The Graph API drive ID to query.
            folder_id: The item ID of the folder to start from. Defaults to
                ``"root"`` which is the root of the drive.
            extensions: An optional set of lowercase file extensions to include,
                e.g. ``{'.pdf', '.docx'}``. Pass ``None`` to return all files.

        Returns:
            A flat list of file item metadata dicts. Each dict includes at
            minimum ``id``, ``name``, ``webUrl``, and ``parentReference``.

        Raises:
            requests.HTTPError: If any Graph API request fails.
        """
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{folder_id}/children"
        items: list[dict] = []

        while url:
            response = requests.get(url, headers=self._headers(), timeout=60)
            response.raise_for_status()
            data = response.json()

            for item in data.get("value", []):
                if "folder" in item:
                    # Recurse into sub-folders
                    items.extend(
                        self.get_drive_items(drive_id, item["id"], extensions)
                    )
                elif "file" in item:
                    name: str = item.get("name", "")
                    if extensions is None or Path(name).suffix.lower() in extensions:
                        items.append(item)
                        logger.debug("Found file: %s", name)

            url = data.get("@odata.nextLink")

        return items

    def download_drive_item(self, drive_id: str, item_id: str) -> bytes:
        """Download a drive item's raw content as bytes.

        Follows redirects automatically (Graph API returns a pre-authenticated
        redirect URL for the actual file content).

        Args:
            drive_id: The Graph API drive ID that owns the item.
            item_id: The Graph API item ID of the file to download.

        Returns:
            The raw file content as a :class:`bytes` object.

        Raises:
            requests.HTTPError: If the download request fails.
        """
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"
        response = requests.get(
            url, headers=self._headers(), timeout=60, allow_redirects=True
        )
        response.raise_for_status()
        return response.content

    @staticmethod
    def extract_file_metadata(item: dict) -> dict:
        """Extract a standardised metadata dict from a Graph API drive item.

        Args:
            item: A drive item dict as returned by :meth:`get_drive_items` or
                :meth:`get_all_files`.

        Returns:
            A dict with the following keys:

            - ``id`` – Graph API item ID (used to track changes by ID and last modified date).
            - ``name`` – filename including extension.
            - ``webUrl`` – full SharePoint URL to the file.
            - ``lastModifiedDateTime`` – ISO-8601 timestamp of last modification.
            - ``filePath`` – server-relative folder path derived from
              ``parentReference.path``, with the ``/drives/{id}/root:`` prefix
              stripped so only the human-readable path remains.
        """
        parent_ref = item.get("parentReference", {})
        raw_path = parent_ref.get("path", "")
        # Strip the /drives/{driveId}/root: prefix → keep only the folder path
        file_path = raw_path.split("root:", 1)[-1] if "root:" in raw_path else raw_path

        return {
            "id": item.get("id", ""),
            "name": item.get("name", ""),
            "webUrl": item.get("webUrl", ""),
            "lastModifiedDateTime": item.get("lastModifiedDateTime", ""),
            "filePath": file_path,
        }

    def get_all_files(
        self,
        extensions: set[str] | None = None,
    ) -> list[dict]:
        """Return all files across every drive (document library) in the site.

        Iterates all drives returned by :meth:`get_site_drives`, calls
        :meth:`get_drive_items` on each, and merges the results into a single
        flat list. A ``"driveId"`` key is injected into each item dict so
        callers can pass it directly to :meth:`download_drive_item` without an
        additional lookup.

        Args:
            extensions: An optional set of lowercase file extensions to include,
                e.g. ``{'.pdf', '.docx', '.pptx'}``. Pass ``None`` to return
                all files regardless of type.

        Returns:
            A flat list of file item metadata dicts, each augmented with a
            ``"driveId"`` key.

        Raises:
            requests.HTTPError: If any underlying Graph API request fails.
        """
        all_items: list[dict] = []
        for drive in self.get_site_drives():
            drive_id = drive["id"]
            items = self.get_drive_items(drive_id, extensions=extensions)
            # Include driveId so caller can download file item without extra lookup
            for item in items:
                item["driveId"] = drive_id
            all_items.extend(items)
            logger.info(
                "Drive '%s': found %d matching file(s).", drive.get("name"), len(items)
            )
        logger.info("Total files found across all drives: %d", len(all_items))
        return all_items

