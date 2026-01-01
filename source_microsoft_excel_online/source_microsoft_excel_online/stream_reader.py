from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import quote

import requests

logger = logging.getLogger("airbyte")


try:
    import msal  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    msal = None


class MicrosoftGraphExcelError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExcelLocation:
    drive_id: str
    workbook_item_id: str
    worksheet_name: str
    range_address: Optional[str] = None


class MicrosoftGraphExcelClient:
    """Tiny Microsoft Graph client to read Excel worksheet values using MSAL client-credentials auth."""

    def __init__(self, config: Mapping[str, Any]):
        if msal is None:
            raise MicrosoftGraphExcelError(
                "Missing dependency 'msal'. Install it (e.g., `pip install msal`) to use this connector."
            )
        self._config = config
        self._graph_base_url = str(config.get("graph_base_url") or "https://graph.microsoft.com/v1.0").rstrip("/")

        tenant_id = str(config["tenant_id"])
        client_id = str(config["client_id"])
        client_secret = str(config["client_secret"])

        self._scopes = list(config.get("scopes") or ["https://graph.microsoft.com/.default"])
        self._msal_app = msal.ConfidentialClientApplication(
            client_id=client_id,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
            client_credential=client_secret,
        )
        self._session = requests.Session()

        # Networking defaults (keep conservative but reliable).
        # Allow overrides via config for advanced users.
        self._timeout_seconds = float(config.get("request_timeout_seconds") or 60)
        self._max_retries = int(config.get("max_retries") or 5)
        self._initial_backoff_seconds = float(config.get("initial_backoff_seconds") or 1.0)
        self._max_backoff_seconds = float(config.get("max_backoff_seconds") or 60.0)

    def _get_access_token(self) -> str:
        token_result = self._msal_app.acquire_token_for_client(scopes=self._scopes)

        if not isinstance(token_result, dict):
            raise MicrosoftGraphExcelError("MSAL did not return a token payload")

        access_token = token_result.get("access_token")
        if access_token:
            return str(access_token)

        error = token_result.get("error")
        desc = token_result.get("error_description")
        correlation_id = token_result.get("correlation_id")
        raise MicrosoftGraphExcelError(
            f"Failed to acquire access token via MSAL: error={error!r} correlation_id={correlation_id!r} description={desc!r}"
        )

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
            "User-Agent": "Airbyte/source-microsoft-excel-online",
        }

    @staticmethod
    def _extract_graph_error_message(resp: requests.Response) -> str:
        request_id = resp.headers.get("request-id") or resp.headers.get("x-ms-request-id")
        client_request_id = resp.headers.get("client-request-id")

        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                payload = resp.json()
                if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
                    err = payload["error"]
                    code = err.get("code")
                    message = err.get("message")
                    parts = [p for p in (code, message) if p]
                    if parts:
                        suffix = []
                        if request_id:
                            suffix.append(f"request_id={request_id}")
                        if client_request_id:
                            suffix.append(f"client_request_id={client_request_id}")
                        return " | ".join([" - ".join(map(str, parts))] + suffix)
            except (ValueError, TypeError):
                pass

        # Fallback: include raw body (truncated) and request ids if any.
        body = (resp.text or "").strip()
        if len(body) > 2000:
            body = body[:2000] + "…"
        suffix = []
        if request_id:
            suffix.append(f"request_id={request_id}")
        if client_request_id:
            suffix.append(f"client_request_id={client_request_id}")
        if suffix:
            return f"{body} | " + " ".join(suffix)
        return body

    def _retry_sleep_seconds(self, resp: requests.Response, attempt: int) -> float:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                pass

        # Exponential backoff with jitter
        base = min(self._max_backoff_seconds, self._initial_backoff_seconds * (2**attempt))
        jitter = random.random() * 0.25 * base
        return base + jitter

    def _request_json(self, method: str, url: str, *, params: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        last_error: Optional[BaseException] = None

        for attempt in range(self._max_retries + 1):
            try:
                resp = self._session.request(
                    method=method,
                    url=url,
                    headers=self._headers(),
                    params=params,
                    timeout=self._timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self._max_retries:
                    break
                sleep_s = min(self._max_backoff_seconds, self._initial_backoff_seconds * (2**attempt))
                logger.warning("Graph request failed (%s). Retrying in %.1fs", type(exc).__name__, sleep_s)
                time.sleep(sleep_s)
                continue

            if resp.status_code < 400:
                return resp.json()

            # Retry on common transient/rate-limited statuses.
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < self._max_retries:
                sleep_s = self._retry_sleep_seconds(resp, attempt)
                logger.warning("Graph API %s for %s. Retrying in %.1fs", resp.status_code, url, sleep_s)
                time.sleep(sleep_s)
                continue

            message = self._extract_graph_error_message(resp)
            raise MicrosoftGraphExcelError(f"Graph API error {resp.status_code} for {url}: {message}")

        raise MicrosoftGraphExcelError(f"Graph request failed after retries for {url}: {last_error}")

    def resolve_sharepoint_site_id(self, *, hostname: str, site_path: str) -> str:
        """Resolve a SharePoint site into a Microsoft Graph site id.

        `hostname`: e.g. contoso.sharepoint.com
        `site_path`: server-relative site path, e.g. /sites/MySite or /teams/MyTeam
        """

        normalized_hostname = hostname.strip()
        if not normalized_hostname:
            raise MicrosoftGraphExcelError("sharepoint_hostname is required")

        normalized_site_path = site_path.strip()
        if not normalized_site_path:
            raise MicrosoftGraphExcelError("sharepoint_site_path is required")

        # Users sometimes provide:
        # - server-relative paths: "/sites/MySite" or "/teams/MyTeam"
        # - relative paths: "sites/MySite"
        # - just the site name: "MySite"
        # We try a few common patterns to be tolerant.
        normalized_site_path = normalized_site_path.lstrip("/")
        candidates: list[str]
        if normalized_site_path.startswith(("sites/", "teams/")):
            candidates = [normalized_site_path]
        elif "/" in normalized_site_path:
            candidates = [normalized_site_path]
        else:
            candidates = [f"sites/{normalized_site_path}", f"teams/{normalized_site_path}", normalized_site_path]

        last_error: Optional[BaseException] = None
        for candidate in candidates:
            url = f"{self._graph_base_url}/sites/{quote(normalized_hostname, safe='')}:/{quote(candidate, safe='/')}"
            try:
                payload = self._request_json("GET", url, params={"$select": "id"})
            except MicrosoftGraphExcelError as exc:
                # Only swallow "site not found" style errors; auth/permission errors should surface.
                msg = str(exc)
                if "Graph API error 404" in msg or "\"itemNotFound\"" in msg:
                    last_error = exc
                    continue
                raise

            site_id = payload.get("id")
            if site_id:
                return str(site_id)

            last_error = MicrosoftGraphExcelError(
                f"Could not resolve SharePoint site id for hostname={hostname!r} site_path={site_path!r} (candidate={candidate!r})"
            )

        if last_error:
            raise MicrosoftGraphExcelError(str(last_error))
        raise MicrosoftGraphExcelError(f"Could not resolve SharePoint site id for hostname={hostname!r} site_path={site_path!r}")

    def resolve_default_drive_id(self, *, site_id: str) -> str:
        """Resolve the site's default document library drive id."""

        url = f"{self._graph_base_url}/sites/{quote(site_id, safe='')}/drive"
        payload = self._request_json("GET", url, params={"$select": "id"})
        drive_id = payload.get("id")
        if not drive_id:
            raise MicrosoftGraphExcelError(f"Could not resolve default drive id for site_id={site_id!r}")
        return str(drive_id)

    def resolve_document_library_drive_id(self, *, site_id: str) -> str:
        """Resolve a site's document library drive id.

        Preferred behavior (matches common SharePoint UI expectations):
        - List drives via /sites/{site-id}/drives
        - Select the drive named "Documents" or "Shared Documents".

        Falls back to the site's default drive if a matching drive isn't found.
        """

        url = f"{self._graph_base_url}/sites/{quote(site_id, safe='')}/drives"
        payload = self._request_json("GET", url, params={"$select": "id,name"})

        drives = payload.get("value")
        if isinstance(drives, list):
            preferred_names = {"Documents", "Shared Documents"}
            for drive in drives:
                if not isinstance(drive, dict):
                    continue
                name = drive.get("name")
                drive_id = drive.get("id")
                if name in preferred_names and drive_id:
                    return str(drive_id)
            if len(drives) == 1 and isinstance(drives[0], dict) and drives[0].get("id"):
                return str(drives[0]["id"])

        # Final fallback: use the default drive endpoint
        return self.resolve_default_drive_id(site_id=site_id)

    def resolve_drive_item_id_by_path(self, *, drive_id: str, item_path: str) -> str:
        """Resolve a drive item id by path inside a drive.

        Uses the Graph path-based addressing forms:
        - /drives/{drive-id}/root:/{path}
        - /drives/{drive-id}/root:/{path}:

        Some tenants seem to require the trailing ':'; we attempt both.
        """

        normalized_path = item_path.strip().strip("/")
        if not normalized_path:
            raise MicrosoftGraphExcelError("Excel file path is empty; check sharepoint_directory_path and excel_file_name")

        encoded = quote(normalized_path, safe="/")
        base = f"{self._graph_base_url}/drives/{quote(drive_id, safe='')}/root:/{encoded}"

        for url in (base, f"{base}:"):
            try:
                payload = self._request_json("GET", url, params={"$select": "id,name"})
            except MicrosoftGraphExcelError:
                continue
            item_id = payload.get("id")
            if item_id:
                return str(item_id)

        raise MicrosoftGraphExcelError(f"Could not resolve drive item id for drive_id={drive_id!r} path={item_path!r}")

    def resolve_excel_location(self, config: Mapping[str, Any]) -> ExcelLocation:
        """Resolve an ExcelLocation from config.

        Supports either:
        - direct Graph IDs: drive_id + workbook_item_id
        - SharePoint location: sharepoint_hostname + sharepoint_site_path + sharepoint_directory_path + excel_file_name
        """

        worksheet_name = str(config["worksheet_name"])
        range_address = str(config["range_address"]) if config.get("range_address") else None

        drive_id = config.get("drive_id")
        item_id = config.get("workbook_item_id")
        if drive_id and item_id:
            return ExcelLocation(
                drive_id=str(drive_id),
                workbook_item_id=str(item_id),
                worksheet_name=worksheet_name,
                range_address=range_address,
            )

        hostname = str(config.get("sharepoint_hostname") or "").strip()
        site_path = str(config.get("sharepoint_site_path") or "").strip()
        directory_path = str(config.get("sharepoint_directory_path") or "").strip()
        file_name = str(config.get("excel_file_name") or "").strip()

        missing = [
            key
            for key, val in (
                ("sharepoint_hostname", hostname),
                ("sharepoint_site_path", site_path),
                ("sharepoint_directory_path", directory_path),
                ("excel_file_name", file_name),
            )
            if not val
        ]
        if missing:
            raise MicrosoftGraphExcelError(
                "Missing configuration. Provide either (drive_id + workbook_item_id) or SharePoint fields: "
                + ", ".join(missing)
            )

        site_id = self.resolve_sharepoint_site_id(hostname=hostname, site_path=site_path)
        drive_id = self.resolve_document_library_drive_id(site_id=site_id)

        # Users frequently copy paths as shown in SharePoint UI, which often includes a
        # leading document library name (e.g. "Shared Documents/Reports"). When we
        # already resolved the document library drive, strip that prefix for tolerance.
        normalized_directory_path = directory_path.strip().strip("/")
        for library_root in ("Shared Documents", "Documents"):
            if normalized_directory_path == library_root:
                normalized_directory_path = ""
                break
            prefix = f"{library_root}/"
            if normalized_directory_path.startswith(prefix):
                normalized_directory_path = normalized_directory_path[len(prefix) :]
                break

        # Build path within the drive.
        # Users typically provide a path like "Shared Documents/Folder".
        if normalized_directory_path:
            item_path = f"{normalized_directory_path.rstrip('/')}/{file_name}"
        else:
            item_path = file_name

        workbook_item_id = self.resolve_drive_item_id_by_path(drive_id=drive_id, item_path=item_path)
        return ExcelLocation(
            drive_id=drive_id,
            workbook_item_id=workbook_item_id,
            worksheet_name=worksheet_name,
            range_address=range_address,
        )

    def get_worksheet_values(
        self,
        *,
        drive_id: str,
        item_id: str,
        worksheet_name: str,
        range_address: Optional[str] = None,
    ) -> list[list[Any]]:
        worksheet_segment = quote(worksheet_name, safe="")
        base = f"{self._graph_base_url}/drives/{quote(drive_id, safe='')}/items/{quote(item_id, safe='')}/workbook/worksheets/{worksheet_segment}"

        if range_address:
            # Example: range(address='A1:D100')
            # Keep quotes and parentheses unescaped in the path; only escape the address itself.
            address = quote(range_address, safe="")
            url = f"{base}/range(address='{address}')"
        else:
            url = f"{base}/usedRange(valuesOnly=true)"

        payload = self._request_json("GET", url, params={"$select": "values"})
        values = payload.get("values")
        if values is None:
            # Some Graph responses can be empty for blank sheets
            return []
        if not isinstance(values, list):
            raise MicrosoftGraphExcelError(f"Unexpected Graph response: 'values' is not a list (got {type(values)})")
        return values  # type: ignore[return-value]


def iter_worksheet_records(values: list[list[Any]], *, header_row: int = 1) -> Iterable[Mapping[str, Any]]:
    """Convert Graph 'values' matrix into Airbyte records.

    - If header_row == 0: no headers, columns are col_1..col_n
    - If header_row >= 1: uses that 1-based row as headers and skips it in output
    """

    if not values:
        return

    header: Optional[list[str]] = None
    header_idx = header_row - 1
    if header_row > 0 and 0 <= header_idx < len(values):
        header = [str(x).strip() if x is not None else "" for x in values[header_idx]]

    for i, row in enumerate(values):
        if header is not None and i == header_idx:
            continue

        cols = header if header is not None else [f"col_{j + 1}" for j in range(len(row))]
        data = {cols[j] if j < len(cols) and cols[j] else f"col_{j + 1}": row[j] for j in range(len(row))}
        yield {"row_number": i + 1, "data": data}