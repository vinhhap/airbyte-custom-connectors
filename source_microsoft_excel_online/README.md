# Source: Microsoft Excel Online (SharePoint)

Custom Airbyte **Python CDK** source connector that reads an Excel workbook stored in **SharePoint / OneDrive for Business** via **Microsoft Graph Excel APIs**.

It authenticates using **MSAL client credentials** (`acquire_token_for_client`) and reads worksheet values via:
- `usedRange(valuesOnly=true)` (default), or
- `range(address='A1:D100')` if you provide `range_address`.

## What’s implemented

- **Client Credentials auth** with `msal.ConfidentialClientApplication.acquire_token_for_client`.
- **SharePoint path resolution**:
  - `sharepoint_hostname` + `sharepoint_site_path` → Graph `site_id`
  - `site_id` → document library `drive_id` (prefers drive named `Documents` or `Shared Documents`)
  - `sharepoint_directory_path` + `excel_file_name` → workbook `workbook_item_id`
- **Multiple streams**: each configured workbook/worksheet becomes its own Airbyte stream.
- Worksheet rows are emitted as records:
  - `row_number` (1-based in the returned matrix)
  - `data` (dict keyed by header row or `col_n`)

## Prerequisites (Azure / SharePoint)

Create an Azure AD App Registration for **application (client credentials)** access.

Typical Microsoft Graph **Application permissions** required:
- `Sites.Read.All`
- `Files.Read.All`

Then grant **Admin Consent** in Azure.

> Note: Microsoft Graph permissions can be tenant-specific and security policies vary. If you get 401/403 errors, confirm that the app has **application** permissions (not delegated) and that admin consent was granted.

## Configuration

You must provide auth + a `workbooks` array. Each workbook item provides either:
- SharePoint location fields (recommended), **or**
- Advanced Graph IDs (`drive_id` + `workbook_item_id`).

### Option A (recommended): SharePoint location

```json
{
  "tenant_id": "<tenant-guid>",
  "client_id": "<app-guid>",
  "client_secret": "<secret>",

  "workbooks": [
    {
      "location_type": "sharepoint",
      "sharepoint_hostname": "contoso.sharepoint.com",
      "sharepoint_site_path": "/sites/MySite",
      "sharepoint_directory_path": "Shared Documents/Reports",
      "excel_file_name": "report.xlsx",

      "worksheet_name": "Sheet1",
      "header_row": 1,
      "range_address": "A1:D100",
      "stream_name": "report_sheet1"
    }
  ]
}
```

Notes:
- `sharepoint_site_path` is the server-relative site path (commonly starts with `/sites/` or `/teams/`).
- `sharepoint_directory_path` is inside the site’s **document library drive**. If you copied a UI path like `Shared Documents/Reports`, that prefix is accepted.

### Option B (advanced): Direct Graph IDs

```json
{
  "tenant_id": "<tenant-guid>",
  "client_id": "<app-guid>",
  "client_secret": "<secret>",

  "workbooks": [
    {
      "location_type": "graph_ids",
      "drive_id": "<graph-drive-id>",
      "workbook_item_id": "<graph-item-id>",
      "worksheet_name": "Sheet1"
    }
  ]
}
```

### Optional settings

- `graph_base_url` (default `https://graph.microsoft.com/v1.0`)
- `scopes` (default `["https://graph.microsoft.com/.default"]`)
- Per-workbook:
  - `range_address` (omit to use `usedRange(valuesOnly=true)`)
  - `header_row` (default `1`; set `0` for no headers)
  - `stream_name` (defaults to `worksheet_name`; duplicates get suffixed with `_2`, `_3`, ...)

Advanced networking (optional):
- `request_timeout_seconds` (default `60`)
- `max_retries` (default `5`; retries on `429` and transient `5xx`)
- `initial_backoff_seconds` (default `1.0`)
- `max_backoff_seconds` (default `60.0`)

## Best practices (Airbyte)

This connector follows Airbyte’s connector best practices where practical:
- **Fail fast, fail actionably**: Graph API errors surface useful `error.code`/`error.message` (and request IDs when available).
- **Rate limiting**: handles Graph `429`/`Retry-After` with exponential backoff + jitter.
- **Lightweight check connection**: if you don’t provide `range_address`, `check` verifies access using a minimal `A1:A1` request rather than pulling the full used range.

## Run locally

This repo can be run directly with Python (commands below). Airbyte’s docs also describe a broader local connector workflow using tools like **Poe** and the **airbyte-cdk CLI**; those are most relevant when developing inside the main Airbyte monorepo.

Reference: https://docs.airbyte.com/platform/connector-development/local-connector-development

### 1) Install dependencies

This project uses `pyproject.toml` (Poetry build backend). Either approach below is fine.

**Option A: Poetry**

```bash
cd /opt/airflow/airbyte-connectors/source_microsoft_excel_online
poetry install
```

**Option B: pip (editable install)**

```bash
cd /opt/airflow/airbyte-connectors/source_microsoft_excel_online
python -m venv .venv
. .venv/bin/activate
pip install -U pip
pip install -e .
```

### 2) Check connection

Create `secrets/config.json` (example above), then:

```bash
python main.py check --config secrets/config.json
```

### 3) Read sample (discover + read)

```bash
python main.py discover --config secrets/config.json > /tmp/catalog.json
python main.py read --config secrets/config.json --catalog /tmp/catalog.json
```

Note: `discover` outputs Airbyte messages (JSONL). This connector's entrypoint will automatically
extract the discovered catalog and convert it to a configured catalog for `read`.

## Airbyte local connector tooling (optional)

If you prefer using Airbyte’s recommended tooling:

- Install the Airbyte CDK CLI:

```bash
uv tool install --upgrade 'airbyte-cdk[dev]'
airbyte-cdk --help
```

- (If working inside the Airbyte monorepo) use Poe tasks to run common steps like install/tests.

> Note: This connector project itself does not include Poe task definitions; the `poe ...` workflow is typically used from within the Airbyte repo.

## Run unit tests

```bash
python -m unittest discover -s unit_tests -p 'test_*.py'
```

## Dockerize

Build the image:

```bash
docker build -t source-microsoft-excel-online:dev .
```

Run a connection check:

```bash
docker run --rm -v "$PWD/secrets":/secrets source-microsoft-excel-online:dev \
  check --config /secrets/config.json
```

## Deploy to an Airbyte cluster (OSS)

High-level steps:
1. Build and push your image to a registry your cluster can pull from:
   - `docker tag source-microsoft-excel-online:dev <registry>/source-microsoft-excel-online:dev`
   - `docker push <registry>/source-microsoft-excel-online:dev`
2. In Airbyte, register a custom connector definition pointing at that image.
3. Create a Source using the config JSON fields above.
4. Create a Connection from this Source to your destination.