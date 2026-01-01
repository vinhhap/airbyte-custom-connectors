import unittest
from unittest.mock import patch

from source_microsoft_excel_online.stream_reader import MicrosoftGraphExcelClient, MicrosoftGraphExcelError, iter_worksheet_records


class _FakeResponse:
    def __init__(self, *, status_code: int, headers: dict[str, str], text: str, json_payload=None):
        self.status_code = status_code
        self.headers = headers
        self.text = text
        self._json_payload = json_payload

    def json(self):
        if self._json_payload is None:
            raise ValueError("No JSON payload")
        return self._json_payload


class _FakeMsalApp:
    def acquire_token_for_client(self, scopes=None):  # noqa: ARG002
        return {"access_token": "fake-token"}


def _make_client(config: dict) -> MicrosoftGraphExcelClient:
    with patch("source_microsoft_excel_online.stream_reader.msal") as msal_mod:
        msal_mod.ConfidentialClientApplication.return_value = _FakeMsalApp()
        return MicrosoftGraphExcelClient(config)


class WorksheetValuesStreamTest(unittest.TestCase):
    def test_emits_records_with_header_row(self):
        records = list(iter_worksheet_records([["id", "name"], [1, "a"], [2, "b"]], header_row=1))

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["data"], {"id": 1, "name": "a"})
        self.assertEqual(records[0]["row_number"], 2)

    def test_emits_records_without_header_row(self):
        records = list(iter_worksheet_records([[1, "a"], [2, "b"]], header_row=0))

        self.assertEqual(records[0]["data"], {"col_1": 1, "col_2": "a"})

    def test_resolve_excel_location_uses_direct_ids_when_present(self):
        config = {
            "tenant_id": "t",
            "client_id": "c",
            "client_secret": "s",
            "drive_id": "drive",
            "workbook_item_id": "item",
            "worksheet_name": "Sheet1",
        }
        client = _make_client(config)

        with patch.object(MicrosoftGraphExcelClient, "_request_json") as request_json:
            location = client.resolve_excel_location(config)
            self.assertEqual(location.drive_id, "drive")
            self.assertEqual(location.workbook_item_id, "item")
            self.assertEqual(location.worksheet_name, "Sheet1")
            request_json.assert_not_called()

    def test_resolve_excel_location_from_sharepoint_fields_builds_expected_urls(self):
        config = {
            "tenant_id": "t",
            "client_id": "c",
            "client_secret": "s",
            "sharepoint_hostname": "contoso.sharepoint.com",
            "sharepoint_site_path": "/sites/My Site",
            "sharepoint_directory_path": "Shared Documents/Reports 2025",
            "excel_file_name": "report.xlsx",
            "worksheet_name": "Sheet 1",
        }
        client = _make_client(config)
        calls: list[tuple[str, str, dict]] = []

        def fake_request_json(method: str, url: str, *, params=None):
            calls.append((method, url, dict(params or {})))
            if "/sites/contoso.sharepoint.com:/sites/My%20Site" in url:
                return {"id": "site-id"}
            if "/sites/site-id/drives" in url:
                return {"value": [{"id": "drive-id", "name": "Shared Documents"}]}
            if "/drives/drive-id/root:/Reports%202025/report.xlsx" in url:
                return {"id": "item-id", "name": "report.xlsx"}
            return {}

        with patch.object(MicrosoftGraphExcelClient, "_request_json", side_effect=fake_request_json):
            location = client.resolve_excel_location(config)

        self.assertEqual(location.drive_id, "drive-id")
        self.assertEqual(location.workbook_item_id, "item-id")
        self.assertEqual(location.worksheet_name, "Sheet 1")

        self.assertGreaterEqual(len(calls), 3)
        self.assertEqual(calls[0][0], "GET")
        self.assertIn("$select", calls[0][2])

    def test_resolve_sharepoint_site_id_accepts_bare_site_name(self):
        config = {
            "tenant_id": "t",
            "client_id": "c",
            "client_secret": "s",
            "sharepoint_hostname": "contoso.sharepoint.com",
            "sharepoint_site_path": "MySite",
            "sharepoint_directory_path": "Shared Documents",
            "excel_file_name": "report.xlsx",
            "worksheet_name": "Sheet1",
        }
        client = _make_client(config)

        def fake_request_json(method: str, url: str, *, params=None):  # noqa: ARG001
            if "/sites/contoso.sharepoint.com:/sites/MySite" in url:
                return {"id": "site-id"}
            if "/sites/site-id/drives" in url:
                return {"value": [{"id": "drive-id", "name": "Documents"}]}
            if "/drives/drive-id/root:/report.xlsx" in url:
                return {"id": "item-id", "name": "report.xlsx"}
            return {}

        with patch.object(MicrosoftGraphExcelClient, "_request_json", side_effect=fake_request_json):
            location = client.resolve_excel_location(config)

        self.assertEqual(location.drive_id, "drive-id")
        self.assertEqual(location.workbook_item_id, "item-id")

    def test_resolve_drive_item_id_by_path_tries_trailing_colon_variant(self):
        config = {"tenant_id": "t", "client_id": "c", "client_secret": "s", "worksheet_name": "Sheet1"}
        client = _make_client(config)

        side_effects = [
            MicrosoftGraphExcelError("Graph API error 404"),
            {"id": "item-id", "name": "report.xlsx"},
        ]
        with patch.object(MicrosoftGraphExcelClient, "_request_json", side_effect=side_effects) as request_json:
            item_id = client.resolve_drive_item_id_by_path(drive_id="drive-id", item_path="Shared Documents/report.xlsx")
            self.assertEqual(item_id, "item-id")
            self.assertEqual(request_json.call_count, 2)

    def test_request_json_raises_readable_graph_error_for_json_bodies(self):
        config = {"tenant_id": "t", "client_id": "c", "client_secret": "s", "worksheet_name": "Sheet1"}
        client = _make_client(config)

        fake = _FakeResponse(
            status_code=400,
            headers={"Content-Type": "application/json"},
            text="{\"error\":{\"message\":\"bad\"}}",
            json_payload={"error": {"message": "bad"}},
        )

        with patch.object(MicrosoftGraphExcelClient, "_get_access_token", return_value="token"):
            with patch.object(client._session, "request", return_value=fake):
                with self.assertRaises(MicrosoftGraphExcelError) as ctx:
                    client._request_json("GET", "https://graph.microsoft.com/v1.0/test")

        msg = str(ctx.exception)
        self.assertIn("Graph API error 400", msg)
        self.assertIn("bad", msg)


if __name__ == "__main__":
    unittest.main()
