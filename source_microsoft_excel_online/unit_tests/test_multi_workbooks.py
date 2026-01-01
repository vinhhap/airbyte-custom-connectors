import unittest
from unittest.mock import patch

from source_microsoft_excel_online.source import SourceMicrosoftExcelOnline


class _FakeMsalApp:
    def acquire_token_for_client(self, scopes=None):  # noqa: ARG002
        return {"access_token": "fake-token"}


class MultiWorkbooksTest(unittest.TestCase):
    def test_streams_creates_one_stream_per_workbook_and_makes_names_unique(self):
        config = {
            "tenant_id": "t",
            "client_id": "c",
            "client_secret": "s",
            "workbooks": [
                {
                    "location_type": "graph_ids",
                    "drive_id": "drive",
                    "workbook_item_id": "item-1",
                    "worksheet_name": "Sheet1",
                },
                {
                    "location_type": "graph_ids",
                    "drive_id": "drive",
                    "workbook_item_id": "item-2",
                    "worksheet_name": "Sheet1",
                },
            ],
        }

        with patch("source_microsoft_excel_online.stream_reader.msal") as msal_mod:
            msal_mod.ConfidentialClientApplication.return_value = _FakeMsalApp()
            streams = SourceMicrosoftExcelOnline().streams(config)

        self.assertEqual([s.name for s in streams], ["Sheet1", "Sheet1_2"])

    def test_streams_supports_legacy_single_workbook_config(self):
        config = {
            "tenant_id": "t",
            "client_id": "c",
            "client_secret": "s",
            "location_type": "graph_ids",
            "drive_id": "drive",
            "workbook_item_id": "item",
            "worksheet_name": "Sheet1",
        }

        with patch("source_microsoft_excel_online.stream_reader.msal") as msal_mod:
            msal_mod.ConfidentialClientApplication.return_value = _FakeMsalApp()
            streams = SourceMicrosoftExcelOnline().streams(config)

        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0].name, "Sheet1")


if __name__ == "__main__":
    unittest.main()
