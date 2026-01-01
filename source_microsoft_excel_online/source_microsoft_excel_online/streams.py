from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import SyncMode

from .stream_reader import MicrosoftGraphExcelClient, iter_worksheet_records


class WorksheetValuesStream(Stream):
    """Reads a worksheet (usedRange or explicit range) and emits row records."""

    def __init__(self, config: Mapping[str, Any], *, stream_name_override: Optional[str] = None):
        super().__init__()
        self._config = config
        self._client = MicrosoftGraphExcelClient(config)

        self._stream_name_override = stream_name_override

        # IMPORTANT: preserve header_row=0 (no headers)
        header_row = config.get("header_row", 1)
        self._header_row = 1 if header_row is None else int(header_row)

    @property
    def name(self) -> str:
        if self._stream_name_override:
            return str(self._stream_name_override)
        return str(self._config.get("stream_name") or self._config.get("worksheet_name") or "worksheet")

    @property
    def primary_key(self) -> Optional[str | list[str] | list[list[str]]]:
        return None

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "row_number": {"type": "integer"},
                "data": {"type": "object", "additionalProperties": True},
            },
        }

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[list[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        _ = (sync_mode, cursor_field, stream_slice, stream_state)

        location = self._client.resolve_excel_location(self._config)

        values = self._client.get_worksheet_values(
            drive_id=location.drive_id,
            item_id=location.workbook_item_id,
            worksheet_name=location.worksheet_name,
            range_address=location.range_address,
        )

        yield from iter_worksheet_records(values, header_row=self._header_row)
