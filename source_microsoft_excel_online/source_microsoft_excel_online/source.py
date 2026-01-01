from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from .config_utils import describe_workbook, iter_workbook_configs

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .stream_reader import MicrosoftGraphExcelClient, MicrosoftGraphExcelError
from .streams import WorksheetValuesStream


class SourceMicrosoftExcelOnline(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> tuple[bool, Optional[Any]]:
        try:
            client = MicrosoftGraphExcelClient(config)
            for idx, workbook_config in iter_workbook_configs(config):
                try:
                    location = client.resolve_excel_location(workbook_config)
                    # Keep this lightweight and fast: verify auth + workbook access + worksheet.
                    # If the user didn't specify a range, request a minimal cell range.
                    range_address = location.range_address or "A1:A1"
                    client.get_worksheet_values(
                        drive_id=location.drive_id,
                        item_id=location.workbook_item_id,
                        worksheet_name=location.worksheet_name,
                        range_address=range_address,
                    )
                except (KeyError, TypeError, ValueError, MicrosoftGraphExcelError) as e:
                    raise MicrosoftGraphExcelError(
                        f"Connection check failed for {describe_workbook(workbook_config, idx)}: {e}"
                    ) from e
            return True, None
        except (KeyError, TypeError, ValueError, MicrosoftGraphExcelError) as e:
            logger.exception("Connection check failed")
            return False, e

    def streams(self, config: Mapping[str, Any]) -> list[Stream]:
        streams: list[Stream] = []
        name_counts: dict[str, int] = {}

        for idx, workbook_config in iter_workbook_configs(config):
            base_name = str(workbook_config.get("stream_name") or workbook_config.get("worksheet_name") or f"worksheet_{idx + 1}")
            count = name_counts.get(base_name, 0) + 1
            name_counts[base_name] = count
            stream_name = base_name if count == 1 else f"{base_name}_{count}"

            streams.append(WorksheetValuesStream(config=workbook_config, stream_name_override=stream_name))

        return streams

