import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping, Optional

from airbyte_cdk import AirbyteEntrypoint, launch

from .source import SourceMicrosoftExcelOnline


def _find_arg_value(args: list[str], flag: str) -> Optional[str]:
    try:
        i = args.index(flag)
    except ValueError:
        return None
    if i + 1 >= len(args):
        return None
    return args[i + 1]


def _load_catalog_like(path: str) -> Mapping[str, Any]:
    """Loads either:
    - a JSON object, or
    - an Airbyte JSONL output containing a CATALOG message.
    """

    raw = Path(path).read_text(encoding="utf-8")

    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            # `discover` often emits Airbyte messages. If the file is a single CATALOG
            # message JSON object, unwrap it.
            if data.get("type") == "CATALOG" and isinstance(data.get("catalog"), dict):
                return data["catalog"]
            return data
    except json.JSONDecodeError:
        pass

    last_catalog: Optional[Mapping[str, Any]] = None
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        if obj.get("type") == "CATALOG" and isinstance(obj.get("catalog"), dict):
            last_catalog = obj["catalog"]

    if last_catalog is None:
        raise ValueError(f"Catalog file {path!r} is not valid JSON or Airbyte JSONL containing a CATALOG message")
    return last_catalog


def _to_configured_catalog(catalog: Mapping[str, Any]) -> Mapping[str, Any]:
    """Converts a discovered AirbyteCatalog into a ConfiguredAirbyteCatalog.

    This is a convenience for local runs: if you pipe the output of `discover` to a
    file and then pass it to `read`, Airbyte's entrypoint expects a *configured*
    catalog, not a discovered catalog or Airbyte messages.
    """

    streams = catalog.get("streams")
    if not isinstance(streams, list):
        raise ValueError("Invalid catalog: missing 'streams'")

    # Already configured?
    if streams and isinstance(streams[0], dict) and "stream" in streams[0]:
        return catalog

    configured_streams: list[dict[str, Any]] = []
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        configured_streams.append(
            {
                "stream": stream,
                "sync_mode": "full_refresh",
                "destination_sync_mode": "append",
            }
        )

    return {"streams": configured_streams}


def _maybe_convert_catalog_arg_for_read(args: list[str]) -> list[str]:
    if not args or args[0] != "read":
        return args

    catalog_path = _find_arg_value(args, "--catalog")
    if not catalog_path:
        return args

    try:
        catalog_like = _load_catalog_like(catalog_path)
        configured = _to_configured_catalog(catalog_like)
    except Exception:
        # If we can't parse/convert, let Airbyte's normal validation raise a clear error.
        return args

    tmp = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", delete=False)
    with tmp:
        json.dump(configured, tmp, ensure_ascii=False)

    updated = list(args)
    i = updated.index("--catalog")
    updated[i + 1] = tmp.name
    return updated


def run() -> None:
    args = sys.argv[1:]
    args = _maybe_convert_catalog_arg_for_read(args)
    # Extract these paths for compatibility with Airbyte's standard CLI invocation.
    # This connector does not currently require direct access to these files.
    _ = (
        AirbyteEntrypoint.extract_catalog(args),
        AirbyteEntrypoint.extract_config(args),
        AirbyteEntrypoint.extract_state(args),
    )
    launch(SourceMicrosoftExcelOnline(), args)