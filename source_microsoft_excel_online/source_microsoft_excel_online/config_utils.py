from __future__ import annotations

from typing import Any, Iterable, Mapping


def iter_workbook_configs(config: Mapping[str, Any]) -> Iterable[tuple[int, dict[str, Any]]]:
    """Yield (index, merged_config) for each workbook.

    The connector supports a modern config shape:

        { ...global_fields, "workbooks": [ { ...workbook_fields }, ... ] }

    For backward compatibility, if "workbooks" is missing but legacy workbook keys are present,
    this yields a single workbook based on the root config.
    """

    workbooks = config.get("workbooks")
    if isinstance(workbooks, list):
        for idx, workbook in enumerate(workbooks):
            if not isinstance(workbook, Mapping):
                raise ValueError(f"workbooks[{idx}] must be an object")

            merged = dict(config)
            merged.pop("workbooks", None)
            merged.update(dict(workbook))
            yield idx, merged
        return

    # Legacy/single-workbook configuration fallback
    if "worksheet_name" in config:
        merged = dict(config)
        merged.pop("workbooks", None)
        yield 0, merged
        return

    raise ValueError("Missing required configuration: provide 'workbooks' (preferred) or legacy workbook fields")


def describe_workbook(config: Mapping[str, Any], idx: int) -> str:
    parts: list[str] = []

    for key in ("stream_name", "excel_file_name", "worksheet_name", "drive_id", "workbook_item_id"):
        val = config.get(key)
        if val:
            parts.append(f"{key}={val!r}")

    if "sharepoint_hostname" in config or "sharepoint_site_path" in config:
        hostname = config.get("sharepoint_hostname")
        site_path = config.get("sharepoint_site_path")
        if hostname or site_path:
            parts.append(f"sharepoint={hostname!r}{site_path!r}")

    suffix = ", ".join(parts) if parts else "(no details)"
    return f"workbooks[{idx}] {suffix}"
