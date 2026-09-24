from __future__ import annotations

_DATA_ENTRY_POINT_ALIASES: dict[str, str] | None = None


def _data_entry_point_aliases() -> dict[str, str]:
    global _DATA_ENTRY_POINT_ALIASES
    if _DATA_ENTRY_POINT_ALIASES is None:
        try:
            from specializations import get_data_entry_point_aliases
        except ModuleNotFoundError:
            _DATA_ENTRY_POINT_ALIASES = {}
        else:
            _DATA_ENTRY_POINT_ALIASES = get_data_entry_point_aliases()
    return _DATA_ENTRY_POINT_ALIASES
