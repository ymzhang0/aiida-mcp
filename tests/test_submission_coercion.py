import pytest

pytest.importorskip("aiida")
pytest_plugins = ["aiida.tools.pytest_fixtures"]

from aiida import orm  # noqa: E402
from aiida.engine.processes.ports import InputPort  # noqa: E402

from aiida_mcp.core.submission import _coerce_argument  # noqa: E402


@pytest.mark.usefixtures("aiida_profile")
@pytest.mark.parametrize(
    ("valid_type", "raw", "expected_type", "expected"),
    [
        (orm.Int, 2, orm.Int, 2),
        (orm.Float, 2, orm.Float, 2.0),
        (orm.Bool, True, orm.Bool, True),
        (orm.Str, "abc", orm.Str, "abc"),
        (orm.Dict, {"a": 1}, orm.Dict, {"a": 1}),
        (orm.List, [1, 2], orm.List, [1, 2]),
    ],
)
def test_raw_json_values_are_wrapped_for_base_type_ports(valid_type, raw, expected_type, expected):
    value = _coerce_argument("x", raw, InputPort("x", valid_type=valid_type))
    assert isinstance(value, expected_type)
    assert (value.get_dict() if expected_type is orm.Dict else value.get_list() if expected_type is orm.List else value.value) == expected


@pytest.mark.usefixtures("aiida_profile")
def test_explicit_pk_reference_still_loads_the_node():
    stored = orm.Int(7).store()
    value = _coerce_argument("x", {"pk": stored.pk}, InputPort("x", valid_type=orm.Int))
    assert value.uuid == stored.uuid
