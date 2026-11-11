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


def _coerce(value, *valid_types, name="x"):
    return _coerce_argument(name, value, InputPort(name, valid_type=valid_types if len(valid_types) > 1 else valid_types[0]))


@pytest.mark.usefixtures("aiida_profile")
def test_bare_integer_on_int_port_is_a_value_even_when_a_node_has_that_pk():
    stored = orm.Int(99).store()

    result = _coerce(stored.pk, orm.Int)

    assert isinstance(result, orm.Int) and not result.is_stored
    assert result.value == stored.pk


@pytest.mark.usefixtures("aiida_profile")
def test_explicit_uuid_loads_the_stored_node():
    stored = orm.Int(99).store()

    assert _coerce({"uuid": stored.uuid}, orm.Int).pk == stored.pk


@pytest.mark.usefixtures("aiida_profile")
def test_explicit_value_creates_a_literal():
    assert _coerce({"value": 2.5}, orm.Float).value == 2.5
    assert _coerce({"value": 5}, orm.Data).value == 5
    assert _coerce(4, orm.Int, type(None)).value == 4


@pytest.mark.usefixtures("aiida_profile")
def test_bare_integer_or_uuid_on_a_non_scalar_port_is_a_node_reference():
    stored = orm.Dict({"a": 1}).store()

    assert _coerce(stored.pk, orm.StructureData).uuid == stored.uuid
    assert _coerce(stored.uuid, orm.StructureData).pk == stored.pk


@pytest.mark.usefixtures("aiida_profile")
def test_bare_scalar_is_rejected_when_both_readings_are_possible():
    stored = orm.Int(99).store()

    with pytest.raises(ValueError, match="ambiguous"):
        _coerce(stored.pk, orm.Data)
    with pytest.raises(ValueError, match="ambiguous"):
        _coerce(stored.uuid, orm.Data)
    assert _coerce("pbe", orm.Data).value == "pbe"
    assert _coerce({"pk": stored.pk}, orm.Data).uuid == stored.uuid


@pytest.mark.usefixtures("aiida_profile")
def test_true_is_never_read_as_pk_one():
    from aiida_mcp.core.submission import _load_node_like

    assert _load_node_like(True) is True


@pytest.mark.usefixtures("aiida_profile")
def test_dict_port_keeps_a_dictionary_whose_only_key_is_value():
    assert _coerce({"value": 1}, orm.Dict, name="parameters").get_dict() == {"value": 1}
    assert _coerce({"pk": 1, "other": 2}, orm.Dict, name="parameters").get_dict() == {"pk": 1, "other": 2}


@pytest.mark.usefixtures("aiida_profile")
def test_literal_that_does_not_fit_the_port_is_rejected():
    with pytest.raises(ValueError, match="cannot hold"):
        _coerce({"value": "text"}, orm.Int)


@pytest.mark.usefixtures("aiida_profile")
def test_binding_error_names_the_port():
    from aiida.plugins import WorkflowFactory

    from aiida_mcp.core.submission import InputBindingError, _coerce_explicit_inputs

    namespace = WorkflowFactory("core.arithmetic.multiply_add").spec().inputs

    with pytest.raises(InputBindingError) as caught:
        _coerce_explicit_inputs(namespace, {"x": 1, "y": {"pk": 987654}})
    assert caught.value.port == "y"


@pytest.mark.usefixtures("aiida_profile")
def test_draft_binds_literals_not_nodes_sharing_the_pk():
    from aiida_mcp.core.submission import _build_builder_result

    shifted = [orm.Int(value).store() for value in (100, 200, 300)]
    computer = orm.Computer("coercion-test", "localhost", transport_type="core.local", scheduler_type="core.direct").store()
    code = orm.InstalledCode(label="add", computer=computer, filepath_executable="/bin/bash", default_calc_job_plugin="core.arithmetic.add").store()

    result = _build_builder_result({"entry_point": "core.arithmetic.multiply_add", "inputs": {"x": shifted[0].pk, "y": shifted[1].pk, "z": shifted[2].pk, "code": {"pk": code.pk}}})

    assert result["success"], result["errors"]
    assert [result["builder_inputs"][key] for key in "xyz"] == [node.pk for node in shifted]


@pytest.mark.usefixtures("aiida_profile")
def test_failed_binding_reports_the_port_not_every_missing_port():
    from aiida_mcp.core.submission import _build_builder_result

    result = _build_builder_result({"entry_point": "core.arithmetic.multiply_add", "inputs": {"x": 1, "y": {"pk": 987654}}})

    assert [(error["stage"], error.get("port")) for error in result["errors"]] == [("bind_explicit_inputs", "y")]
