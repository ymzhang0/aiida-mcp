import pytest

pytest.importorskip("aiida")
pytest_plugins = ["aiida.tools.pytest_fixtures"]

from aiida import orm  # noqa: E402

from aiida_mcp.core.engine import WorkerDomainError  # noqa: E402
from aiida_mcp.worker.application import computer_service  # noqa: E402

SETUP_YAML = "label: yaml-local\nhostname: localhost\ntransport: core.local\nscheduler: core.direct\nwork_dir: /tmp/aiida-mcp-test/\n"


def test_yaml_keys_map_to_request_fields_and_typos_are_rejected() -> None:
    fields = computer_service._from_yaml(SETUP_YAML + "prepend_text: module load x\n", computer_service._COMPUTER_YAML_KEYS, "computer setup")
    assert fields["computer_label"] == "yaml-local" and fields["transport_type"] == "core.local"
    assert computer_service._from_yaml("script_before: kinit\n", computer_service._CONFIGURE_YAML_KEYS, "computer configure") == {"authentication_script": "kinit"}
    with pytest.raises(ValueError, match="hostnme"):
        computer_service._from_yaml("hostnme: x\n", computer_service._COMPUTER_YAML_KEYS, "computer setup")


@pytest.mark.usefixtures("aiida_profile_clean")
def test_create_computer_and_code_end_to_end() -> None:
    created = computer_service.create_computer({"setup_yaml": SETUP_YAML, "configure_yaml": "safe_interval: 0\n"})
    assert created["status"] == "success" and created["connection_status"] == "success"
    with pytest.raises(WorkerDomainError):
        computer_service.create_computer({"setup_yaml": SETUP_YAML})

    missing = computer_service.create_code({"fields": {"label": "nope", "computer_label": "yaml-local", "remote_abspath": "definitely-not-installed-x", "default_calc_job_plugin": "core.arithmetic.add"}})
    assert missing["status"] == "not_created"
    assert not orm.QueryBuilder().append(orm.AbstractCode, filters={"label": "nope"}).count()

    code_yaml = "label: add\ncomputer: yaml-local\nfilepath_executable: bash\ndefault_calc_job_plugin: core.arithmetic.add\nwith_mpi: false\n"
    code = computer_service.create_code({"yaml": code_yaml})
    assert code["full_label"] == "add@yaml-local"
    assert code["filepath_executable"].endswith("/bash") and code["filepath_executable"].startswith("/")
    assert orm.load_code(code["pk"]).with_mpi is False
    with pytest.raises(WorkerDomainError):
        computer_service.create_code({"yaml": code_yaml})


@pytest.mark.usefixtures("aiida_profile_clean")
def test_unreachable_computer_is_not_stored() -> None:
    result = computer_service.create_computer({"fields": {"computer_label": "nowhere", "hostname": "no-such-host.invalid", "transport_type": "core.ssh", "timeout": 5}})
    assert result["status"] == "not_created"
    assert not orm.Computer.collection.find(filters={"label": "nowhere"})
