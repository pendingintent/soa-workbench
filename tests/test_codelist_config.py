"""Tests for src/soa_builder/web/codelist_config.py."""

import importlib

from soa_builder.web import codelist_config


_DEFAULTS = {
    "CODELIST_EPOCH_TYPE": ("EPOCH_TYPE_CODELIST", "C99079"),
    "CODELIST_ARM_TYPE": ("ARM_TYPE_CODELIST", "C174222"),
    "CODELIST_ARM_DATA_ORIGIN_TYPE": (
        "ARM_DATA_ORIGIN_TYPE_CODELIST",
        "C188727",
    ),
    "CODELIST_ROLE_TYPE": ("ROLE_TYPE_CODELIST", "C215480"),
    "CODELIST_ORGANIZATION_TYPE": ("ORGANIZATION_TYPE_CODELIST", "C215480"),
    "CODELIST_AMENDMENT_REASON": ("AMENDMENT_REASON_CODELIST", "C207415"),
    "CODELIST_AMENDMENT_IMPACT_TYPE": (
        "AMENDMENT_IMPACT_TYPE_CODELIST",
        "C215481",
    ),
    "CODELIST_GEOGRAPHIC_SCOPE_TYPE": (
        "GEOGRAPHIC_SCOPE_TYPE_CODELIST",
        "C207412",
    ),
    "CODELIST_GOVERNANCE_DATE_TYPE": (
        "GOVERNANCE_DATE_TYPE_CODELIST",
        "C207413",
    ),
    "CODELIST_STUDY_TITLE_TYPE": ("STUDY_TITLE_TYPE_CODELIST", "C207419"),
    "CODELIST_INTERVENTION_ROLE": ("INTERVENTION_ROLE_CODELIST", "C207417"),
    "CODELIST_INTERVENTION_TYPE": ("INTERVENTION_TYPE_CODELIST", "C99078"),
    "CODELIST_INTERVENTION_UNIT": ("INTERVENTION_UNIT_CODELIST", "C66781"),
    "CODELIST_OBJECTIVE_LEVEL": ("OBJECTIVE_LEVEL_CODELIST", "C188725"),
    "CODELIST_ENDPOINT_LEVEL": ("ENDPOINT_LEVEL_CODELIST", "C188726"),
    "CODELIST_ENCOUNTER_ENVIRONMENTAL_SETTING": (
        "ENCOUNTER_ENVIRONMENTAL_SETTING_CODELIST",
        "C127262",
    ),
    "CODELIST_ENCOUNTER_CONTACT_MODE": (
        "ENCOUNTER_CONTACT_MODE_CODELIST",
        "C171445",
    ),
    "CODELIST_TIMING_TYPE": ("TIMING_TYPE_CODELIST", "C201264"),
    "CODELIST_TIMING_RELATIVE_TO_FROM": (
        "TIMING_RELATIVE_TO_FROM_CODELIST",
        "C201265",
    ),
}


def test_defaults_match_prior_hardcoded_values(monkeypatch):
    """Unset env vars must fall back to the values previously hardcoded."""
    for env_var in _DEFAULTS:
        monkeypatch.delenv(env_var, raising=False)
    importlib.reload(codelist_config)
    try:
        for env_var, (attr, default) in _DEFAULTS.items():
            assert getattr(codelist_config, attr) == default
    finally:
        importlib.reload(codelist_config)


def test_env_var_overrides_default(monkeypatch):
    """Setting a CODELIST_* env var overrides the built-in default."""
    monkeypatch.setenv("CODELIST_ROLE_TYPE", "C999999")
    importlib.reload(codelist_config)
    try:
        assert codelist_config.ROLE_TYPE_CODELIST == "C999999"
        # An unrelated constant remains at its default.
        assert codelist_config.ARM_TYPE_CODELIST == "C174222"
    finally:
        monkeypatch.delenv("CODELIST_ROLE_TYPE", raising=False)
        importlib.reload(codelist_config)


def test_all_constants_are_non_empty_strings():
    for _, (attr, _default) in _DEFAULTS.items():
        value = getattr(codelist_config, attr)
        assert isinstance(value, str)
        assert value
