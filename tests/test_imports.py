"""Stage S0 smoke tests: every scaffolded module imports cleanly."""

import importlib

import pytest

MODULES = [
    "backend",
    "backend.gtfs",
    "backend.gtfs.errors",
    "backend.gtfs.types",
    "backend.gtfs.csvio",
    "backend.gtfs.extractor",
    "backend.gtfs.parser",
    "backend.trimet",
    "backend.trimet.config",
    "backend.trimet.maxfilter",
    "backend.trimet.catalog",
    "backend.trimet.dataset",
    "backend.trimet.load_cli",
]


@pytest.mark.parametrize("module_path", MODULES)
def test_module_imports(module_path: str) -> None:
    module = importlib.import_module(module_path)
    assert module is not None
