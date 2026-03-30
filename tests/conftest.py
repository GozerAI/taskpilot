"""Shared test fixtures for taskpilot."""

import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def _unlock_license_gate():
    """Patch the license gate singleton so all features are allowed during tests."""
    with patch("taskpilot.licensing.license_gate.check_feature", return_value=True):
        yield
