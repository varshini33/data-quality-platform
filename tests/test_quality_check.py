
from pathlib import Path


def test_quality_checks_module_defines_required_functions():
    from data_quality.quality import checks

    required_functions = [
        "check_not_null",
        "check_unique",
        "check_allowed_values",
        "check_timestamp_order",
        "check_positive",
        "check_non_negative",
    ]

    for function_name in required_functions:
        assert callable(getattr(checks, function_name))


def test_numeric_checks_use_safe_decimal_conversion():
    source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "data_quality"
        / "quality"
        / "checks.py"
    )
    code = source.read_text(encoding="utf-8")

    assert "def check_positive(" in code
    assert "def check_non_negative(" in code
    assert "try_cast" in code
