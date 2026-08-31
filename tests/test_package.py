from webradar_v2 import __version__


def test_package_exposes_initial_version() -> None:
    assert __version__ == "0.1.0"
