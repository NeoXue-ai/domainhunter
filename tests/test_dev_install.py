from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


def test_dev_install_uses_the_directory_containing_the_script_as_the_project_root(
    tmp_path: Path,
) -> None:
    """The documented root-level invocation installs the package successfully."""
    repository = tmp_path / "domain hunter"
    repository.mkdir()
    script = Path(__file__).resolve().parents[1] / "dev_install.sh"
    shutil.copy2(script, repository / "dev_install.sh")
    (repository / "pyproject.toml").write_text(
        """\
[project]
name = "domainhunter"
version = "0.0.0"
requires-python = ">=3.12"

[project.scripts]
domainhunter = "domainhunter.cli:main"

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"
"""
    )
    package = repository / "src" / "domainhunter"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text('__version__ = "0.0.0"\n')
    (package / "cli.py").write_text(
        'import sys\n\ndef main() -> None:\n    print(" ".join(sys.argv[1:]) or "ready")\n'
    )
    subprocess.run([sys.executable, "-m", "venv", ".venv"], cwd=repository, check=True)

    result = subprocess.run(
        ["bash", "dev_install.sh"], cwd=repository, capture_output=True, text=True
    )

    assert result.returncode == 0, result.stderr
    assert "ok:" in result.stdout

    cli_result = subprocess.run(
        [str(repository / ".venv" / "bin" / "domainhunter"), "first-pass"],
        cwd=repository,
        capture_output=True,
        text=True,
    )
    assert cli_result.returncode == 0, cli_result.stderr
    assert cli_result.stdout == "first-pass\n"

    if sys.platform == "darwin":
        site_packages = subprocess.check_output(
            [
                str(repository / ".venv" / "bin" / "python"),
                "-c",
                "import sysconfig; print(sysconfig.get_paths()['purelib'])",
            ],
            text=True,
        ).strip()
        subprocess.run(["chflags", "hidden", f"{site_packages}/domainhunter.pth"], check=True)

        recovered_result = subprocess.run(
            [str(repository / ".venv" / "bin" / "domainhunter"), "recovered"],
            cwd=repository,
            capture_output=True,
            text=True,
        )
        assert recovered_result.returncode == 0, recovered_result.stderr
        assert recovered_result.stdout == "recovered\n"

        reinstall_result = subprocess.run(
            ["bash", "dev_install.sh"], cwd=repository, capture_output=True, text=True
        )
        assert reinstall_result.returncode == 0, reinstall_result.stderr

        refreshed_result = subprocess.run(
            [str(repository / ".venv" / "bin" / "domainhunter"), "refreshed"],
            cwd=repository,
            capture_output=True,
            text=True,
        )
        assert refreshed_result.returncode == 0, refreshed_result.stderr
        assert refreshed_result.stdout == "refreshed\n"
