from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from docbt.cli import cli
from docbt.runner import run_project
from docbt.synth import generate_invoices


def test_init_creates_runnable_project(tmp_path: Path) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
        result = runner.invoke(cli, ["init", "scaffold"], catch_exceptions=False)
        assert result.exit_code == 0, result.output

        target = Path(cwd) / "scaffold"
        assert (target / "docbt_project.yml").exists()
        assert (target / "profiles.yml").exists()
        assert (target / "sources" / "invoices.yml").exists()
        assert (target / "models" / "raw_invoices.yml").exists()
        project_text = (target / "docbt_project.yml").read_text()
        profiles_text = (target / "profiles.yml").read_text()
        assert "name: scaffold" in project_text
        assert "profile: scaffold" in project_text
        assert "schema: scaffold" in profiles_text
        assert "__PROJECT_NAME__" not in project_text
        assert "__PROJECT_NAME__" not in profiles_text


def test_init_with_pdf_template(tmp_path: Path) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
        result = runner.invoke(cli, ["init", "p", "--template", "pdf"])
        assert result.exit_code == 0, result.output
        target = Path(cwd) / "p"
        assert (target / "models" / "raw_pdf_text.yml").exists()
        assert "backend: pdf" in (target / "models" / "raw_pdf_text.yml").read_text()
        assert "default_backend: pdf" in (target / "docbt_project.yml").read_text()


def test_init_with_markdown_template(tmp_path: Path) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
        result = runner.invoke(cli, ["init", "p", "--template", "markdown"])
        assert result.exit_code == 0, result.output
        target = Path(cwd) / "p"
        assert (target / "models" / "raw_documents.yml").exists()


def test_init_with_html_template(tmp_path: Path) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
        result = runner.invoke(cli, ["init", "p", "--template", "html"])
        assert result.exit_code == 0, result.output
        target = Path(cwd) / "p"
        assert (target / "models" / "raw_pages.yml").exists()


def test_init_unknown_template_rejected(tmp_path: Path) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["init", "p", "--template", "bogus"])
        assert result.exit_code != 0


def test_init_refuses_existing_dir(tmp_path: Path) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        Path("existing").mkdir()
        result = runner.invoke(cli, ["init", "existing"])
        assert result.exit_code != 0
        assert "already exists" in result.output


def test_show_prints_rows(tmp_path: Path, example_project_dir: Path) -> None:
    import shutil

    dst = tmp_path / "project"
    shutil.copytree(
        example_project_dir,
        dst,
        ignore=shutil.ignore_patterns("data", "target", "__pycache__"),
    )
    generate_invoices(8, dst / "data" / "invoices", seed=1)
    run_project(dst)

    runner = CliRunner()
    result = runner.invoke(cli, ["--project-dir", str(dst), "show", "raw_invoices", "--limit", "3"])
    assert result.exit_code == 0, result.output
    assert "shape: (3," in result.output  # polars repr always starts with shape


def test_show_missing_db(tmp_path: Path, example_project_dir: Path) -> None:
    import shutil

    dst = tmp_path / "project"
    shutil.copytree(
        example_project_dir,
        dst,
        ignore=shutil.ignore_patterns("data", "target", "__pycache__"),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["--project-dir", str(dst), "show", "raw_invoices"])
    assert result.exit_code != 0
    assert "No database" in result.output


def test_show_unknown_model(tmp_path: Path, example_project_dir: Path) -> None:
    import shutil

    dst = tmp_path / "project"
    shutil.copytree(
        example_project_dir,
        dst,
        ignore=shutil.ignore_patterns("data", "target", "__pycache__"),
    )
    generate_invoices(2, dst / "data" / "invoices", seed=1)
    run_project(dst)

    runner = CliRunner()
    result = runner.invoke(cli, ["--project-dir", str(dst), "show", "no_such_model"])
    assert result.exit_code != 0
    assert "Could not query" in result.output
