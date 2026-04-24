from pathlib import Path

import pytest

from claude_team import project_root


def test_find_walks_up_to_marker(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("")
    nested = tmp_path / "a" / "b" / "c"
    nested.mkdir(parents=True)

    result = project_root.find(nested)
    assert result.path == tmp_path
    assert result.marker == "pyproject.toml"


def test_find_prefers_claude_team_marker(tmp_path: Path) -> None:
    (tmp_path / ".claude-team").mkdir()
    (tmp_path / "pyproject.toml").write_text("")
    result = project_root.find(tmp_path)
    assert result.marker == ".claude-team"


def test_find_falls_back_to_cwd_when_no_marker(tmp_path: Path) -> None:
    # tmp_path has no markers at all
    result = project_root.find(tmp_path)
    assert result.path == tmp_path
    assert result.marker is None


def test_find_locates_git_as_convenience_marker(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    nested = tmp_path / "sub"
    nested.mkdir()
    result = project_root.find(nested)
    assert result.path == tmp_path
    assert result.marker == ".git"
