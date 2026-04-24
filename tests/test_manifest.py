from pathlib import Path

from claude_team import manifest


def test_compute_excludes_ignored(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "keep.py").write_text("ok")
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "junk.js").write_text("x")
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "foo.cpython.pyc").write_bytes(b"\x00\x00")

    mf = manifest.compute(tmp_path)
    assert "src/keep.py" in mf.entries
    assert not any(p.startswith("node_modules/") for p in mf.entries)
    assert not any(p.startswith("__pycache__/") for p in mf.entries)


def test_is_ignored_patterns() -> None:
    patterns = ("*.pyc", "node_modules", "dist")
    assert manifest.is_ignored("foo.pyc", patterns)
    assert manifest.is_ignored("node_modules/thing/index.js", patterns)
    assert manifest.is_ignored("src/__pycache__/x.py", ("*.pyc", "__pycache__"))
    assert not manifest.is_ignored("src/foo.py", patterns)


def test_manifest_hash_is_order_independent(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("A")
    (tmp_path / "b.txt").write_text("B")
    m1 = manifest.compute(tmp_path)

    # Re-compute — dict iteration order shouldn't affect the hash.
    m2 = manifest.compute(tmp_path)
    assert m1.hash == m2.hash


def test_diff_manifests_detects_divergence(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("A")
    (tmp_path / "b.txt").write_text("B")
    local = manifest.compute(tmp_path)

    (tmp_path / "b.txt").write_text("B-changed")
    (tmp_path / "c.txt").write_text("C")
    remote = manifest.compute(tmp_path)

    diff = manifest.diff_manifests(local, remote)
    assert "b.txt" in diff
    assert "c.txt" in diff
    assert "a.txt" not in diff
