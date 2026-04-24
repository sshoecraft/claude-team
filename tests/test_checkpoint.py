from pathlib import Path

from claude_team import checkpoint, manifest


def test_take_write_read_roundtrip(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "foo.py").write_text("hello\n")
    (tmp_path / "src" / "bar.py").write_text("world\n")

    meta, mf = checkpoint.take(
        tmp_path, cluster_id="abc", created_by="nodeA", previous=None,
    )
    assert meta.checkpoint_number == 1
    checkpoint.write(tmp_path, meta, mf)

    loaded = checkpoint.read(tmp_path)
    assert loaded is not None
    loaded_meta, loaded_mf = loaded
    assert loaded_meta == meta
    assert loaded_mf.entries == mf.entries


def test_take_increments_number(tmp_path: Path) -> None:
    (tmp_path / "x.txt").write_text("a")
    meta1, _ = checkpoint.take(tmp_path, "c", "n", previous=None)
    (tmp_path / "x.txt").write_text("b")
    meta2, _ = checkpoint.take(tmp_path, "c", "n", previous=meta1)
    assert meta2.checkpoint_number == 2
    assert meta2.checkpoint_id != meta1.checkpoint_id


def test_verify_against_detects_divergence(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("A")
    (tmp_path / "b.txt").write_text("B")
    mf = manifest.compute(tmp_path)

    (tmp_path / "a.txt").write_text("A modified")
    (tmp_path / "c.txt").write_text("C")
    current = manifest.compute(tmp_path)

    result = checkpoint.verify_against(current, mf)
    assert not result.matches
    assert "a.txt" in result.differing
    assert "c.txt" in result.extra_local
    assert "b.txt" not in result.all_divergent_paths
