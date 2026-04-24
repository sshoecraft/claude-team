from pathlib import Path

from claude_team import cluster


def test_cluster_id_stable_for_same_basename(tmp_path: Path) -> None:
    a = tmp_path / "x" / "myproj"
    b = tmp_path / "y" / "myproj"
    a.mkdir(parents=True)
    b.mkdir(parents=True)
    assert cluster.cluster_id_from_root(a) == cluster.cluster_id_from_root(b)


def test_cluster_id_differs_for_different_basenames(tmp_path: Path) -> None:
    a = tmp_path / "myproj"
    b = tmp_path / "otherproj"
    a.mkdir()
    b.mkdir()
    assert cluster.cluster_id_from_root(a) != cluster.cluster_id_from_root(b)


def test_override_changes_cluster_id(tmp_path: Path) -> None:
    a = tmp_path / "myproj"
    a.mkdir()
    without = cluster.cluster_id_from_root(a)
    withov = cluster.cluster_id_from_root(a, override="explicit-name")
    assert without != withov


def test_node_id_is_unique() -> None:
    ids = {cluster.make_node_id() for _ in range(20)}
    assert len(ids) == 20


def test_make_identity_fields(tmp_path: Path) -> None:
    ident = cluster.make_identity(tmp_path)
    assert ident.cluster_id
    assert ident.node_id
    assert ident.hostname
    assert ident.pid > 0
    assert ident.project_root == tmp_path.resolve()
