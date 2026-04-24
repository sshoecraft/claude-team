from pathlib import Path

from claude_team import overlay


def test_snapshot_missing_file(tmp_path: Path) -> None:
    snap = overlay.snapshot_path(tmp_path, "no_such.txt", max_size=1024)
    assert snap.exists is False
    assert snap.content is None


def test_snapshot_small_file(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("hello\n")
    snap = overlay.snapshot_path(tmp_path, "a.txt", max_size=1024)
    assert snap.exists is True
    assert snap.content == b"hello\n"
    assert snap.sha256 == overlay.sha256_bytes(b"hello\n")


def test_snapshot_oversize_is_hash_only(tmp_path: Path) -> None:
    big = b"x" * 100
    (tmp_path / "big.bin").write_bytes(big)
    snap = overlay.snapshot_path(tmp_path, "big.bin", max_size=10)
    assert snap.exists is True
    assert snap.content is None
    assert snap.sha256 == overlay.sha256_bytes(big)


def test_compute_diff_unified_for_text() -> None:
    pre = ("".join(f"line{i}\n" for i in range(40))).encode()
    post_lines = ["line" + str(i) + "\n" for i in range(40)]
    post_lines[20] = "CHANGED\n"
    post = ("".join(post_lines)).encode()
    snap = overlay.Snapshot(path="x", exists=True, sha256=overlay.sha256_bytes(pre), content=pre)
    fmt, payload = overlay.compute_diff_payload(snap, post, overlay.sha256_bytes(post), 100_000)
    assert fmt == overlay.DiffFormat.UNIFIED
    applied = overlay.apply_diff_to_text(pre.decode(), payload)
    assert applied.encode() == post


def test_compute_diff_binary_falls_back() -> None:
    pre = b"header\x00data\x00more"
    post = b"header\x00data\x00changed"
    snap = overlay.Snapshot(path="x", exists=True, sha256=overlay.sha256_bytes(pre), content=pre)
    fmt, payload = overlay.compute_diff_payload(snap, post, overlay.sha256_bytes(post), 100_000)
    assert fmt == overlay.DiffFormat.BINARY_INVAL
    assert payload == ""


def test_compute_diff_replaced_for_tiny_files() -> None:
    pre = b"a\n"
    post = b"b\n"
    snap = overlay.Snapshot(path="x", exists=True, sha256=overlay.sha256_bytes(pre), content=pre)
    fmt, _ = overlay.compute_diff_payload(snap, post, overlay.sha256_bytes(post), 100_000)
    # With tiny input, unified diff is larger than the content; falls back.
    assert fmt == overlay.DiffFormat.REPLACED


def test_envelope_roundtrip() -> None:
    env = overlay.EventEnvelope(
        type="diff", path="src/foo.py", sender="nodeA",
        payload={"pre_sha256": "aaa", "post_sha256": "bbb", "format": "unified", "payload": "xyz"},
    )
    back = overlay.EventEnvelope.from_bytes(env.to_bytes())
    assert back == env
