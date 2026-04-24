from claude_team import dlm


def holder(node: str, mode: str) -> dlm.Holder:
    return dlm.Holder(
        node=node, mode=mode, epoch=1, acquired_at=0.0, pid=1, hostname="h",
    )


def test_compatible_empty() -> None:
    state = dlm.LockState(path="p")
    assert dlm.compatible(state, "me", dlm.LockMode.EXCLUSIVE)
    assert dlm.compatible(state, "me", dlm.LockMode.SHARED)


def test_compatible_self_holder_is_ok() -> None:
    state = dlm.LockState(path="p", holders=[holder("me", "EX")])
    assert dlm.compatible(state, "me", dlm.LockMode.EXCLUSIVE)
    assert dlm.compatible(state, "me", dlm.LockMode.SHARED)


def test_compatible_other_ex_blocks_all() -> None:
    state = dlm.LockState(path="p", holders=[holder("other", "EX")])
    assert not dlm.compatible(state, "me", dlm.LockMode.EXCLUSIVE)
    assert not dlm.compatible(state, "me", dlm.LockMode.SHARED)


def test_compatible_other_shared_allows_shared_blocks_ex() -> None:
    state = dlm.LockState(path="p", holders=[holder("other", "SHARED")])
    assert dlm.compatible(state, "me", dlm.LockMode.SHARED)
    assert not dlm.compatible(state, "me", dlm.LockMode.EXCLUSIVE)


def test_path_key_stable_and_distinct() -> None:
    assert dlm.path_key("src/foo.py") == dlm.path_key("src/foo.py")
    assert dlm.path_key("src/foo.py") != dlm.path_key("src/bar.py")


def test_serialize_roundtrip() -> None:
    state = dlm.LockState(
        path="src/x.py",
        holders=[holder("nodeA", "EX"), holder("nodeB", "SHARED")],
    )
    data = dlm.serialize(state)
    assert dlm.deserialize(data) == state
