"""The landing gate that PR #61 (dd0dfa8) did not have (#3965).

`.github/workflows/build.yml` runs `uv run pytest`, but GitHub Actions has never
run on this fork, so nothing ran the suite before a landing. dd0dfa8 changed
`repository.newer_context` and all 48 tests in
`tests/integration/test_gateway_traffic_isolation.py` landed red on `main`.

The reproduction here is that landing sequence: a base whose suite was green, a
commit that changes the tree without a fresh green suite, and a push of `main`.
Before the gate the push succeeded, which is exactly how `main` went red; it must
now be refused, and must stay allowed for the sequence where the suite did run
on the tree being landed.
"""

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GATE = REPO_ROOT / "scripts" / "merge_gate.py"
HOOK = REPO_ROOT / "scripts" / "githooks" / "pre-push"
RUNNER = REPO_ROOT / "scripts" / "run_test_suite.sh"

# Every git call in the scratch clone ignores the host's global and system
# config: a global core.hooksPath would otherwise decide which hook runs.
GIT_ENV = {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}


def _load_gate():
    spec = importlib.util.spec_from_file_location("merge_gate", GATE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # Its dataclasses resolve annotations through sys.modules while executing.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _run(cwd: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(args), cwd=cwd, text=True, capture_output=True, check=False, env={**os.environ, **GIT_ENV, **(env or {})})


def _git(repository: Path, *args: str) -> str:
    result = _run(repository, "git", *args)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def _gate(repository: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return _run(repository, "python3", str(repository / "scripts" / "merge_gate.py"), *args, env=env)


def _commit(repository: Path, name: str, body: str, message: str) -> str:
    (repository / name).write_text(body, encoding="utf-8")
    _git(repository, "add", "-A")
    _git(repository, "commit", "-q", "-m", message)
    return _git(repository, "rev-parse", "HEAD")


def _push(repository: Path, *refspecs: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return _run(repository, "git", "push", "origin", *(refspecs or ("main",)), env=env)


def _remote_main(repository: Path) -> str:
    return _git(repository, "ls-remote", "origin", "refs/heads/main").split()[0]


def _attest(repository: Path) -> None:
    result = _gate(repository, "record", "--tier", "pytest", "--scope", "full")
    assert result.returncode == 0, result.stderr


@pytest.fixture
def clone(tmp_path: Path) -> Path:
    """A repository carrying the gate, its hook installed, and a remote.

    A scratch clone rather than this one: the subject is what `git push` does,
    and the only honest way to observe that is to make a real push.
    """
    repository = tmp_path / "clone"
    (repository / "scripts" / "githooks").mkdir(parents=True)
    shutil.copy2(GATE, repository / "scripts" / "merge_gate.py")
    shutil.copy2(HOOK, repository / "scripts" / "githooks" / "pre-push")
    shutil.copy2(RUNNER, repository / "scripts" / "run_test_suite.sh")
    _git(tmp_path, "init", "-q", "-b", "main", str(repository))
    _git(repository, "config", "user.email", "gate@example.invalid")
    _git(repository, "config", "user.name", "gate")
    _git(tmp_path, "init", "-q", "--bare", str(tmp_path / "remote.git"))
    _git(repository, "remote", "add", "origin", str(tmp_path / "remote.git"))
    _commit(repository, "repository.py", "base\n", "base")
    installed = _gate(repository, "install")
    assert installed.returncode == 0, installed.stderr
    _attest(repository)
    assert _push(repository).returncode == 0
    return repository


def test_main_cannot_land_a_tree_the_suite_never_ran_over(clone: Path):
    """The dd0dfa8 landing: a suite-breaking change merged without the suite."""
    base = _remote_main(clone)
    _commit(clone, "repository.py", "newer_context reads hermes_wakeup_events.envelope\n", "Stale check by recipient")

    result = _push(clone)

    assert result.returncode != 0
    assert "refusing to push" in result.stderr
    assert _git(clone, "rev-parse", "HEAD^{tree}") in result.stderr
    assert "./scripts/run_test_suite.sh" in result.stderr
    assert _remote_main(clone) == base


def test_main_lands_a_tree_the_suite_passed_on(clone: Path):
    head = _commit(clone, "repository.py", "fixed\n", "Fix")
    _attest(clone)

    assert _push(clone).returncode == 0
    assert _remote_main(clone) == head


def test_no_ff_merge_of_a_verified_branch_lands_on_the_branch_run(clone: Path):
    """The reconcile lane's shape: verify the branch, merge it locally, push."""
    _git(clone, "checkout", "-q", "-b", "maint/1")
    _commit(clone, "repository.py", "fixed\n", "Fix")
    _attest(clone)
    _git(clone, "checkout", "-q", "main")
    _git(clone, "merge", "-q", "--no-ff", "-m", "Merge maint/1", "maint/1")

    assert _push(clone).returncode == 0


def test_red_suite_records_nothing_so_the_landing_stays_refused(clone: Path, tmp_path: Path):
    """`run_test_suite.sh` attests only a green run; a red one leaves `main` blocked."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_uv = bin_dir / "uv"
    fake_uv.write_text('#!/usr/bin/env bash\nexit "${FAKE_PYTEST_STATUS}"\n', encoding="utf-8")
    fake_uv.chmod(0o755)
    env = {"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
    _commit(clone, "repository.py", "newer_context reads hermes_wakeup_events.envelope\n", "Stale check by recipient")
    runner = str(clone / "scripts" / "run_test_suite.sh")

    red = _run(clone, runner, env={**env, "FAKE_PYTEST_STATUS": "1"})
    assert red.returncode == 1
    assert _push(clone).returncode != 0

    focused = _run(clone, runner, "tests/unit", env={**env, "FAKE_PYTEST_STATUS": "0"})
    assert focused.returncode == 0
    assert _push(clone).returncode != 0

    green = _run(clone, runner, env={**env, "FAKE_PYTEST_STATUS": "0"})
    assert green.returncode == 0, green.stderr
    assert "attested for tree" in green.stdout
    assert _push(clone).returncode == 0


def test_a_run_over_a_dirty_tree_attests_nothing(clone: Path):
    _commit(clone, "repository.py", "change\n", "Change")
    (clone / "repository.py").write_text("uncommitted\n", encoding="utf-8")

    result = _gate(clone, "record", "--tier", "pytest", "--scope", "full")

    assert result.returncode == 0
    assert "recording nothing" in result.stderr
    assert _push(clone).returncode != 0


def test_other_branches_push_unverified(clone: Path):
    _git(clone, "checkout", "-q", "-b", "maint/1")
    _commit(clone, "repository.py", "work in progress\n", "WIP")

    assert _push(clone, "maint/1").returncode == 0


def test_override_needs_a_stated_reason(clone: Path):
    _commit(clone, "repository.py", "change\n", "Change")

    blank = _push(clone, env={"POSTGRES_MCP_MERGE_GATE_ALLOW_UNVERIFIED": "  "})
    assert blank.returncode != 0

    stated = _push(clone, env={"POSTGRES_MCP_MERGE_GATE_ALLOW_UNVERIFIED": "docker is down, #1234"})
    assert stated.returncode == 0
    assert "overridden (docker is down, #1234)" in stated.stderr


def test_landing_policy_is_local_only_and_the_runner_runs_the_ci_suite():
    """Maint-Manager's production-deploy-lane check reads this pin to flag GitHub-side merges."""
    gate = _load_gate()

    assert gate.LOCAL_LANDING_ONLY is True
    assert "refs/heads/main" in gate.PROTECTED_REMOTE_REFS
    assert gate.REQUIRED_RUNS == (("pytest", "full"),)
    assert "LOCAL_LANDING_ONLY = True" in GATE.read_text(encoding="utf-8")
    assert "uv run pytest" in (REPO_ROOT / ".github" / "workflows" / "build.yml").read_text(encoding="utf-8")
    assert 'uv run pytest "$@"' in RUNNER.read_text(encoding="utf-8")
    assert os.access(HOOK, os.X_OK) and os.access(RUNNER, os.X_OK)
