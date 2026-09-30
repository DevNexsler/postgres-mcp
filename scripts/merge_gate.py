#!/usr/bin/env python3
"""Landing gate: `main` only advances on a tree the full test suite proved.

Maint-Manager #3965. `.github/workflows/build.yml` runs `uv run pytest`, but
GitHub Actions has never run on this fork (`actions/runs` total_count 0, no
check suite on any merge commit), and nothing else ran the suite before a
landing. PR #61 (dd0dfa8) changed `repository.newer_context` to read
`hermes_wakeup_events.envelope` and all 48 tests in
`tests/integration/test_gateway_traffic_isolation.py` landed red on `main`,
where they stayed until #3961.

The gate has two halves:

* `record` writes an attestation after a green full-suite run
  (`scripts/run_test_suite.sh`). It is keyed by the **tree** the run tested,
  not by the commit, because that is the identity the gate needs: a `--no-ff`
  merge of a verified branch into an unmoved base produces a new commit with
  the same tree, so the run that proved the branch has proved the merged tree
  as well. A run on a dirty working tree tested content no commit holds, so it
  attests nothing.
* `pre-push` refuses any push that would advance a protected remote ref to a
  tree with no attestation. Installed as this clone's `pre-push` hook by
  `merge_gate.py install`.

`REQUIRED_RUNS` is the whole policy -- which tier at which scope must have
passed. Demanding another tier later is one entry here plus the `record` call
in that runner; nothing else in this file knows which tiers exist.

The gate binds the landing path this host controls -- the push that advances
`main` -- and cannot see a merge performed through GitHub's own API or merge
button. `LOCAL_LANDING_ONLY` pins that policy; Maint-Manager's
`production-deploy-lane` check reads it and flags GitHub-side merges.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Iterable
from typing import Mapping
from typing import Sequence

# PRs land by a local merge and push so the pre-push hook runs; a GitHub-side
# merge advances `main` with no local push for this gate to judge.
LOCAL_LANDING_ONLY = True
# The refs a push may not advance unverified. Remote-side names, as git writes
# them on the hook's stdin.
PROTECTED_REMOTE_REFS = ("refs/heads/main",)
# What a landing must have proved: `uv run pytest` over the whole suite rather
# than a focused selection.
REQUIRED_RUNS = (("pytest", "full"),)
# Per-clone state, beside the object store so every worktree of a clone shares
# it: the worktree that ran the suite is rarely the one that pushes.
ATTESTATION_DIRNAME = "postgres-mcp-merge-gate"
OVERRIDE_ENV = "POSTGRES_MCP_MERGE_GATE_ALLOW_UNVERIFIED"
HOOKS_PATH = "scripts/githooks"
SUITE_RUNNER = "scripts/run_test_suite.sh"

ALLOWED, REFUSED, UNUSABLE = 0, 1, 2


class GateError(Exception):
    """The gate could not be evaluated, so nothing about the push is known."""


@dataclass(frozen=True)
class Attestation:
    """One green tier run over one tree."""

    tree: str
    tier: str
    scope: str
    commit: str
    run_id: str
    recorded_at: str

    @property
    def run(self) -> tuple[str, str]:
        return (self.tier, self.scope)

    def payload(self) -> dict[str, str]:
        return {
            "tree": self.tree,
            "tier": self.tier,
            "scope": self.scope,
            "commit": self.commit,
            "run_id": self.run_id,
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> Attestation:
        return cls(
            tree=str(payload["tree"]),
            tier=str(payload["tier"]),
            scope=str(payload["scope"]),
            commit=str(payload.get("commit", "")),
            run_id=str(payload.get("run_id", "")),
            recorded_at=str(payload.get("recorded_at", "")),
        )


@dataclass(frozen=True)
class PushUpdate:
    """One line of the `pre-push` hook's stdin."""

    local_ref: str
    local_sha: str
    remote_ref: str
    remote_sha: str

    @property
    def is_delete(self) -> bool:
        return set(self.local_sha) == {"0"}


@dataclass(frozen=True)
class Landing:
    """A push update resolved to the content it would put on a protected ref."""

    remote_ref: str
    commit: str
    tree: str


@dataclass(frozen=True)
class Decision:
    """Why a push may not proceed; empty means it may."""

    refusals: tuple[str, ...]

    @property
    def allowed(self) -> bool:
        return not self.refusals


def parse_push_updates(text: str) -> list[PushUpdate]:
    """The ref updates git offered the hook, one per non-empty stdin line."""
    updates = []
    for line in text.splitlines():
        fields = line.split()
        if not fields:
            continue
        if len(fields) != 4:
            raise GateError(f"unreadable pre-push update: {line!r}")
        updates.append(PushUpdate(*fields))
    return updates


def protected_updates(
    updates: Iterable[PushUpdate],
    protected_refs: Sequence[str] = PROTECTED_REMOTE_REFS,
) -> list[PushUpdate]:
    """The updates this gate judges: content landing on a protected ref.

    A deletion lands no content, so it is not this gate's business; a ref that
    is not protected is nobody's landing.
    """
    return [update for update in updates if update.remote_ref in protected_refs and not update.is_delete]


def decide(
    landings: Sequence[Landing],
    attestations: Mapping[str, Sequence[Attestation]],
    required_runs: Sequence[tuple[str, str]] = REQUIRED_RUNS,
) -> Decision:
    """Refuse every landing whose tree is missing a required green run.

    `attestations` is keyed by tree, so a landing is judged on the content it
    would put on the branch and never on how that content was reached.
    """
    refusals = []
    for landing in landings:
        proved = {record.run for record in attestations.get(landing.tree, ())}
        missing = [run for run in required_runs if run not in proved]
        if missing:
            refusals.append(
                f"{landing.remote_ref} would land tree {landing.tree}"
                f" (commit {landing.commit[:12]}) with no green " + ", ".join(f"{tier} tier ({scope} scope)" for tier, scope in missing)
            )
    return Decision(tuple(refusals))


def refusal_report(decision: Decision) -> str:
    """What an operator has to do about a refused push."""
    lines = ["refusing to push: the full test suite has not passed on this tree"]
    lines += [f"  {refusal}" for refusal in decision.refusals]
    lines += [
        "",
        "run the suite on exactly this tree, then push again:",
        f"  ./{SUITE_RUNNER}",
        "A run over a dirty working tree records nothing -- commit first.",
        "A focused run (any pytest argument) proves a selection, not the suite.",
        "",
        f"To land without it, state why: {OVERRIDE_ENV}='<reason>' git push ...",
    ]
    return "\n".join(lines)


def git(*args: str, cwd: Path | None = None) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:  # pragma: no cover - git is a hard dependency
        raise GateError(f"could not run git: {exc}") from exc
    if result.returncode != 0:
        raise GateError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def repository_root(cwd: Path | None = None) -> Path:
    return Path(git("rev-parse", "--show-toplevel", cwd=cwd))


def attestation_dir(cwd: Path | None = None) -> Path:
    """Where this clone keeps its attestations.

    The common dir rather than the worktree's own: a maintenance worktree runs
    the suite, the checkout that owns `main` pushes, and both are the same
    clone.
    """
    common = git("rev-parse", "--path-format=absolute", "--git-common-dir", cwd=cwd)
    return Path(common) / ATTESTATION_DIRNAME


def tree_of(revision: str, cwd: Path | None = None) -> str:
    return git("rev-parse", f"{revision}^{{tree}}", cwd=cwd)


def working_tree_dirt(cwd: Path | None = None) -> str:
    return git("status", "--porcelain", cwd=cwd)


def load_attestations(directory: Path, trees: Iterable[str]) -> dict[str, list[Attestation]]:
    """Every attestation on record for the given trees.

    An unreadable record is treated as absent: the gate fails closed, and a
    corrupt file must never be the reason a landing is allowed.
    """
    found: dict[str, list[Attestation]] = {}
    for tree in trees:
        records = []
        for path in sorted(directory.glob(f"{tree}.*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                record = Attestation.from_payload(payload)
            except (OSError, ValueError, KeyError):
                continue
            if record.tree == tree:
                records.append(record)
        found[tree] = records
    return found


def write_attestation(directory: Path, record: Attestation) -> Path:
    """Persist one attestation atomically, one file per (tree, tier)."""
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{record.tree}.{record.tier}.json"
    handle, raw_tmp = tempfile.mkstemp(prefix=f".{record.tree}.", suffix=".tmp", dir=directory)
    tmp = Path(raw_tmp)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(record.payload(), stream, sort_keys=True)
            stream.write("\n")
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    return target


def record_command(args: argparse.Namespace) -> int:
    """Attest the tree a green tier just ran over."""
    root = repository_root()
    dirt = working_tree_dirt(cwd=root)
    if dirt:
        print(
            "landing gate: working tree is dirty, so this run proved no commit's tree; recording nothing",
            file=sys.stderr,
        )
        return ALLOWED
    record = Attestation(
        tree=tree_of("HEAD", cwd=root),
        tier=args.tier,
        scope=args.scope,
        commit=git("rev-parse", "HEAD", cwd=root),
        run_id=args.run_id or os.environ.get("MAINT_DOCKER_RUN_ID", ""),
        recorded_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    write_attestation(attestation_dir(cwd=root), record)
    print(f"landing gate: {record.tier} tier ({record.scope} scope) attested for tree {record.tree}")
    return ALLOWED


def pre_push_command(args: argparse.Namespace) -> int:
    """Judge the push git is about to make, from its stdin."""
    updates = protected_updates(parse_push_updates(sys.stdin.read()))
    if not updates:
        return ALLOWED
    root = repository_root()
    landings = [
        Landing(
            remote_ref=update.remote_ref,
            commit=update.local_sha,
            tree=tree_of(update.local_sha, cwd=root),
        )
        for update in updates
    ]
    decision = decide(
        landings,
        load_attestations(attestation_dir(cwd=root), {landing.tree for landing in landings}),
    )
    if decision.allowed:
        return ALLOWED
    reason = os.environ.get(OVERRIDE_ENV, "").strip()
    if reason:
        print(
            f"landing gate: overridden ({reason})\n" + refusal_report(decision),
            file=sys.stderr,
        )
        return ALLOWED
    print(refusal_report(decision), file=sys.stderr)
    return REFUSED


def common_hooks_dir(cwd: Path | None = None) -> Path:
    """The hooks directory every worktree of this clone shares by default."""
    common = git("rev-parse", "--path-format=absolute", "--git-common-dir", cwd=cwd)
    return Path(common) / "hooks"


def effective_hook(name: str, cwd: Path) -> Path:
    """The file git runs as hook `name` in the worktree at `cwd`."""
    return Path(git("rev-parse", "--path-format=absolute", "--git-path", f"hooks/{name}", cwd=cwd)).resolve()


def install_command(args: argparse.Namespace) -> int:
    """Install the gate as this clone's shared `pre-push` hook.

    The hook lives in the clone's common hooks directory, pinned by an absolute
    `core.hooksPath` (as Graphiti-Factbook #3427). That setting is clone-wide config other tools
    rewrite: agent worktree setup has pointed it at `.git/hooks`, and now turns
    a relative value into an absolute path under whichever checkout the
    session started in -- a maintenance worktree that is later reaped, leaving
    git no hook to run. The common directory is git's default and the one that
    setup names, so the gate survives it. The hook still judges with the
    pushing worktree's own `scripts/merge_gate.py` and is inert where that
    does not exist.
    """
    root = repository_root()
    source = root / HOOKS_PATH / "pre-push"
    if not os.access(source, os.X_OK):
        print(f"landing gate: {source} is missing or not executable", file=sys.stderr)
        return UNUSABLE
    target = common_hooks_dir(cwd=root) / "pre-push"
    if effective_hook("pre-push", cwd=root) != target.resolve():
        current = git("config", "--default", "", "--get", "core.hooksPath", cwd=root)
        if not args.force:
            print(
                f"landing gate: core.hooksPath is already {current!r}, so git would not run {target}; re-run with --force to replace it",
                file=sys.stderr,
            )
            return UNUSABLE
    if target.exists() and target.read_bytes() != source.read_bytes() and not args.force:
        print(
            f"landing gate: {target} already exists and is not the landing gate; re-run with --force to replace it",
            file=sys.stderr,
        )
        return UNUSABLE
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    git("config", "--local", "core.hooksPath", str(target.parent.resolve()), cwd=root)
    print(f"landing gate: installed {target}")
    return ALLOWED


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    commands = parser.add_subparsers(dest="command", required=True)

    record = commands.add_parser("record", help="attest the tree a green tier ran over")
    record.add_argument("--tier", required=True)
    record.add_argument("--scope", required=True)
    record.add_argument("--run-id", default="")
    record.set_defaults(handler=record_command)

    pre_push = commands.add_parser("pre-push", help="judge a push (git pre-push hook)")
    pre_push.add_argument("remote", nargs="?", default="")
    pre_push.add_argument("url", nargs="?", default="")
    pre_push.set_defaults(handler=pre_push_command)

    install = commands.add_parser("install", help="install the hook in this clone")
    install.add_argument("--force", action="store_true")
    install.set_defaults(handler=install_command)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except GateError as exc:
        print(f"landing gate: {exc}", file=sys.stderr)
        return UNUSABLE


if __name__ == "__main__":
    raise SystemExit(main())
