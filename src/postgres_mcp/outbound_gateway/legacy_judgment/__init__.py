"""The gateway's send judgment exactly as deployed at bf41be6 (2026-09-26).

FROZEN. Nothing in the gateway runtime imports this package. It is the parity
oracle for the one-stale-question simplification: the traffic-control
staleness block, the preflight `newer_inbound` / recipient / context checks
and the newer-outbound question, side by side with the current service, so
tests/unit/outbound_gateway/test_parity_one_question.py can run every
scenario through both and declare each intended difference by name.

Only shared data types and I/O helpers (models, record, context, adapters,
recovery) come from the live package; every decision module is a copy.
It also stands in for the pre-split oracle (legacy_service.py, retired): the
recovery parity compares against it. Delete this package once the
simplification has been in production long enough to retire the comparison.
"""

from .service import OutboundActionService as LegacyJudgmentService

__all__ = ["LegacyJudgmentService"]
