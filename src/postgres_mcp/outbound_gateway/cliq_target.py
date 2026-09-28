"""Pure Cliq target classification.

Agent Email Server exposes two, non-interchangeable Cliq post tools:

- ``cliq_channel_bot_post`` takes ``channel_unique_name`` -- a short slug
  like ``"maintenance"`` -- and refuses a channel display name, ``#name``,
  numeric chat id, or ``CT_*`` conversation id.
- ``cliq_chat_post`` takes ``chat_id`` matching AES's
  ``CLIQ_CHAT_ID_PATTERN`` (see Agent-Email-Server's
  ``src/tools/cliq-chat-id.ts``): a numeric id of at least
  ``MIN_CLIQ_NUMERIC_CHAT_ID_DIGITS`` digits, or a ``CT_<chat>_<user>``
  conversation id.

The agent naturally gets numeric channel ids and ``CT_*`` chat ids from CDS
wake envelopes and has no reason to know which AES tool a given id needs.
Every ``cliq.channel.post`` that failed on 2026-09-28 named a target of one
of those two shapes, AES's own validation rejected it (see
cliq-bot-tools.ts / cliq-chat-id.ts), and the gateway then misclassified
that rejection as an ambiguous transport failure and parked the action in
``manual_review`` after 5 fruitless reconciles.

This module is the single place that decides what an agent-supplied Cliq
target actually is, and which AES tool/field it needs. It is used both by
context derivation (``context.py``, which resolves the target once at
execute time and can refuse with an instructive message) and by the Cliq
adapter (``adapters/cliq.py``, which turns an already-resolved target into
the AES request on every invoke/poll/reconcile) so the two never disagree
about what a given target id is.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Mapping

# Mirrors Agent-Email-Server's MIN_CLIQ_NUMERIC_CHAT_ID_DIGITS /
# CLIQ_CHAT_ID_PATTERN (src/tools/cliq-chat-id.ts) exactly. Keep these two
# definitions in lockstep -- the vendored fixture in
# tests/unit/outbound_gateway/fixtures/agent_email_tool_schemas.json pins
# AES's actual pattern string, and test_cliq_contract.py fails if this
# drifts from it.
MIN_CLIQ_NUMERIC_CHAT_ID_DIGITS = 15
CLIQ_CHAT_ID_PATTERN = rf"^(?:[0-9]{{{MIN_CLIQ_NUMERIC_CHAT_ID_DIGITS},}}|CT_[A-Za-z0-9_/%-]*[0-9][A-Za-z0-9_/%-]*)$"

_CLIQ_CHAT_ID_RE = re.compile(CLIQ_CHAT_ID_PATTERN)
_DIGITS_RE = re.compile(r"^[0-9]+$")


class CliqTargetKind(StrEnum):
    CHAT = "cliq_chat"
    CHANNEL = "cliq_channel"


def is_cliq_chat_id(value: str) -> bool:
    """True when ``value`` is shaped like a Zoho Cliq chat/conversation id:
    a >=15-digit numeric id, or a ``CT_<chat>_<user>`` id -- the same shape
    Agent Email Server's ``cliq_chat_post`` requires and
    ``cliq_channel_bot_post`` refuses."""
    return bool(_CLIQ_CHAT_ID_RE.match(value.strip()))


class CliqChannelIdUnresolvedError(ValueError):
    """A numeric Cliq channel id has no known channel_unique_name."""

    def __init__(self, channel_id: str):
        self.channel_id = channel_id
        super().__init__(
            f"Nothing was sent: channel id {channel_id} has no known unique name; call cliq_channels_list "
            "and pass channel_unique_name (e.g. 'maintenance'), or use cliq.chat.post for a DM/chat id."
        )


def resolve_cliq_target(
    raw_target_id: str,
    requested_kind: CliqTargetKind,
    channel_unique_names_by_id: Mapping[str, str],
) -> tuple[CliqTargetKind, str]:
    """Classify and, where needed, normalize a Cliq target the agent named.

    ``requested_kind`` is what the agent's chosen operation
    (cliq.channel.post / cliq.chat.post) implies. It is honored except in
    the one direction 2026-09-28 proved wrong:

    - A target requested as a CHANNEL but shaped like a chat id
      (``is_cliq_chat_id``) is overridden to CHAT: AES's
      ``cliq_channel_bot_post`` refuses that shape outright for
      channel_unique_name, so sending it there can never succeed, while
      ``cliq_chat_post`` already accepts it as-is.
    - A target requested as a CHANNEL that is a short numeric id (not
      chat-shaped) is a legacy/alternate Cliq channel id and is resolved to
      its channel_unique_name via the configured map. An id with no
      configured mapping refuses instead of guessing -- posting to a
      guessed channel is worse than not posting.
    - A target requested as a CHAT is trusted as given: AES's own schema
      pattern is the authority on whether it is a valid chat id, and a
      mismatch there now surfaces as a clean, message-carrying definitive
      rejection instead of a gateway-side misroute (see
      adapters/base.py's provider_rejected_request handling).
    - Anything else (already a unique_name like ``"maintenance"``) passes
      through unchanged.
    """
    value = raw_target_id.strip()
    if requested_kind is CliqTargetKind.CHAT:
        return CliqTargetKind.CHAT, value
    if is_cliq_chat_id(value):
        return CliqTargetKind.CHAT, value
    if _DIGITS_RE.match(value):
        resolved = channel_unique_names_by_id.get(value)
        if not resolved:
            raise CliqChannelIdUnresolvedError(value)
        return CliqTargetKind.CHANNEL, resolved
    return CliqTargetKind.CHANNEL, value


def cliq_tool_for_kind(kind: CliqTargetKind) -> tuple[str, str]:
    """The AES tool name and target argument key for an already-resolved
    Cliq target kind. Used by the adapter so the tool/field choice always
    matches what context derivation (``resolve_cliq_target``) decided the
    target actually is, never what operation the agent happened to name."""
    if kind is CliqTargetKind.CHAT:
        return "cliq_chat_post", "chat_id"
    return "cliq_channel_bot_post", "channel_unique_name"
