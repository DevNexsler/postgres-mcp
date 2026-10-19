"""Stable diagnostics for a Cliq send refused by the local account guard."""

CLIQ_CHAT_ACCOUNT_MISMATCH = "cliq_chat_account_mismatch"
CLIQ_CHAT_ACCOUNT_MISMATCH_DETAIL = (
    "Not sent: this chat belongs to another account; Nigel's write account does not participate. "
    "The provider was not called. Do not retry or override the same target. "
    "For a channel notification, resolve the intended channel's unique name and execute "
    "cliq.channel.post through this gateway with that name. For a DM/group chat, use a "
    "verified chat belonging to Nigel's write account. If no supported target is available, record needs_human."
)
