# Cliq ticket threads

Keep creation and replies inside `outbound_action`. Create a normal channel root
for the canonical repair ticket, then create its thread using that root's exact
provider message ID:

```json
{
  "op": "execute",
  "wakeup_event_id": 12345,
  "operation": "cliq.channel.post",
  "action_role": "internal_notification",
  "intent_kind": "manual_review_alert",
  "arguments": {
    "channel_or_chat_id": "maintenance",
    "text": "🟠 Address / unit · SOR #123\n👤 Owner — next action by deadline",
    "thread_message_id": "1793138871358_307080186942",
    "thread_title": "Address / unit · SOR #123",
    "post_in_parent": false
  }
}
```

Substitute the actual wake, channel, ticket, parent and message. The parent must
already exist in that exact joined channel. `reply_to` is an optional quote
target, distinct from thread creation. Channel reply mode must permit `both` or
`threads`. Agent Email Server verifies the configuration and exact parent before
sending, then requires matching thread metadata from Zoho.

Poll the existing action with `op: status`. A completed result includes
`provider_message_id` and `thread_information: {chat_id, parent_message_id}`.
Store these with the canonical SOR ticket; status and duplicate replay retain
the thread identity.

For later internal notifications, keep `cliq.channel.post` and target the returned
`CT_...-T-...` chat ID. Context derivation selects the existing chat sender. Replies
to an inbound thread wake use `cliq.chat.post`, `internal_reply/internal_reply`
and the exact inbound thread destination. Thread replies default to
`post_in_parent: false`. Optional `reply_to` quotes a message inside the thread.

An ambiguous response is not permission to create another root or resend under
a fresh key: reconcile the original action. Channel settings changes require
the existing owner/admin connection, separately from sending a thread.

Deploy Agent Email Server support before this gateway version, then refresh
caller tool catalogs. This adds no operation, recipient authority, task-closing
rule or tenant-send permission.
