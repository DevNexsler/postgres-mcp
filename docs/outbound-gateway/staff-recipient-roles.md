# Staff coordination and customer reply classification

Select role and intent by recipient purpose. SMS and email each advertise two
recipes in the gateway's public tool description:

| Recipient purpose | action_role | intent_kind |
| --- | --- | --- |
| Actual customer answer | `prospect_reply` | `inquiry_reply` (or the applicable showing reply intent) |
| Staff coordination, including updates about a prospect | `internal_notification` | `manual_review_alert` |

A prospect wake can trigger a staff notification. Its wake source, transport,
or override provenance does not turn that notification into a customer answer.
For example, a showing update to staff uses:

```json
{
  "request": {
    "op": "execute",
    "wakeup_event_id": 12345,
    "action_role": "internal_notification",
    "operation": "quo.sms.send",
    "intent_kind": "manual_review_alert",
    "arguments": {
      "to_phone": "+19085550199",
      "from_phone": "+16105550100",
      "text": "Prospect is heading to the showing."
    }
  }
}
```

For staff email use the same role/intent, `email.send`, and arguments
`{"to_address":"staff@example.com","text":"Showing update","cc":[]}`.
Choose the actual configured sending line/mailbox and verified destination.
Same-source Cliq replies retain the existing `internal_reply` contract.

## Cause and regression

The old catalog advertised `prospect_reply/inquiry_reply` for "text anyone" and
"email anyone". Following that recipe stored staff coordination as customer
answers. The parser and context loader already supported staff notifications;
the guidance omitted their SMS/email recipes.

In the October 23 incident, a staff SMS was accepted at **15:42:26.050 UTC**.
Courtney's new deposit question was stored at **15:42:26.000 UTC**. CDS migration
270 intentionally finds customer replies through their trigger-message contact
keys, across channels and recipients. Misclassification made the staff SMS the
newest customer answer and disabled the new-question exception. Shared subject
keys were not the matcher predicate. Timestamp precision does not establish
physical event ordering; these stored values determine the branch.

`tests/integration/test_gateway_recently_answered.py` builds requests from the
public catalog, derives real context, persists through `PostgresActionStore`,
and evaluates the actual CDS reply-status/trigger-key SQL and gateway preflight.
Its fixture uses migration 269 plus the unmodified migration 270 from CDS commit
`023b04ab`; the latter's function body matches the captured deployed definition.

Controls cover SMS and email, normal and `operator_remediation` provenance,
acceptance 50 ms before/equal to/50 ms after the question, genuine customer
replies through trigger aliases, and same-sender repeated questions. The legacy
misclassified request deliberately still reproduces the refusal: this patch
corrects producer guidance, without guessing purpose from phone numbers or text.

```bash
uv run pytest tests/integration/test_gateway_recently_answered.py -q
```

## Existing ledger: read-only audit proposal

Old polluted rows retain their original classification. Do not bulk-rewrite
overrides: genuine customer `operator_remediation` replies must count. Do not
restrict matching to the same channel or remove trigger aliases.

The following transaction only inspects two incident staff actions and projects
their proposed semantic classification. It preserves all fields in the composite
row except the projected role/intent; it updates no record and sends nothing.

```sql
BEGIN READ ONLY;
SELECT a.action_id, a.wakeup_event_id, a.action_role, a.intent_kind,
       a.provenance, a.state, a.completion_kind,
       a.recipient_scope->>'target_id' AS recipient,
       a.provider_accepted_at, a.provider_message_id,
       a.retry_of_action_id,
       public.prospect_reply_status(a) AS recorded_reply_status,
       public.prospect_reply_status(jsonb_populate_record(
           a, '{"action_role":"internal_notification","intent_kind":"manual_review_alert"}'::jsonb
       )) AS proposed_reply_status
FROM public.outbound_actions AS a
WHERE a.action_id IN (
    '538d8960-c003-5ab5-8978-ca0f4515be1c'::uuid,
    '36870f14-4b33-5e86-b71e-267b5448a60e'::uuid
)
ORDER BY a.provider_accepted_at, a.action_id;
ROLLBACK;
```

For broader inventory, use an explicitly verified staff-recipient list and the
72-hour reply window to select candidates, then review original request, purpose,
trigger, override reason, and receipt for each action. Recipient membership alone
is not approval to change a row. Capture selected IDs, before/after classification,
receipt references, reviewer, and rollback plan in a separate remediation review.
Choose a CDS-supported correction or quarantine procedure only after checking
ledger immutability and all consumers. No write procedure is authorized here.

## Rollout review

After merge, deploy through the established gateway release workflow and refresh
the caller's tool catalog. Review caller guidance that still teaches universal
customer roles or blames shared subjects/channel identity. Verify a staff SMS/email
request is recorded as `internal_notification/manual_review_alert`, has isolated
staff context, and has `prospect_reply_status = NULL`; verify a genuine customer
override still counts. Provider sends and production deployment require separate
authorization. Review old-ledger remediation separately; this PR performs none.
