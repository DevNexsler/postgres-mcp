# Outbound gateway: pluggable capabilities (target design)

Status: **future work, not started.** Recorded 2026-09-29 at the owner's request: "modular and expandable
outbound gateway; ability to plug in new capabilities and new channels later in a modular way."

## Why

On 2026-09-29 an agent was asked to "add a comment in the ticket with a picture or PDF". The gateway has no
TenantCloud ticket-comment or attachment operation, so the agent could not do it. It reworded the request
and then told the operator the comment had been added.

Every new capability like this is expensive today, because what the gateway knows about an operation is spread
across the codebase. The goal is that adding a capability or a channel means adding **one module**.

## What is already modular

- **Provider adapters.** Each provider implements the `ProviderAdapter` protocol
  (`src/postgres_mcp/outbound_gateway/adapters/base.py`):
  `validate`, `build_request`, `invoke`, `poll`, `parse_receipt`, `reconcile`.
  Adapters exist for `email`, `quo`, `cliq`, `calendar` and `tenantcloud`.
- **Delivery.** One generic Restate workflow (`delivery_workflow.py`) delivers every operation. Retry comes from
  `retry_policy.py` (exponential, 1 h ceiling, then one staff warning via `staff_warning.py`), and resend
  safety from `idempotency_policy.py`.
- **Recording.** The action ledger, the state machine, stale-context checks and evidence are all
  provider-agnostic.

## What is not modular yet

Adding one operation (for example `tenantcloud.maintenance.comment`) touches all of these:

| Where | What has to change |
|---|---|
| `models.py` | the `Operation` enum and `OPERATION_USAGE` (role, intent, purpose) |
| `context.py`, `tenantcloud_shared.py` | provider sets such as `_TENANTCLOUD_OPERATIONS` / `TENANTCLOUD_OPERATIONS` |
| `metrics.py` | `_PROVIDER_BY_OPERATION` |
| `server.py` and deploy env | `enabled_operations` and `OUTBOUND_RESTATE_OPERATIONS` (env/compose) |
| `idempotency_policy.py` | the per-operation resend-safety class |
| Database | the allowlists for operations **and** intents |
| The adapter | argument validation, request building, receipt parsing |
| Agent instructions | the generated tool description, and the managed skill and SOUL guidance in Comm-Data-Store |

Missing any one of these produces a partial capability: the operation is enabled but refused, or delivered but
not counted, or described to the agent but not allowlisted.

## Target design

### 1. One capability = one module

```
outbound_gateway/capabilities/
  tenantcloud_maintenance_comment/
    manifest.py      # declarative definition (below)
    adapter.py       # ProviderAdapter implementation (or reuse of the channel adapter)
    tests/
```

A **channel** (for example WhatsApp or a new PMS) is a module that provides the provider adapter plus its client
config. A **capability** is an operation on an existing channel, and usually reuses the channel's adapter
with its own manifest.

### 2. The manifest is the single source of truth

```python
CAPABILITY = Capability(
    operation="tenantcloud.maintenance.comment",
    channel="tenantcloud",
    action_roles={"provider_mutation"},
    intent_kinds={"tenantcloud_maintenance_comment"},
    arguments=Schema(request_id=int, text=str, attachments=Optional[list[Attachment]]),
    supports_attachments=True,
    idempotency=IdempotencyClass.READBACK_BEFORE_RESEND,
    retry=RetryClass.STANDARD,       # maps onto retry_policy
    delivery="restate",              # or "direct"
    description="Add a comment (optionally with files) to a TenantCloud maintenance request.",
)
```

### 3. A registry generates everything else

At startup a registry discovers the capability modules and derives:

- the `Operation` values and the role/intent usage table;
- the provider-by-operation map for metrics and context;
- the enabled set, from the registry intersected with config, instead of hand-listed env values;
- the idempotency and retry lookups;
- the MCP tool description the agent sees (already generated; it would read from the registry);
- the seed data for the database operation and intent allowlists (a migration or sync step, with the registry
  as the source).

### 4. A contract test keeps capabilities complete

One parametrized test over every registered capability asserts that:

- it has a manifest, an adapter, a DB allowlist row for the operation and every intent, a tool-description entry,
  an idempotency class and a retry class;
- an end-to-end run through the Restate replay stand-in reaches `provider_receipt_verified`
  (the pattern of the existing per-operation e2e matrix);
- attachment-capable operations round-trip one attachment.

### 5. Honest reporting stays generic

The agent should report only actions that have an `action_id` in state `sent`, and name anything it could not
do. The gateway already returns that evidence. When a capability is missing, the tool description and a
refusal should say so plainly, so the agent answers "I can't do X yet" rather than approximating it.

## Migration path (incremental; no big bang)

1. Introduce the `Capability` manifest and registry, and generate the existing tables from the registry for
   today's 11 operations. Behaviour must not change; prove it with a parity test.
2. Move each existing operation into its own module, one PR per channel.
3. Switch the enabled set, metrics map and tool description to registry-derived values.
4. Add the database allowlist sync from the registry.
5. Add the first new capabilities through the new path:
   - `tenantcloud.maintenance.comment` (with attachments);
   - attachment upload on `tenantcloud.message.send`.

## Current operations (2026-09-29)

`email.send`, `quo.sms.send`, `cliq.channel.post`, `cliq.chat.post`, `calendar.create`, `calendar.update`,
`calendar.delete`, `tenantcloud.message.send`, `tenantcloud.lead.status.update`,
`tenantcloud.maintenance.create`, `tenantcloud.maintenance.status.update`.

## Related

- The gateway runs as three processes: `outbound-gateway`, `outbound-gateway-worker` and
  `tenantcloud-delivery-worker` (the Restate endpoint). Deploy all three together.
- Comm-Data-Store owns the agent instruction surfaces (managed skills and the SOUL policy). Capability
  descriptions should flow from the registry into those instructions, not be maintained by hand in two places.
