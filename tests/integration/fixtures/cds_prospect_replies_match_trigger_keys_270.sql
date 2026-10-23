-- 270: prospect_replies_to_keys also matches an earlier reply by the contact
-- keys of the message that triggered it (self-heal contract C4 follow-up,
-- 2026-10-18).
--
-- Migration 269 matched an earlier reply only by the key of its RECIPIENT.
-- A reply addressed to a TenantCloud thread id, or to a Zillow proxy that the
-- later wake's message does not carry, shares no recipient key with the later
-- wake, so the outbound gateway's `recently_answered` check (postgres-mcp #84,
-- 33d77aa, under a per-contact-key advisory lock) and wake context missed it.
-- 2026-09-28 Aimee Tapia: wake 27332 (Zillow extract) answered her proxy; the
-- TenantCloud lead wake 27331 carries only her email hash, which the proxy
-- recipient never normalizes to, but which the extract message 817301 that
-- triggered the first reply does carry.
--
-- Now a reply matches when either
--   * its recipient's contact_key() is in p_keys (as before), or
--   * message_contact_keys() of its trigger message -- the wake's message_id,
--     and the action's source_message_id when that differs -- intersects
--     p_keys.
-- contact_key reports the key that matched, the recipient's first; one row
-- per action. Signature and output columns are unchanged, so the gateway and
-- wake context need no change.
--
-- Cost: the gateway runs this on every prospect reply while holding the
-- lock. The trigger leg runs message_contact_keys per candidate reply, so
-- candidates are first bounded by the time range: sent_at is one of
-- created_at / provider_accepted_at / completed_at, never later than their
-- greatest, and idx_outbound_actions_prospect_reply_latest indexes that
-- greatest. Only replies whose greatest timestamp reaches p_since are
-- examined (production 2026-10-18: 1476 prospect replies in all; a 72 h
-- window holds a few dozen).

BEGIN;

SET LOCAL lock_timeout = '5s';

CREATE INDEX IF NOT EXISTS idx_outbound_actions_prospect_reply_latest
    ON public.outbound_actions (greatest(created_at, provider_accepted_at, completed_at))
    WHERE action_role = 'prospect_reply';

-- Prospect replies sent to one of p_keys, or triggered by a message that
-- carries one of p_keys, sent at or after p_since and (when p_until is given)
-- at or before p_until, newest first. sent_at is the provider acceptance
-- (else completion) of an answered reply, the provider acceptance (else
-- creation) of an in-flight one. In-flight replies only when
-- p_include_in_flight. contact_key is the key that matched (the recipient's
-- when both match). Callers apply their own scope on top (the gateway: other
-- wakes and its intents; wake context: provenance already matches).
CREATE OR REPLACE FUNCTION public.prospect_replies_to_keys(
    p_keys text[],
    p_since timestamptz,
    p_include_in_flight boolean DEFAULT false,
    p_until timestamptz DEFAULT NULL
) RETURNS TABLE (
    action_id uuid,
    wakeup_event_id bigint,
    intent_kind text,
    reply_status text,
    state text,
    sent_at timestamptz,
    recipient text,
    contact_key text,
    reply_text text
)
LANGUAGE sql
STABLE
AS $$
    WITH candidate AS MATERIALIZED (
        -- Replies that can fall in the range, read without the whole row.
        -- sent_at is one of these three timestamps, never later than their
        -- greatest (idx_outbound_actions_prospect_reply_latest).
        SELECT a.action_id,
               a.wakeup_event_id,
               a.source_message_id,
               a.recipient_scope->>'target_id' AS recipient
        FROM public.outbound_actions AS a
        WHERE a.action_role = 'prospect_reply'
          AND cardinality(p_keys) > 0
          AND greatest(a.created_at, a.provider_accepted_at, a.completed_at) >= p_since
          AND (p_until IS NULL OR a.created_at <= p_until)
    ), hit AS MATERIALIZED (
        -- The key each candidate matched: its recipient's first, else the
        -- smallest key its trigger message carries.
        SELECT DISTINCT ON (candidate.action_id)
               candidate.action_id, matched.contact_key
        FROM candidate
        CROSS JOIN LATERAL (
            SELECT 0 AS leg, public.contact_key(candidate.recipient) AS contact_key
            UNION ALL
            SELECT 1 AS leg, trigger_key.contact_key
            FROM public.hermes_wakeup_events AS wake
            CROSS JOIN LATERAL (
                SELECT DISTINCT trigger.message_id
                FROM unnest(ARRAY[wake.message_id, candidate.source_message_id])
                     AS trigger(message_id)
                WHERE trigger.message_id IS NOT NULL
            ) AS trigger
            CROSS JOIN LATERAL public.message_contact_keys(trigger.message_id) AS trigger_key
            WHERE wake.id = candidate.wakeup_event_id
        ) AS matched
        WHERE matched.contact_key = ANY(p_keys)
        ORDER BY candidate.action_id, matched.leg, matched.contact_key
    )
    SELECT reply.action_id,
           reply.wakeup_event_id,
           reply.intent_kind,
           reply.reply_status,
           reply.state,
           reply.sent_at,
           reply.recipient,
           reply.contact_key,
           reply.reply_text
    FROM (
        -- prospect_reply_status(a) reads (and detoasts) the whole row: only
        -- for replies that matched a key.
        SELECT a.action_id,
               a.wakeup_event_id,
               a.intent_kind,
               status.value AS reply_status,
               a.state,
               CASE WHEN status.value = 'answered'
                    THEN coalesce(a.provider_accepted_at, a.completed_at)
                    ELSE coalesce(a.provider_accepted_at, a.created_at)
               END AS sent_at,
               a.recipient_scope->>'target_id' AS recipient,
               hit.contact_key,
               a.arguments->>'text' AS reply_text
        FROM hit
        JOIN public.outbound_actions AS a ON a.action_id = hit.action_id
        CROSS JOIN LATERAL (SELECT public.prospect_reply_status(a) AS value) AS status
        WHERE status.value = 'answered'
           OR (coalesce(p_include_in_flight, false) AND status.value = 'in_flight')
    ) AS reply
    WHERE reply.sent_at >= p_since
      AND (p_until IS NULL OR reply.sent_at <= p_until)
    ORDER BY reply.sent_at DESC, reply.action_id
$$;

COMMENT ON FUNCTION public.prospect_replies_to_keys(text[], timestamptz, boolean, timestamptz) IS
    'Prospect replies sent to, or triggered by a message carrying, one of the keys, sent in [since, until], newest first (migrations 269, 270).';
COMMENT ON INDEX public.idx_outbound_actions_prospect_reply_latest IS
    'Bounds prospect_replies_to_keys candidates by time before the trigger-key leg (migration 270).';

COMMIT;
