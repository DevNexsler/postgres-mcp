-- Comm-Data-Store's contact identity and "answered" functions, verbatim, for
-- the gateway's integration tests (tests/integration/test_gateway_recently_answered.py):
-- dedupe_phone_last10 from migration 268, the rest from migration 269
-- (Comm-Data-Store 750019fb, docs/contact-identity-functions.md).
-- Comm-Data-Store owns them; refresh this copy when they change. The CDS
-- system tier runs the gateway against the real schema.


CREATE OR REPLACE FUNCTION dedupe_phone_last10(p_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN length(regexp_replace(coalesce(p_value, ''), '\D', '', 'g')) >= 10
            THEN right(regexp_replace(p_value, '\D', '', 'g'), 10)
    END
$$;

CREATE OR REPLACE FUNCTION public.contact_recently_answered_window()
RETURNS interval
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT interval '72 hours'
$$;

CREATE OR REPLACE FUNCTION public.zillow_extract_field(p_body text, p_field text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN p_field ~ '^[a-z_]+$' THEN
            nullif(btrim(substring(coalesce(p_body, '') FROM '(?:^|[|] )' || p_field || ': ([^|]*)')), '')
    END
$$;

CREATE OR REPLACE FUNCTION public.contact_key(p_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN v ~ '^tenantcloud:customer-email-sha256:[0-9a-f]{64}$'
            THEN 'email_sha256:' || substring(v FROM '([0-9a-f]{64})$')
        WHEN v ~ '^[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+$'
            THEN 'email:' || v
        WHEN v ~ '^[+]?[0-9() .-]+$' AND public.dedupe_phone_last10(v) IS NOT NULL
            THEN 'phone:' || public.dedupe_phone_last10(v)
    END
    FROM (SELECT lower(btrim(coalesce(p_value, ''))) AS v) AS value
$$;

CREATE OR REPLACE FUNCTION public.contact_key_kind(p_key text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN p_key LIKE 'email:%@convo.zillow.com' THEN 'zillow_proxy'
        WHEN p_key LIKE 'email:%' THEN 'email'
        WHEN p_key LIKE 'phone:%' THEN 'phone'
        WHEN p_key LIKE 'email_sha256:%' THEN 'email_sha256'
    END
$$;

CREATE OR REPLACE FUNCTION public.contact_key_is_agency(p_key text)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.agency_identifiers AS a
        WHERE a.kind = 'email_domain'
          AND p_key LIKE 'email:%'
          AND split_part(p_key, '@', 2) = lower(btrim(a.value))
    ) OR EXISTS (
        SELECT 1 FROM public.agency_identifiers AS a
        WHERE a.kind = 'phone' AND p_key LIKE 'phone:%'
          AND public.contact_key(a.value) = p_key
    ) OR EXISTS (
        SELECT 1 FROM public.channels AS c
        WHERE c.source = 'quo' AND c.name IS NOT NULL AND p_key LIKE 'phone:%'
          AND public.contact_key(c.name) = p_key
    )
$$;

CREATE OR REPLACE FUNCTION public.contact_keys_from_parts(
    p_source text,
    p_direction text,
    p_sender_identifier text,
    p_body text,
    p_payload jsonb
) RETURNS TABLE (key_kind text, contact_key text)
LANGUAGE sql
STABLE
AS $$
    WITH candidate(value) AS (
        SELECT v FROM unnest(ARRAY[
            p_sender_identifier,
            CASE WHEN p_source = 'zillow_rm_web_extract'
                 THEN public.zillow_extract_field(p_body, 'direct_email') END,
            CASE WHEN p_source = 'zillow_rm_web_extract'
                 THEN public.zillow_extract_field(p_body, 'proxy_email') END,
            CASE WHEN p_source = 'zillow_rm_web_extract'
                 THEN public.zillow_extract_field(p_body, 'phone') END,
            p_payload->>'email',
            p_payload->>'direct_email',
            p_payload #>> '{message_data,email}',
            p_payload->>'phone',
            p_payload #>> '{message_data,phone}'
        ]) AS v
        WHERE coalesce(p_direction, '') <> 'outbound'
    ), plain AS (
        SELECT DISTINCT public.contact_key(value) AS contact_key
        FROM candidate
    ), keyed AS (
        SELECT contact_key FROM plain
        WHERE contact_key IS NOT NULL
          AND NOT public.contact_key_is_agency(contact_key)
        UNION
        SELECT 'email_sha256:' || encode(public.digest(substring(contact_key FROM 7), 'sha256'), 'hex')
        FROM plain
        WHERE public.contact_key_kind(contact_key) = 'email'
          AND NOT public.contact_key_is_agency(contact_key)
    )
    SELECT public.contact_key_kind(contact_key), contact_key
    FROM keyed
    ORDER BY 2
$$;

CREATE OR REPLACE FUNCTION public.message_contact_keys(p_message_id bigint)
RETURNS TABLE (key_kind text, contact_key text)
LANGUAGE sql
STABLE
AS $$
    SELECT keys.key_kind, keys.contact_key
    FROM public.messages AS m
    LEFT JOIN public.raw_events AS r ON r.id = m.raw_event_id
    CROSS JOIN LATERAL public.contact_keys_from_parts(
        m.source, m.direction, m.sender_identifier, m.body, r.payload
    ) AS keys
    WHERE m.id = p_message_id
$$;

CREATE OR REPLACE FUNCTION public.prospect_reply_status(p_action public.outbound_actions)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT CASE
        WHEN p_action.action_role IS DISTINCT FROM 'prospect_reply'
          OR coalesce(p_action.provenance, '') NOT IN ('customer', 'operator_remediation')
          OR EXISTS (
              SELECT 1 FROM public.outbound_action_quarantines AS q
              WHERE q.action_id = p_action.action_id
          )
            THEN NULL
        WHEN p_action.state = 'completed'
         AND p_action.completion_kind = 'sent'
         AND coalesce(p_action.detail_code, '') NOT IN (
             'already_handled', 'existing_lock_receipt', 'duplicate_inquiry_already_handled'
         )
            THEN 'answered'
        WHEN p_action.state IN ('prepared', 'dispatching', 'provider_accepted', 'unknown')
            THEN 'in_flight'
    END
$$;

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
    SELECT *
    FROM (
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
               recipient_key.value AS contact_key,
               a.arguments->>'text' AS reply_text
        FROM public.outbound_actions AS a
        CROSS JOIN LATERAL (
            SELECT public.contact_key(a.recipient_scope->>'target_id') AS value
        ) AS recipient_key
        CROSS JOIN LATERAL (SELECT public.prospect_reply_status(a) AS value) AS status
        WHERE a.action_role = 'prospect_reply'
          AND cardinality(p_keys) > 0
          AND recipient_key.value = ANY(p_keys)
          AND (p_until IS NULL OR a.created_at <= p_until)
          AND (status.value = 'answered'
               OR (coalesce(p_include_in_flight, false) AND status.value = 'in_flight'))
    ) AS reply
    WHERE reply.sent_at >= p_since
      AND (p_until IS NULL OR reply.sent_at <= p_until)
    ORDER BY reply.sent_at DESC, reply.action_id
$$;
