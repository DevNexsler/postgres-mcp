-- Comm-Data-Store's outbound_alias_is_hub (migration 251) and the two
-- functions it calls (normalize_intent_lock_text, migration 026;
-- cds_email_identity_class, migration 154), verbatim, for the gateway's
-- integration tests (repository.py reads the hub rule). Comm-Data-Store owns
-- them; refresh this copy when they change.

CREATE OR REPLACE FUNCTION normalize_intent_lock_text(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT regexp_replace(lower(btrim(coalesce(value, ''))), '\s+', ' ', 'g')
$$;

CREATE OR REPLACE FUNCTION cds_email_identity_class(value text)
RETURNS text
LANGUAGE sql STABLE STRICT PARALLEL SAFE
AS $$
WITH normalized AS (
    SELECT
        lower(btrim(value)) AS email,
        split_part(lower(btrim(value)), '@', 1) AS local_part,
        split_part(lower(btrim(value)), '@', 2) AS domain
), shaped AS (
    SELECT
        email,
        domain,
        regexp_replace(split_part(local_part, '+', 1), '[-_.]', '', 'g') AS compact_local
    FROM normalized
)
SELECT CASE
    WHEN email !~ '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$'
        THEN 'invalid'
    WHEN EXISTS (
        SELECT 1
        FROM agency_identifiers AS agency
        WHERE agency.kind = 'email_domain'
          AND (domain = lower(agency.value) OR domain LIKE '%.' || lower(agency.value))
    )
        THEN 'agency'
    WHEN compact_local = ANY (ARRAY[
        'noreply', 'donotreply', 'notification', 'notifications',
        'mailer', 'mailerdaemon', 'systemgenerated', 'postmaster',
        'bounce', 'calendarnotification'
    ]::text[])
      OR domain = ANY (ARRAY[
        'amazonses.com', 'zohocalendar.com', 'mailer.zohocliq.com',
        'notifications.intuit.com', 'business-updates.facebook.com',
        'if-alerts.firstenergycorp.com'
      ]::text[])
      OR EXISTS (
        SELECT 1
        FROM unnest(ARRAY[
            'amazonses.com', 'zohocalendar.com', 'mailer.zohocliq.com',
            'notifications.intuit.com', 'business-updates.facebook.com',
            'if-alerts.firstenergycorp.com'
        ]::text[]) AS transport_domain(value)
        WHERE domain LIKE '%.' || transport_domain.value
      )
        THEN 'transport'
    ELSE 'identity'
END
FROM shaped;
$$;

CREATE OR REPLACE FUNCTION public.outbound_alias_is_hub(p_alias text)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog, public, pg_temp
AS $$
    WITH alias AS (
        SELECT regexp_replace(normalize_intent_lock_text(p_alias), '^prospect:', '') AS key
    ), typed AS (
        SELECT key,
               coalesce(substring(key FROM '^([a-z_]+):'), '') AS kind,
               regexp_replace(key, '^[a-z_]+:', '') AS value
        FROM alias
    )
    SELECT key = 'nigel'
        OR (kind IN ('email', '') AND value LIKE '%@%'
            AND cds_email_identity_class(value) IN ('agency', 'transport'))
        OR (kind IN ('phone', 'sms', '') AND (
                value IN ('+19083397021', '+19085385092')
                OR EXISTS (
                    SELECT 1 FROM agency_identifiers AS agency
                    WHERE agency.kind = 'phone' AND agency.value = typed.value
                )))
        OR (kind IN ('zoho_cliq', 'cliq_user') AND EXISTS (
                SELECT 1 FROM agency_identifiers AS agency
                WHERE agency.kind = 'cliq_user_id'
                  AND agency.value = regexp_replace(typed.value, '^user:', '')
            ))
    FROM typed
$$;
