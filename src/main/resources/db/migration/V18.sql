ALTER TABLE public.cdr
    ADD COLUMN IF NOT EXISTS charging_date TIMESTAMPTZ;

ALTER TABLE public.cdr
    ADD COLUMN IF NOT EXISTS mcc_mnc_destination VARCHAR(6);

CREATE TABLE IF NOT EXISTS public.billing_parameter
(
    name  VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.billing_cdr
(
    cdr_id                   BIGINT      NOT NULL,
    charging_date            TIMESTAMPTZ NOT NULL,
    record_date              TIMESTAMPTZ NOT NULL,
    submit_date              TIMESTAMPTZ,
    delivery_date            TIMESTAMPTZ,
    message_id               VARCHAR(50),
    parent_id                VARCHAR(50),
    message_type             VARCHAR(50),
    origination_type         VARCHAR(5),
    origination_network_id   VARCHAR(20),
    origination_network_name VARCHAR(50),
    destination_network_id   VARCHAR(20),
    destination_network_name VARCHAR(50),
    addr_src_digits          VARCHAR(50),
    addr_dst_digits          VARCHAR(50),
    status                   VARCHAR(50),
    status_code              VARCHAR(50),
    comment                  TEXT,
    retry_number             INT,
    total_segment            INT,
    segment_sequence         INT,
    loaded_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    mcc_mnc_destination         VARCHAR(6),
    PRIMARY KEY (cdr_id, record_date, charging_date)
) PARTITION BY RANGE (charging_date);

ALTER TABLE public.billing_cdr
    ADD COLUMN IF NOT EXISTS mcc_mnc_destination VARCHAR(6);

CREATE INDEX IF NOT EXISTS idx_billing_cdr_network_charging_date ON public.billing_cdr (origination_network_id, charging_date);
CREATE INDEX IF NOT EXISTS idx_billing_cdr_message_id ON public.billing_cdr (message_id);

CREATE TABLE IF NOT EXISTS public.billing_cdr_load_state
(
    name         VARCHAR(50) PRIMARY KEY,
    loaded_until TIMESTAMP   NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION public.billing_cdr_timezone() RETURNS TEXT
    LANGUAGE sql
    STABLE
AS
$$
SELECT current_setting('TimeZone')
$$;

CREATE OR REPLACE FUNCTION public.billing_cdr_ensure_partition(p_day DATE) RETURNS VOID
    LANGUAGE plpgsql
AS
$$
DECLARE
    partition_name TEXT := format('billing_cdr_%s', to_char(p_day, 'YYYYMMDD'));
BEGIN
    IF to_regclass('public.' || partition_name) IS NULL THEN
        EXECUTE format('CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.billing_cdr FOR VALUES FROM (%L) TO (%L)',
                       partition_name,
                       p_day::TIMESTAMP AT TIME ZONE 'UTC',
                       (p_day + 1)::TIMESTAMP AT TIME ZONE 'UTC');
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.billing_cdr_load_range(p_from TIMESTAMP, p_to TIMESTAMP,
                                                         p_charging_from TIMESTAMPTZ DEFAULT NULL,
                                                         p_charging_to TIMESTAMPTZ DEFAULT NULL) RETURNS BIGINT
    LANGUAGE plpgsql
AS
$$
DECLARE
    tz          TEXT := public.billing_cdr_timezone();
    charging    DATE;
    loaded_rows BIGINT;
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS billing_cdr_staging ON COMMIT DROP AS
        SELECT * FROM public.billing_cdr WITH NO DATA;
    TRUNCATE billing_cdr_staging;

    INSERT INTO billing_cdr_staging (cdr_id, charging_date, record_date, submit_date, delivery_date, message_id, parent_id,
                                     message_type, origination_type, origination_network_id, origination_network_name,
                                     destination_network_id, destination_network_name, addr_src_digits, addr_dst_digits,
                                     status, status_code, comment, retry_number, total_segment, segment_sequence, loaded_at,
                                     mcc_mnc_destination)
    SELECT source.*
    FROM (SELECT c.id,
                 COALESCE(c.charging_date, c.submit_date AT TIME ZONE tz, c.record_date AT TIME ZONE tz) AS charging_date,
                 c.record_date AT TIME ZONE tz,
                 c.submit_date AT TIME ZONE tz,
                 c.delivery_date AT TIME ZONE tz,
                 c.message_id,
                 c.parent_id,
                 c.message_type,
                 c.origination_type,
                 c.origination_network_id,
                 c.origination_network_name,
                 c.destination_network_id,
                 c.destination_network_name,
                 c.addr_src_digits,
                 c.addr_dst_digits,
                 c.status,
                 c.status_code,
                 c.comment,
                 c.retry_number,
                 c.total_segment,
                 c.segment_sequence,
                 now(),
                 NULLIF(c.mcc_mnc_destination, '')
          FROM public.cdr c
          WHERE c.record_date >= p_from
            AND c.record_date < p_to
            AND c.message_type = 'MESSAGE'
            AND c.origination_type = 'SP') source
    WHERE (p_charging_from IS NULL OR source.charging_date >= p_charging_from)
      AND (p_charging_to IS NULL OR source.charging_date < p_charging_to);

    FOR charging IN SELECT DISTINCT (charging_date AT TIME ZONE 'UTC')::DATE FROM billing_cdr_staging
        LOOP
            PERFORM public.billing_cdr_ensure_partition(charging);
        END LOOP;

    INSERT INTO public.billing_cdr
    SELECT * FROM billing_cdr_staging
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS loaded_rows = ROW_COUNT;

    TRUNCATE billing_cdr_staging;
    RETURN loaded_rows;
END;
$$;

CREATE OR REPLACE PROCEDURE public.billing_cdr_incremental_load(p_lookback INTERVAL DEFAULT INTERVAL '2 hours',
                                                                p_initial_window INTERVAL DEFAULT INTERVAL '7 days')
    LANGUAGE plpgsql
AS
$$
DECLARE
    tz           TEXT      := public.billing_cdr_timezone();
    load_to      TIMESTAMP := now() AT TIME ZONE tz;
    load_from    TIMESTAMP;
    chunk_to     TIMESTAMP;
    last_loaded  TIMESTAMP;
    loaded_rows  BIGINT;
BEGIN
    PERFORM pg_advisory_lock(hashtext('billing_process'));

    SELECT loaded_until INTO last_loaded FROM public.billing_cdr_load_state WHERE name = 'cdr';
    load_from := COALESCE(last_loaded - p_lookback, date_trunc('day', load_to - p_initial_window));

    WHILE load_from < load_to
        LOOP
            chunk_to := LEAST(date_trunc('day', load_from) + INTERVAL '1 day', load_to);
            loaded_rows := public.billing_cdr_load_range(load_from, chunk_to);

            INSERT INTO public.billing_cdr_load_state (name, loaded_until, updated_at)
            VALUES ('cdr', chunk_to, now())
            ON CONFLICT (name) DO UPDATE SET loaded_until = GREATEST(billing_cdr_load_state.loaded_until, excluded.loaded_until),
                                             updated_at   = excluded.updated_at;
            COMMIT;

            RAISE NOTICE 'billing_cdr loaded % rows from % to %', loaded_rows, load_from, chunk_to;
            load_from := chunk_to;
        END LOOP;

    PERFORM pg_advisory_unlock(hashtext('billing_process'));
END;
$$;

CREATE OR REPLACE FUNCTION public.billing_cdr_source_available(p_local_day DATE) RETURNS BOOLEAN
    LANGUAGE sql
    STABLE
AS
$$
SELECT EXISTS (SELECT 1
               FROM pg_inherits
                        JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
                        JOIN pg_class child ON child.oid = pg_inherits.inhrelid
               WHERE parent.relname = 'cdr'
                 AND (child.relname = 'cdr_' || to_char(p_local_day, 'YYYYMMDD')
                   OR child.relname LIKE 'cdr\_' || to_char(p_local_day, 'YYYYMMDD') || '\_%'))
$$;

CREATE OR REPLACE FUNCTION public.billing_cdr_reload_day(p_day DATE) RETURNS BIGINT
    LANGUAGE plpgsql
AS
$$
DECLARE
    tz        TEXT        := public.billing_cdr_timezone();
    day_start TIMESTAMPTZ := p_day::TIMESTAMP AT TIME ZONE 'UTC';
    day_end   TIMESTAMPTZ := (p_day + 1)::TIMESTAMP AT TIME ZONE 'UTC';
BEGIN
    IF NOT public.billing_cdr_source_available((day_start AT TIME ZONE tz)::DATE) THEN
        RAISE EXCEPTION 'The cdr table no longer has the data of % to reload it', p_day;
    END IF;

    PERFORM public.billing_cdr_ensure_partition(p_day);
    DELETE FROM public.billing_cdr WHERE charging_date >= day_start AND charging_date < day_end;
    RETURN public.billing_cdr_load_range((day_start AT TIME ZONE tz) - INTERVAL '1 hour',
                                         now() AT TIME ZONE tz,
                                         day_start,
                                         day_end);
END;
$$;

CREATE OR REPLACE PROCEDURE public.billing_cdr_reload(p_from DATE, p_to DATE)
    LANGUAGE plpgsql
AS
$$
DECLARE
    reload_day  DATE;
    loaded_rows BIGINT;
BEGIN
    IF p_from IS NULL OR p_to IS NULL OR p_to < p_from THEN
        RAISE EXCEPTION 'Invalid reload range: % - %', p_from, p_to;
    END IF;

    FOR reload_day IN SELECT generate_series(p_from, p_to, INTERVAL '1 day')::DATE
        LOOP
            loaded_rows := public.billing_cdr_reload_day(reload_day);
            COMMIT;
            RAISE NOTICE 'billing_cdr reloaded % rows for %', loaded_rows, reload_day;
        END LOOP;
END;
$$;

CREATE OR REPLACE PROCEDURE public.billing_cdr_process_reload_requests()
    LANGUAGE plpgsql
AS
$$
DECLARE
    request     RECORD;
    reload_day  DATE;
    loaded_rows BIGINT;
    total_rows  BIGINT;
    error_text  TEXT;
BEGIN
    IF to_regclass('public.billing_reprocess') IS NULL THEN
        RETURN;
    END IF;

    PERFORM pg_advisory_lock(hashtext('billing_process'));

    UPDATE public.billing_reprocess
    SET status        = 'FAILED',
        error_message = 'The reload was interrupted before finishing',
        finished_at   = now(),
        updated_at    = now()::TIMESTAMP
    WHERE type = 'HISTORY'
      AND status = 'RUNNING';
    COMMIT;

    FOR request IN
        SELECT id, range_from, range_to
        FROM public.billing_reprocess
        WHERE type = 'HISTORY'
          AND status = 'PENDING'
        ORDER BY id
        LOOP
            UPDATE public.billing_reprocess
            SET status     = 'RUNNING',
                started_at = now(),
                updated_at = now()::TIMESTAMP
            WHERE id = request.id;
            COMMIT;

            error_text := NULL;
            total_rows := 0;
            IF request.range_from IS NULL OR request.range_to IS NULL OR request.range_to < request.range_from THEN
                error_text := format('Invalid reload range: %s - %s', request.range_from, request.range_to);
            END IF;

            IF error_text IS NULL THEN
                FOR reload_day IN SELECT generate_series(request.range_from, request.range_to, INTERVAL '1 day')::DATE
                    LOOP
                        BEGIN
                            loaded_rows := public.billing_cdr_reload_day(reload_day);
                        EXCEPTION
                            WHEN OTHERS THEN
                                error_text := SQLERRM;
                        END;
                        EXIT WHEN error_text IS NOT NULL;
                        total_rows := total_rows + loaded_rows;
                        COMMIT;
                    END LOOP;
            END IF;

            UPDATE public.billing_reprocess
            SET status        = CASE WHEN error_text IS NULL THEN 'COMPLETED' ELSE 'FAILED' END,
                error_message = error_text,
                finished_at   = now(),
                updated_at    = now()::TIMESTAMP
            WHERE id = request.id;
            COMMIT;

            RAISE NOTICE 'billing_cdr reload request % finished with % rows, error: %', request.id, total_rows, error_text;
        END LOOP;

    PERFORM pg_advisory_unlock(hashtext('billing_process'));
END;
$$;

CREATE OR REPLACE PROCEDURE public.billing_cdr_daily_maintenance()
    LANGUAGE plpgsql
AS
$$
DECLARE
    today DATE := (now() AT TIME ZONE 'UTC')::DATE;
BEGIN
    PERFORM public.billing_cdr_ensure_partition(today);
    PERFORM public.billing_cdr_ensure_partition(today + 1);
    PERFORM public.billing_cdr_ensure_partition(today + 2);
    COMMIT;

    CALL public.billing_cdr_incremental_load();
END;
$$;

CREATE OR REPLACE FUNCTION public.billing_resolve_mcc_mnc(p_msisdn TEXT) RETURNS VARCHAR
    LANGUAGE plpgsql
    STABLE
AS
$$
DECLARE
    digits   TEXT;
    resolved VARCHAR;
BEGIN
    IF p_msisdn IS NULL OR p_msisdn !~ '^[0-9+[:space:]-]+$' THEN
        RETURN NULL;
    END IF;

    digits := regexp_replace(p_msisdn, '[^0-9]', '', 'g');
    IF digits = '' THEN
        RETURN NULL;
    END IF;

    IF length(digits) <= 15 THEN
        SELECT CASE WHEN digits::NUMERIC <= r.sn_range_stop::NUMERIC THEN r.mcc_mnc END
        INTO resolved
        FROM (SELECT pp.sn_range_start, pp.sn_range_stop, pc.mcc || p.mnc AS mcc_mnc
              FROM public.plmn_prefix pp
                       JOIN public.plmn p ON p.id = pp.plmn_id
                       JOIN public.plmn_country pc ON pc.id = p.plmn_country_id
              WHERE pp.sn_range_start ~ '^[0-9]+$'
                AND pp.sn_range_stop ~ '^[0-9]+$'
                AND length(pp.sn_range_start) = length(digits)
                AND left(pp.sn_range_start, 1) = left(digits, 1)
                AND pp.sn_range_start::NUMERIC <= digits::NUMERIC
              ORDER BY pp.sn_range_start::NUMERIC DESC
              LIMIT 1) r;
        IF resolved IS NOT NULL THEN
            RETURN resolved;
        END IF;
    END IF;

    digits := left(digits, 15);
    SELECT pc.mcc || p.mnc
    INTO resolved
    FROM public.plmn_prefix pp
             JOIN public.plmn p ON p.id = pp.plmn_id
             JOIN public.plmn_country pc ON pc.id = p.plmn_country_id
    WHERE COALESCE(pp.sn_range_start, '') = ''
      AND pp.prefix IN (SELECT left(digits, length) FROM generate_series(1, length(digits)) length)
    ORDER BY length(pp.prefix) DESC
    LIMIT 1;
    RETURN resolved;
END;
$$;

CREATE OR REPLACE FUNCTION public.billing_next_cutoff(p_after DATE, p_cut_off_day INT) RETURNS DATE
    LANGUAGE sql
    IMMUTABLE
AS
$$
SELECT min(candidate)
FROM (SELECT month_start::DATE
                 + (LEAST(p_cut_off_day, EXTRACT(DAY FROM month_start + INTERVAL '1 month' - INTERVAL '1 day')::INT) - 1) AS candidate
      FROM (SELECT date_trunc('month', p_after) + make_interval(months => offset_months) AS month_start
            FROM generate_series(0, 1) offset_months) months) candidates
WHERE candidate > p_after
$$;

CREATE OR REPLACE FUNCTION public.billing_cutoff_delay() RETURNS INTERVAL
    LANGUAGE sql
    STABLE
AS
$$
SELECT make_interval(mins => COALESCE((SELECT value::INT FROM public.billing_parameter WHERE name = 'cutoff_delay_minutes'), 0))
$$;

CREATE OR REPLACE FUNCTION public.billing_snapshot_bill(p_bill_id INT) RETURNS VOID
    LANGUAGE plpgsql
AS
$$
BEGIN
    UPDATE public.bill b
    SET client_name   = s.client_name,
        message_cost  = s.message_cost,
        currency_code = s.currency_code,
        charge_unit   = s.charge_unit
    FROM public.billing_setting s
    WHERE b.id = p_bill_id
      AND s.id = b.billing_setting_id;

    DELETE FROM public.bill_service_provider WHERE bill_id = p_bill_id;
    INSERT INTO public.bill_service_provider (id, bill_id, network_id, service_provider_name, charge_success, charge_retries,
                                              charge_failed, success_count, retries_count, failed_count, billable_count, amount)
    SELECT nextval('public.bill_service_provider_id_seq'), b.id, ssp.network_id, COALESCE(sp.name, ssp.network_id::TEXT),
           ssp.charge_success, ssp.charge_retries, ssp.charge_failed, 0, 0, 0, 0, 0
    FROM public.bill b
             JOIN public.billing_setting_service_provider ssp ON ssp.billing_setting_id = b.billing_setting_id
             LEFT JOIN public.service_provider sp ON sp.network_id = ssp.network_id
    WHERE b.id = p_bill_id;

    DELETE FROM public.bill_network_price WHERE bill_id = p_bill_id;
    INSERT INTO public.bill_network_price (id, bill_id, mcc_mnc, network_name, country, message_cost)
    SELECT nextval('public.bill_network_price_id_seq'), b.id, np.mcc_mnc, net.network_name, net.country, np.message_cost
    FROM public.bill b
             JOIN public.billing_setting_network_price np ON np.billing_setting_id = b.billing_setting_id
             LEFT JOIN LATERAL (SELECT p.network_name, pc.country
                                FROM public.plmn p
                                         JOIN public.plmn_country pc ON pc.id = p.plmn_country_id
                                WHERE pc.mcc || p.mnc = np.mcc_mnc
                                LIMIT 1) net ON TRUE
    WHERE b.id = p_bill_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.billing_summarize_day(p_bill_id INT, p_day DATE) RETURNS VOID
    LANGUAGE plpgsql
AS
$$
DECLARE
    bill_row   public.bill%ROWTYPE;
    day_from TIMESTAMPTZ;
    day_to   TIMESTAMPTZ;
BEGIN
    SELECT * INTO bill_row FROM public.bill WHERE id = p_bill_id;
    day_from := GREATEST(p_day::TIMESTAMP AT TIME ZONE bill_row.timezone, bill_row.period_start);
    day_to := LEAST((p_day + 1)::TIMESTAMP AT TIME ZONE bill_row.timezone, bill_row.period_end);

    DELETE FROM public.bill_daily_summary WHERE bill_id = p_bill_id AND summary_date = p_day;

    WITH attempts AS (SELECT c.origination_network_id::INT                     AS network_id,
                             c.message_id,
                             COALESCE(NULLIF(c.parent_id, ''), c.message_id) AS parent_key,
                             c.status,
                             COALESCE(c.retry_number, 0)                     AS retry_number,
                             c.record_date,
                             c.mcc_mnc_destination,
                             c.addr_dst_digits                               AS charged_number
                      FROM public.billing_cdr c
                      WHERE c.charging_date >= day_from
                        AND c.charging_date < day_to
                        AND c.origination_network_id IN (SELECT network_id::TEXT
                                                         FROM public.bill_service_provider
                                                         WHERE bill_id = p_bill_id)),
         parts AS (SELECT network_id,
                          message_id,
                          min(parent_key)                                                            AS parent_key,
                          (array_agg(status ORDER BY retry_number DESC, record_date DESC))[1] = 'SUCCESS' AS success,
                          count(*) - 1                                                               AS retries,
                          max(mcc_mnc_destination)                                                      AS mcc_mnc_destination,
                          min(charged_number)                                                        AS charged_number
                   FROM attempts
                   GROUP BY network_id, message_id),
         units AS (SELECT network_id,
                          bool_and(success)   AS success,
                          max(retries)          AS retries,
                          max(mcc_mnc_destination) AS mcc_mnc_destination,
                          min(charged_number)   AS charged_number
                   FROM parts
                   GROUP BY network_id, CASE WHEN bill_row.charge_unit = 'PART' THEN message_id ELSE parent_key END),
         resolved AS MATERIALIZED (SELECT charged_number, public.billing_resolve_mcc_mnc(charged_number) AS mcc_mnc
                      FROM (SELECT DISTINCT charged_number FROM units WHERE mcc_mnc_destination IS NULL) numbers),
         priced AS (SELECT u.network_id,
                           COALESCE(u.mcc_mnc_destination, r.mcc_mnc) AS mcc_mnc,
                           COALESCE(np.message_cost, bill_row.message_cost) AS message_cost,
                           u.success,
                           u.retries
                    FROM units u
                             LEFT JOIN resolved r ON r.charged_number = u.charged_number
                             LEFT JOIN public.bill_network_price np
                                       ON np.bill_id = p_bill_id AND np.mcc_mnc = COALESCE(u.mcc_mnc_destination, r.mcc_mnc)),
         counted AS (SELECT p.network_id,
                            p.mcc_mnc,
                            p.message_cost,
                            count(*) FILTER (WHERE p.success)     AS success_count,
                            sum(p.retries)                         AS retries_count,
                            count(*) FILTER (WHERE NOT p.success) AS failed_count,
                            sum(CASE WHEN p.success AND sp.charge_success THEN 1 ELSE 0 END
                                    + CASE WHEN NOT p.success AND sp.charge_failed THEN 1 ELSE 0 END
                                    + CASE WHEN sp.charge_retries THEN p.retries ELSE 0 END) AS billable_count
                     FROM priced p
                              JOIN public.bill_service_provider sp ON sp.bill_id = p_bill_id AND sp.network_id = p.network_id
                     GROUP BY p.network_id, p.mcc_mnc, p.message_cost)
    INSERT
    INTO public.bill_daily_summary (id, bill_id, network_id, summary_date, mcc_mnc, message_cost, success_count,
                                    retries_count, failed_count, billable_count, amount)
    SELECT nextval('public.bill_daily_summary_id_seq'), p_bill_id, network_id, p_day, mcc_mnc, message_cost, success_count,
           retries_count, failed_count, billable_count, billable_count * message_cost
    FROM counted;
END;
$$;

CREATE OR REPLACE FUNCTION public.billing_reload_bill_days(p_bill_id INT, p_from DATE, p_to DATE) RETURNS VOID
    LANGUAGE plpgsql
AS
$$
DECLARE
    bill_timezone TEXT;
    reload_day    DATE;
    first_day     DATE;
    last_day      DATE;
    tz            TEXT := public.billing_cdr_timezone();
BEGIN
    SELECT timezone INTO bill_timezone FROM public.bill WHERE id = p_bill_id;

    first_day := ((p_from::TIMESTAMP AT TIME ZONE bill_timezone) AT TIME ZONE 'UTC')::DATE;
    last_day := ((((p_to + 1)::TIMESTAMP AT TIME ZONE bill_timezone) - INTERVAL '1 microsecond') AT TIME ZONE 'UTC')::DATE;

    FOR reload_day IN SELECT generate_series(first_day, last_day, INTERVAL '1 day')::DATE
        LOOP
            CONTINUE WHEN NOT public.billing_cdr_source_available(((reload_day::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE tz)::DATE);
            PERFORM public.billing_cdr_reload_day(reload_day);
        END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.billing_build_bill(p_bill_id INT, p_from DATE DEFAULT NULL, p_to DATE DEFAULT NULL) RETURNS VOID
    LANGUAGE plpgsql
AS
$$
DECLARE
    bill_row    public.bill%ROWTYPE;
    period_from DATE;
    period_to   DATE;
    summary_day DATE;
BEGIN
    SELECT * INTO bill_row FROM public.bill WHERE id = p_bill_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Bill % was not found', p_bill_id;
    END IF;

    period_from := (bill_row.period_start AT TIME ZONE bill_row.timezone)::DATE;
    period_to := ((bill_row.period_end - INTERVAL '1 microsecond') AT TIME ZONE bill_row.timezone)::DATE;

    IF p_from IS NULL THEN
        PERFORM public.billing_snapshot_bill(p_bill_id);
        DELETE FROM public.bill_daily_summary WHERE bill_id = p_bill_id;
    ELSE
        period_from := GREATEST(period_from, p_from);
        period_to := LEAST(period_to, p_to);
        PERFORM public.billing_reload_bill_days(p_bill_id, period_from, period_to);
    END IF;

    FOR summary_day IN SELECT generate_series(period_from, period_to, INTERVAL '1 day')::DATE
        LOOP
            PERFORM public.billing_summarize_day(p_bill_id, summary_day);
        END LOOP;

    UPDATE public.bill_service_provider sp
    SET success_count  = COALESCE(totals.success_count, 0),
        retries_count  = COALESCE(totals.retries_count, 0),
        failed_count   = COALESCE(totals.failed_count, 0),
        billable_count = COALESCE(totals.billable_count, 0),
        amount         = COALESCE(totals.amount, 0)
    FROM public.bill_service_provider target
             LEFT JOIN (SELECT network_id,
                               sum(success_count)  AS success_count,
                               sum(retries_count)  AS retries_count,
                               sum(failed_count)   AS failed_count,
                               sum(billable_count) AS billable_count,
                               sum(amount)         AS amount
                        FROM public.bill_daily_summary
                        WHERE bill_id = p_bill_id
                        GROUP BY network_id) totals ON totals.network_id = target.network_id
    WHERE target.id = sp.id
      AND target.bill_id = p_bill_id;

    UPDATE public.bill
    SET total_billable = (SELECT COALESCE(sum(billable_count), 0) FROM public.bill_service_provider WHERE bill_id = p_bill_id),
        total_amount   = (SELECT COALESCE(sum(amount), 0) FROM public.bill_service_provider WHERE bill_id = p_bill_id)
    WHERE id = p_bill_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.billing_create_due_bills() RETURNS INT
    LANGUAGE plpgsql
AS
$$
DECLARE
    setting       RECORD;
    period_from   DATE;
    period_to     DATE;
    new_bill_id   INT;
    created_bills INT := 0;
BEGIN
    FOR setting IN SELECT * FROM public.billing_setting WHERE enabled = 1 ORDER BY id
        LOOP
            LOOP
                SELECT COALESCE((max(period_end) AT TIME ZONE setting.timezone)::DATE, setting.start_date)
                INTO period_from
                FROM public.bill
                WHERE billing_setting_id = setting.id;

                EXIT WHEN setting.end_date IS NOT NULL AND period_from > setting.end_date;

                period_to := public.billing_next_cutoff(period_from, setting.cut_off_day);
                IF setting.end_date IS NOT NULL THEN
                    period_to := LEAST(period_to, setting.end_date + 1);
                END IF;

                EXIT WHEN now() < (period_to::TIMESTAMP AT TIME ZONE setting.timezone) + public.billing_cutoff_delay();

                new_bill_id := nextval('public.bill_id_seq');
                INSERT INTO public.bill (id, created_at, updated_at, billing_setting_id, client_name, period_start, period_end,
                                         invoice_date, timezone, message_cost, currency_code, charge_unit, status)
                VALUES (new_bill_id, now()::TIMESTAMP, now()::TIMESTAMP, setting.id, setting.client_name,
                        period_from::TIMESTAMP AT TIME ZONE setting.timezone, period_to::TIMESTAMP AT TIME ZONE setting.timezone,
                        period_to, setting.timezone, setting.message_cost, setting.currency_code, setting.charge_unit, 'GENERATING');

                INSERT INTO public.billing_reprocess (id, created_at, updated_at, type, bill_id, status)
                VALUES (nextval('public.billing_reprocess_id_seq'), now()::TIMESTAMP, now()::TIMESTAMP, 'BILL', new_bill_id, 'PENDING');
                created_bills := created_bills + 1;
            END LOOP;
        END LOOP;
    RETURN created_bills;
END;
$$;

CREATE OR REPLACE FUNCTION public.billing_has_due_bills() RETURNS BOOLEAN
    LANGUAGE plpgsql
    STABLE
AS
$$
DECLARE
    setting     RECORD;
    period_from DATE;
    period_to   DATE;
BEGIN
    FOR setting IN SELECT * FROM public.billing_setting WHERE enabled = 1
        LOOP
            SELECT COALESCE((max(period_end) AT TIME ZONE setting.timezone)::DATE, setting.start_date)
            INTO period_from
            FROM public.bill
            WHERE billing_setting_id = setting.id;

            CONTINUE WHEN setting.end_date IS NOT NULL AND period_from > setting.end_date;

            period_to := public.billing_next_cutoff(period_from, setting.cut_off_day);
            IF setting.end_date IS NOT NULL THEN
                period_to := LEAST(period_to, setting.end_date + 1);
            END IF;

            IF now() >= (period_to::TIMESTAMP AT TIME ZONE setting.timezone) + public.billing_cutoff_delay() THEN
                RETURN TRUE;
            END IF;
        END LOOP;
    RETURN FALSE;
END;
$$;

CREATE OR REPLACE PROCEDURE public.billing_process_bills()
    LANGUAGE plpgsql
AS
$$
DECLARE
    request    RECORD;
    error_text TEXT;
BEGIN
    IF to_regclass('public.bill') IS NULL OR to_regclass('public.billing_reprocess') IS NULL THEN
        RETURN;
    END IF;

    PERFORM pg_advisory_lock(hashtext('billing_process'));

    UPDATE public.bill b
    SET status         = 'FAILED',
        failure_reason = 'The generation was interrupted before finishing',
        updated_at     = clock_timestamp()::TIMESTAMP
    FROM public.billing_reprocess r
    WHERE r.type = 'BILL'
      AND r.status = 'RUNNING'
      AND r.bill_id = b.id;

    UPDATE public.billing_reprocess
    SET status        = 'FAILED',
        error_message = 'The generation was interrupted before finishing',
        finished_at   = clock_timestamp(),
        updated_at    = clock_timestamp()::TIMESTAMP
    WHERE type = 'BILL'
      AND status = 'RUNNING';
    COMMIT;

    IF NOT public.billing_has_due_bills()
        AND NOT EXISTS (SELECT 1 FROM public.billing_reprocess WHERE type = 'BILL' AND status = 'PENDING') THEN
        PERFORM pg_advisory_unlock(hashtext('billing_process'));
        RETURN;
    END IF;

    CALL public.billing_cdr_incremental_load();

    PERFORM public.billing_create_due_bills();
    COMMIT;

    FOR request IN
        SELECT id, bill_id, range_from, range_to
        FROM public.billing_reprocess
        WHERE type = 'BILL'
          AND status = 'PENDING'
        ORDER BY id
        LOOP
            UPDATE public.billing_reprocess
            SET status     = 'RUNNING',
                started_at = clock_timestamp(),
                updated_at = clock_timestamp()::TIMESTAMP
            WHERE id = request.id;

            UPDATE public.bill
            SET status         = 'GENERATING',
                failure_reason = NULL,
                updated_at     = clock_timestamp()::TIMESTAMP
            WHERE id = request.bill_id;
            COMMIT;

            error_text := NULL;
            BEGIN
                PERFORM public.billing_build_bill(request.bill_id, request.range_from, request.range_to);
            EXCEPTION
                WHEN OTHERS THEN
                    error_text := SQLERRM;
            END;

            UPDATE public.bill
            SET status         = CASE WHEN error_text IS NULL THEN 'GENERATED' ELSE 'FAILED' END,
                failure_reason = error_text,
                updated_at     = clock_timestamp()::TIMESTAMP
            WHERE id = request.bill_id;

            UPDATE public.billing_reprocess
            SET status        = CASE WHEN error_text IS NULL THEN 'COMPLETED' ELSE 'FAILED' END,
                error_message = error_text,
                finished_at   = clock_timestamp(),
                updated_at    = clock_timestamp()::TIMESTAMP
            WHERE id = request.id;
            COMMIT;

            RAISE NOTICE 'bill % generation finished, error: %', request.bill_id, error_text;
        END LOOP;

    PERFORM pg_advisory_unlock(hashtext('billing_process'));
END;
$$;

DO
$$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.schedule('billing_cdr_daily_maintenance', '30 0 * * *', 'CALL public.billing_cdr_daily_maintenance()');
        PERFORM cron.schedule('billing_cdr_reload_requests', '*/5 * * * *', 'CALL public.billing_cdr_process_reload_requests()');
        PERFORM cron.schedule('billing_process_bills', '*/5 * * * *', 'CALL public.billing_process_bills()');
    ELSE
        RAISE NOTICE 'pg_cron is not installed, the billing_cdr jobs were not scheduled';
    END IF;
END;
$$;
