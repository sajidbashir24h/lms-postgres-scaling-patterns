-- ============================================================
-- 00_schema_and_synthetic_data.sql
-- Synthetic LMS schema and anonymized data generator.
-- ============================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS daily_activity_logs CASCADE;
DROP TABLE IF EXISTS newsletter_leads CASCADE;
DROP TABLE IF EXISTS course_enrollments CASCADE;
DROP TABLE IF EXISTS lms_users CASCADE;

CREATE TABLE lms_users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email_hash TEXT NOT NULL UNIQUE,
    country_code CHAR(2) NOT NULL,
    plan_tier TEXT NOT NULL CHECK (plan_tier IN ('free', 'pro', 'enterprise')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_active_date DATE
);

CREATE TABLE course_enrollments (
    enrollment_id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES lms_users(user_id) ON DELETE CASCADE,
    course_id BIGINT NOT NULL,
    enrolled_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    progress_pct NUMERIC(5,2) NOT NULL DEFAULT 0.00 CHECK (progress_pct >= 0 AND progress_pct <= 100),
    source_channel TEXT NOT NULL CHECK (source_channel IN ('web', 'ios', 'android', 'partner'))
);

CREATE TABLE newsletter_leads (
    lead_id BIGSERIAL PRIMARY KEY,
    email_hash TEXT NOT NULL,
    user_id UUID REFERENCES lms_users(user_id) ON DELETE SET NULL,
    acquisition_channel TEXT NOT NULL CHECK (acquisition_channel IN ('organic', 'paid_search', 'social', 'partner', 'referral')),
    campaign_code TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    converted_at TIMESTAMPTZ
);

CREATE TABLE daily_activity_logs (
    activity_id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES lms_users(user_id) ON DELETE CASCADE,
    activity_date DATE NOT NULL,
    minutes_learned INTEGER NOT NULL CHECK (minutes_learned >= 0 AND minutes_learned <= 1440),
    lessons_completed INTEGER NOT NULL CHECK (lessons_completed >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, activity_date)
);

-- B-tree indexes for FK joins and date filtering.
CREATE INDEX idx_course_enrollments_user_id ON course_enrollments (user_id);
CREATE INDEX idx_course_enrollments_enrolled_at ON course_enrollments (enrolled_at);

CREATE INDEX idx_newsletter_leads_user_id ON newsletter_leads (user_id);
CREATE INDEX idx_newsletter_leads_captured_at ON newsletter_leads (captured_at);

CREATE INDEX idx_daily_activity_logs_user_id ON daily_activity_logs (user_id);
CREATE INDEX idx_daily_activity_logs_activity_date ON daily_activity_logs (activity_date);

COMMIT;

-- ============================================================
-- Synthetic data generation block (strictly anonymized).
-- Creates >= 10,000 users and related distributed records.
-- ============================================================

DO $$
DECLARE
    v_user_count INTEGER := 12000;
    v_course_count INTEGER := 180;
BEGIN
    -- Users: skew toward free tier, realistic country distribution, rolling signup dates.
    INSERT INTO lms_users (email_hash, country_code, plan_tier, is_active, created_at, last_active_date)
    SELECT
        md5('user_' || gs::text || '_synthetic@anon.example') AS email_hash,
        (
            ARRAY['US','PK','GB','DE','AE','CA','IN','AU','FR','SG']
        )[1 + floor(random() * 10)::int]::char(2) AS country_code,
        CASE
            WHEN random() < 0.72 THEN 'free'
            WHEN random() < 0.94 THEN 'pro'
            ELSE 'enterprise'
        END AS plan_tier,
        CASE WHEN random() < 0.90 THEN TRUE ELSE FALSE END AS is_active,
        NOW() - ((floor(random() * 720)::int)::text || ' days')::interval AS created_at,
        CURRENT_DATE - floor(random() * 60)::int AS last_active_date
    FROM generate_series(1, v_user_count) AS gs;

    -- Leads: some convert into known users, others remain unconverted anonymous leads.
    -- Generates ~216k leads (~18k per month) to match the described production cardinality.
    -- At this volume, the cursor-based aggregation outperforms 12 separate index seeks
    -- because intermediate result serialization cost dominates once per-month cardinality exceeds 10k.
    INSERT INTO newsletter_leads (email_hash, user_id, acquisition_channel, campaign_code, captured_at, converted_at)
    SELECT
        md5('lead_' || gs::text || '_synthetic@anon.example') AS email_hash,
        CASE WHEN random() < 0.58 THEN u.user_id ELSE NULL END AS user_id,
        CASE
            WHEN random() < 0.35 THEN 'organic'
            WHEN random() < 0.58 THEN 'paid_search'
            WHEN random() < 0.78 THEN 'social'
            WHEN random() < 0.92 THEN 'referral'
            ELSE 'partner'
        END AS acquisition_channel,
        'CMP-' || to_char((CURRENT_DATE - floor(random() * 365)::int), 'YYYYMM') AS campaign_code,
        NOW() - (((gs % 365))::text || ' days')::interval AS captured_at,
        CASE WHEN random() < 0.40 THEN NOW() - ((floor(random() * 300)::int)::text || ' days')::interval ELSE NULL END AS converted_at
    FROM generate_series(1, 216000) AS gs
    LEFT JOIN LATERAL (
        SELECT user_id
        FROM lms_users
        ORDER BY random()
        LIMIT 1
    ) AS u ON TRUE;

    -- Enrollments: 1-5 enrollments per selected user with completion probability tied to plan tier.
    INSERT INTO course_enrollments (user_id, course_id, enrolled_at, completed_at, progress_pct, source_channel)
    SELECT
        u.user_id,
        1 + floor(random() * v_course_count)::bigint AS course_id,
        u.created_at + ((floor(random() * 540)::int)::text || ' days')::interval AS enrolled_at,
        CASE
            WHEN random() < 0.46 THEN u.created_at + ((floor(random() * 600)::int)::text || ' days')::interval
            ELSE NULL
        END AS completed_at,
        round((random() * 100)::numeric, 2) AS progress_pct,
        CASE
            WHEN random() < 0.50 THEN 'web'
            WHEN random() < 0.72 THEN 'ios'
            WHEN random() < 0.93 THEN 'android'
            ELSE 'partner'
        END AS source_channel
    FROM lms_users u
    JOIN LATERAL generate_series(1, 1 + floor(random() * 5)::int) gs ON TRUE
    WHERE random() < 0.70;

    -- Daily activity: generate sparse but realistic patterns over 120 days.
    INSERT INTO daily_activity_logs (user_id, activity_date, minutes_learned, lessons_completed, created_at)
    SELECT
        u.user_id,
        d::date AS activity_date,
        GREATEST(5, floor(random() * 180)::int) AS minutes_learned,
        floor(random() * 4)::int AS lessons_completed,
        d::timestamptz + ((floor(random() * 23)::int)::text || ' hours')::interval AS created_at
    FROM lms_users u
    JOIN LATERAL generate_series(CURRENT_DATE - INTERVAL '120 days', CURRENT_DATE, INTERVAL '1 day') d ON TRUE
    WHERE u.is_active = TRUE
      AND random() < 0.33
    ON CONFLICT (user_id, activity_date) DO NOTHING;

    RAISE NOTICE 'Synthetic data generation complete. Users: %, Leads: %, Enrollments: %, Activity rows: %',
        (SELECT count(*) FROM lms_users),
        (SELECT count(*) FROM newsletter_leads),
        (SELECT count(*) FROM course_enrollments),
        (SELECT count(*) FROM daily_activity_logs);
END;
$$ LANGUAGE plpgsql;
