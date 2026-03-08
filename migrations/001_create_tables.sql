-- NAD Reports schema for Neon Postgres
-- Run on both main and dev branches after common-tables.sql

-- Common infrastructure tables
CREATE TABLE IF NOT EXISTS ingestion_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    records_added INTEGER NOT NULL DEFAULT 0,
    records_updated INTEGER NOT NULL DEFAULT 0,
    records_skipped INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    error_details JSONB,
    status TEXT NOT NULL DEFAULT 'running'
        CONSTRAINT ck_ingestion_log_status CHECK (status IN ('running', 'success', 'partial', 'failed'))
);

CREATE TABLE IF NOT EXISTS sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    url TEXT,
    scrape_frequency TEXT,
    last_run_at TIMESTAMPTZ,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS data_quality (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name TEXT NOT NULL,
    record_id UUID NOT NULL,
    issue_type TEXT NOT NULL,
    details TEXT,
    flagged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);

-- Collected report URLs (replaces MongoDB Urls collection)
CREATE TABLE IF NOT EXISTS urls (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date TEXT NOT NULL,
    title TEXT,
    link TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_urls_link UNIQUE (link)
);

CREATE INDEX IF NOT EXISTS idx_urls_date ON urls(date);

-- Daily reports: one row per NAD daily report
CREATE TABLE IF NOT EXISTS nad_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id TEXT NOT NULL,
    source_url TEXT,
    report_date DATE NOT NULL,
    title_arabic TEXT,
    title_english TEXT,
    raw_data JSONB,
    scraped_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingestion_id UUID REFERENCES ingestion_log(id),
    CONSTRAINT uq_nad_reports_source_id UNIQUE (source_id)
);

CREATE INDEX IF NOT EXISTS idx_nad_reports_source_id ON nad_reports(source_id);
CREATE INDEX IF NOT EXISTS idx_nad_reports_report_date ON nad_reports(report_date);

-- Narrative violations: one row per violation detail within a report
CREATE TABLE IF NOT EXISTS nad_narrative_violations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id UUID NOT NULL REFERENCES nad_reports(id) ON DELETE CASCADE,
    region TEXT,
    region_arabic TEXT,
    governorate TEXT,
    governorate_arabic TEXT,
    violation_type TEXT,
    violation_type_arabic TEXT,
    description_english TEXT,
    description_arabic TEXT,
    translation_source TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nad_narrative_violations_report_id ON nad_narrative_violations(report_id);
CREATE INDEX IF NOT EXISTS idx_nad_narrative_violations_region ON nad_narrative_violations(region);
CREATE INDEX IF NOT EXISTS idx_nad_narrative_violations_violation_type ON nad_narrative_violations(violation_type);

-- Seed the sources table
INSERT INTO sources (name, url, scrape_frequency, notes)
VALUES (
    'NAD Daily Reports',
    'https://www.nad.ps/ar/violations-reports/daily-report',
    'daily',
    'PLO Negotiations Affairs Department daily incident reports. ~3,275 reports from 2017-present.'
)
ON CONFLICT DO NOTHING;
