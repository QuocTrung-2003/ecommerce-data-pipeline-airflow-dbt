-- Schemas
-- ----------------------
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS public_staging;
CREATE SCHEMA IF NOT EXISTS public_analytics;

-- =============================================================================
-- RAW LANDING TABLES
-- =============================================================================

-- Customers
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id UUID PRIMARY KEY,
    company_name TEXT,
    country TEXT,
    industry TEXT,
    company_size TEXT,
    signup_date TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    is_churned BOOLEAN
);