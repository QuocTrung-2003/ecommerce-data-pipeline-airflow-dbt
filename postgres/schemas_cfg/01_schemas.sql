-- Schemas
-- ----------------------
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS intermediate_data;
CREATE SCHEMA IF NOT EXISTS mart_analytics;

-- =============================================================================
-- RAW LANDING TABLES
-- =============================================================================

-- Customers
CREATE TABLE IF NOT EXISTS raw_data.customers (
    customer_id UUID PRIMARY KEY,
    company_name TEXT,
    country TEXT,
    industry TEXT,
    company_size TEXT,
    signup_date TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    is_churned BOOLEAN
);

-- Products 
CREATE TABLE IF NOT EXISTS raw_data.products (
    product_id UUID PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price NUMERIC,
    currency TEXT,
    updated_at TIMESTAMPTZ
);

-- Orders
CREATE TABLE IF NOT EXISTS raw_data.orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID,
    total_amount NUMERIC,
    status TEXT,
    payment_method TEXT,
    country TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON raw_data.orders(customer_id);

-- Order Items
CREATE TABLE IF NOT EXISTS raw_data.order_items (
    order_item_id UUID PRIMARY KEY,
    order_id UUID,
    product_id UUID,
    quantity INT,
    price NUMERIC,
    total_price NUMERIC,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON raw_data.order_items(order_id);

-- Visits (REPLACE sessions)
CREATE TABLE IF NOT EXISTS raw_data.visits (
    visit_id UUID PRIMARY KEY,
    customer_id UUID,
    source TEXT,
    medium TEXT,
    device TEXT,
    country TEXT,
    pageviews INT,
    session_duration_s INT,
    bounced INT,
    converted INT,
    visit_start TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_visits_customer ON raw_data.visits(customer_id);