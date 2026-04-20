#!/bin/bash
set -e

BI_USER=$(cat /run/secrets/bi_user)
BI_PASS=$(cat /run/secrets/bi_password)

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-EOSQL

DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$BI_USER') THEN
        EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '$BI_USER', '$BI_PASS');
    ELSE
        EXECUTE format('ALTER ROLE %I WITH LOGIN PASSWORD %L', '$BI_USER', '$BI_PASS');
    END IF;
END
\$\$;

GRANT CONNECT ON DATABASE $POSTGRES_DB TO "$BI_USER";

-- schemas
GRANT USAGE ON SCHEMA stg_raw TO "$BI_USER";
GRANT USAGE ON SCHEMA analytics_marts TO "$BI_USER";

-- tables
GRANT SELECT ON ALL TABLES IN SCHEMA stg_raw TO "$BI_USER";
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_marts TO "$BI_USER";

-- sequences
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA stg_raw TO "$BI_USER";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA analytics_marts TO "$BI_USER";

-- default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA stg_raw
GRANT SELECT ON TABLES TO "$BI_USER";


ALTER DEFAULT PRIVILEGES IN SCHEMA analytics_marts
GRANT SELECT ON TABLES TO "$BI_USER";

-- search path
ALTER ROLE "$BI_USER" SET search_path TO analytics_marts, stg_raw;

EOSQL