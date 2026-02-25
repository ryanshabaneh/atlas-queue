-- pgcrypto provides gen_random_uuid().
-- Built-in since PostgreSQL 13, but enabling the extension here makes
-- the schema portable to PostgreSQL 12 and makes the dependency explicit.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
