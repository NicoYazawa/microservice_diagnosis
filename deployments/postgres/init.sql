-- mfdh PostgreSQL init script (M0)
-- Session / fix / approval / knowledge base / webhook tables will be created in the M3 milestone.
-- Only extensions are created for now, so UUID primary keys can be used later.
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
