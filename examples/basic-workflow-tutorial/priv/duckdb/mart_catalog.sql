-- The Mart phase is its own DuckDB catalog, so a Mart relation addresses as
-- mart.<domain>.<name>. A resource script runs once, when the physical session
-- is created — not on every checkout from the pool.
ATTACH @database_path AS mart;
