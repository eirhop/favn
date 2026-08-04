-- The Core phase is its own DuckDB catalog, so a Core relation addresses as
-- core.<domain>.<name>. A resource script runs once, when the physical session
-- is created — not on every checkout from the pool.
ATTACH @database_path AS core;
