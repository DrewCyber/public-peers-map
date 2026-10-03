-- ymonitor schema: current peer state + coordinate change log.
-- Rows are written by the collector (POST /v1/ingest, bearer token);
-- everything is readable via the public GET endpoints.

CREATE TABLE IF NOT EXISTS peers (
  key        TEXT PRIMARY KEY,  -- ed25519 public key, hex
  ipv6       TEXT NOT NULL,     -- 200::/7 address derived from the key
  endpoints  TEXT NOT NULL DEFAULT '[]', -- JSON array of peer URIs
  coords     TEXT,              -- '1.2.3' (path from root); '' = root node; NULL = not in tree
  updated_at INTEGER NOT NULL,  -- unix ts of the last row write (change or heartbeat)
  last_answer_ts INTEGER        -- unix ts of the last poll where the peer answered a probe;
                                -- NULL = never answered. Drives the stateless vanish debounce.
);

CREATE TABLE IF NOT EXISTS events (
  ts         INTEGER NOT NULL,  -- unix ts of the poll that observed the change
  peer_key   TEXT NOT NULL,
  old_coords TEXT,              -- NULL in old = appeared in the tree
  new_coords TEXT               -- NULL in new = disappeared from the tree
);
CREATE INDEX IF NOT EXISTS idx_events_peer_ts ON events(peer_key, ts);
-- Idempotency for retried ingest batches ('~' sentinel: '' is a valid root
-- coords value and must stay distinct from NULL).
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_uniq
  ON events(ts, peer_key, IFNULL(old_coords, '~'), IFNULL(new_coords, '~'));

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
INSERT OR IGNORE INTO meta(key, value) VALUES('poll_ts', '0');
