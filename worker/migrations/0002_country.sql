-- Country of the peer (from the public-peers markdown file it is listed in),
-- backfilled by the collector via row rewrites (heartbeat).
ALTER TABLE peers ADD COLUMN country TEXT;
