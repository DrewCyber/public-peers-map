/**
 * ymonitor-api — public JSON API + authenticated ingest endpoint.
 *
 * GET endpoints are open (CORS *); POST /v1/ingest requires a bearer token.
 * Data lives in D1 (SQLite). See worker/migrations/ for the schema.
 */

export interface Env {
  DB: D1Database;
  INGEST_TOKEN: string;
}

const KEY_RE = /^[0-9a-f]{64}$/;
const MAX_PEERS = 1000;
const MAX_EVENTS = 10000;
const BATCH_CHUNK = 50;

interface PeerRow {
  key: string;
  ipv6: string;
  endpoints: string[];
  coords: string | null;
  last_answer_ts: number | null;
}
interface EventRow {
  ts: number;
  peer_key: string;
  old_coords: string | null;
  new_coords: string | null;
}
interface IngestBody {
  poll_ts: number;
  peers?: PeerRow[];
  events?: EventRow[];
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.method === "OPTIONS") return cors(new Response(null, { status: 204 }));
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";

    try {
      if (request.method === "GET" || request.method === "HEAD") {
        if (path === "/v1/peers") return cors(await getPeers(env));
        if (path === "/v1/tree") return cors(await getTree(env));
        if (path === "/v1/stats") return cors(await getStats(env));
        if (path === "/healthz") return cors(json({ ok: true }));
        const m = path.match(/^\/v1\/peers\/([0-9a-fA-F]{64})$/);
        if (m) return cors(await getPeer(env, m[1].toLowerCase()));
        const h = path.match(/^\/v1\/peers\/([0-9a-fA-F]{64})\/history$/);
        if (h) return cors(await getHistory(env, h[1].toLowerCase(), url));
      }
      if (request.method === "POST" && path === "/v1/ingest") return cors(await ingest(request, env));
      return cors(json({ error: "not found" }, 404));
    } catch (err) {
      return cors(json({ error: String(err) }, 500));
    }
  },
};

// ---------- handlers ----------

async function getPeers(env: Env): Promise<Response> {
  const { results } = await env.DB.prepare(
    "SELECT key, ipv6, endpoints, coords, updated_at, last_answer_ts FROM peers ORDER BY key"
  ).all<DbPeer>();
  const peers = (results ?? []).map((r) => ({
    key: r.key,
    ipv6: r.ipv6,
    endpoints: JSON.parse(r.endpoints) as string[],
    coords: r.coords,
    updated_at: r.updated_at,
    last_answer_ts: r.last_answer_ts,
  }));
  const present = peers.filter((p) => p.coords !== null).length;
  return json({ as_of: await pollTs(env), count: peers.length, present, peers });
}

async function getPeer(env: Env, key: string): Promise<Response> {
  const r = await env.DB.prepare(
    "SELECT key, ipv6, endpoints, coords, updated_at, last_answer_ts FROM peers WHERE key = ?"
  )
    .bind(key)
    .first<DbPeer>();
  if (!r) return json({ error: "peer not found" }, 404);
  return json({
    key: r.key,
    ipv6: r.ipv6,
    endpoints: JSON.parse(r.endpoints) as string[],
    coords: r.coords,
    updated_at: r.updated_at,
    last_answer_ts: r.last_answer_ts,
  });
}

async function getHistory(env: Env, key: string, url: URL): Promise<Response> {
  const from = intParam(url.searchParams.get("from"));
  const to = intParam(url.searchParams.get("to"));
  const limit = Math.min(intParam(url.searchParams.get("limit")) ?? 1000, 10000);
  let sql = "SELECT ts, old_coords, new_coords FROM events WHERE peer_key = ?";
  const args: unknown[] = [key];
  if (from !== null) (sql += " AND ts >= ?"), args.push(from);
  if (to !== null) (sql += " AND ts <= ?"), args.push(to);
  sql += " ORDER BY ts DESC LIMIT ?";
  args.push(limit);
  const { results } = await env.DB.prepare(sql).bind(...args).all<EventRow>();
  return json({ key, events: results ?? [] });
}

// getTree returns the minimal snapshot an interactive tree UI needs: the
// peers that are currently in the tree with their coordinates. Dots (non-
// public ancestor nodes) and edges are NOT included — both are derived on
// the client from coords alone:
//   * parent of "1.2.3" is "1.2"; the implicit root is "" (a peer with
//     coords "" IS the root);
//   * every strict prefix of any coords is a "dot" node; chains of degree-1
//     dots may be freely path-compressed for readability.
async function getTree(env: Env): Promise<Response> {
  const { results } = await env.DB.prepare(
    "SELECT coords, key, ipv6 FROM peers WHERE coords IS NOT NULL ORDER BY key"
  ).all<{ coords: string; key: string; ipv6: string }>();
  return json({
    as_of: await pollTs(env),
    // field order matters for readability: structure first, then identity
    peers: (results ?? []).map((r) => ({ coords: r.coords, key: r.key, ipv6: r.ipv6 })),
  });
}

async function getStats(env: Env): Promise<Response> {
  const peers = await env.DB.prepare(
    "SELECT COUNT(*) AS total, SUM(coords IS NOT NULL) AS present FROM peers"
  ).first<{ total: number; present: number }>();
  const events = await env.DB.prepare("SELECT COUNT(*) AS total FROM events").first<{ total: number }>();
  const dayAgo = Math.floor(Date.now() / 1000) - 86400;
  const recent = await env.DB.prepare("SELECT COUNT(*) AS total FROM events WHERE ts >= ?")
    .bind(dayAgo)
    .first<{ total: number }>();
  return json({
    as_of: await pollTs(env),
    peers_total: peers?.total ?? 0,
    peers_present: peers?.present ?? 0,
    events_total: events?.total ?? 0,
    events_24h: recent?.total ?? 0,
  });
}

async function ingest(request: Request, env: Env): Promise<Response> {
  if (!env.INGEST_TOKEN) return json({ error: "ingest token not configured" }, 500);
  const auth = request.headers.get("Authorization") ?? "";
  const expected = `Bearer ${env.INGEST_TOKEN}`;
  if (auth.length !== expected.length || !timingSafeEqual(auth, expected)) {
    return json({ error: "unauthorized" }, 401);
  }

  let body: IngestBody;
  try {
    body = (await request.json()) as IngestBody;
  } catch {
    return json({ error: "invalid json" }, 400);
  }
  if (!Number.isInteger(body.poll_ts) || body.poll_ts <= 0) {
    return json({ error: "poll_ts must be a unix timestamp" }, 400);
  }
  const peers = body.peers ?? [];
  const events = body.events ?? [];
  if (peers.length > MAX_PEERS) return json({ error: `too many peers (max ${MAX_PEERS})` }, 400);
  if (events.length > MAX_EVENTS) return json({ error: `too many events (max ${MAX_EVENTS})` }, 400);

  for (const p of peers) {
    if (
      !KEY_RE.test(p.key) ||
      typeof p.ipv6 !== "string" ||
      !Array.isArray(p.endpoints) ||
      (p.last_answer_ts !== null && p.last_answer_ts !== undefined && !Number.isInteger(p.last_answer_ts))
    ) {
      return json({ error: "bad peer row" }, 400);
    }
  }
  for (const e of events) {
    if (!Number.isInteger(e.ts) || !KEY_RE.test(e.peer_key ?? "")) {
      return json({ error: "bad event row" }, 400);
    }
  }

  const stmts: D1PreparedStatement[] = [];
  for (const p of peers) {
    stmts.push(
      env.DB.prepare(
        `INSERT INTO peers (key, ipv6, endpoints, coords, updated_at, last_answer_ts)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(key) DO UPDATE SET
           ipv6 = excluded.ipv6, endpoints = excluded.endpoints,
           coords = excluded.coords, updated_at = excluded.updated_at,
           last_answer_ts = excluded.last_answer_ts`
      ).bind(p.key, p.ipv6, JSON.stringify(p.endpoints), p.coords, body.poll_ts, p.last_answer_ts ?? null)
    );
  }
  for (const e of events) {
    stmts.push(
      env.DB.prepare(
        `INSERT OR IGNORE INTO events (ts, peer_key, old_coords, new_coords)
         VALUES (?1, ?2, ?3, ?4)`
      ).bind(e.ts, e.peer_key, e.old_coords, e.new_coords)
    );
  }
  stmts.push(
    env.DB.prepare(
      `INSERT INTO meta (key, value) VALUES ('poll_ts', ?1)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`
    ).bind(String(body.poll_ts))
  );

  for (let i = 0; i < stmts.length; i += BATCH_CHUNK) {
    await env.DB.batch(stmts.slice(i, i + BATCH_CHUNK));
  }
  return json({ ok: true, applied: { peers: peers.length, events: events.length } });
}

async function pollTs(env: Env): Promise<number> {
  const r = await env.DB.prepare("SELECT value FROM meta WHERE key = 'poll_ts'").first<{ value: string }>();
  return Number(r?.value ?? 0);
}

// ---------- helpers ----------

interface DbPeer {
  key: string;
  ipv6: string;
  endpoints: string;
  coords: string | null;
  updated_at: number;
  last_answer_ts: number | null;
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "public, max-age=30",
    },
  });
}

function cors(res: Response): Response {
  const out = new Response(res.body, res);
  out.headers.set("Access-Control-Allow-Origin", "*");
  out.headers.set("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
  out.headers.set("Access-Control-Allow-Headers", "Authorization, Content-Type");
  return out;
}

function intParam(v: string | null): number | null {
  if (v === null || v === "") return null;
  const n = Number(v);
  return Number.isInteger(n) ? n : null;
}

// Constant-time string comparison (both are ASCII, same length checked).
function timingSafeEqual(a: string, b: string): boolean {
  const A = new TextEncoder().encode(a);
  const B = new TextEncoder().encode(b);
  let diff = 0;
  for (let i = 0; i < Math.max(A.length, B.length); i++) {
    diff |= (A[i] ?? 0) ^ (B[i] ?? 0);
  }
  return diff === 0;
}
