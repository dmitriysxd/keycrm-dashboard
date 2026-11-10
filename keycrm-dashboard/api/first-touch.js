// Збір "першого джерела" (first-touch) клієнта з сайту Horoshop.
//
// POST /api/first-touch
//   Body (text/plain JSON, шлеться через navigator.sendBeacon зі скрипта на
//   сайті): { phone, ft:{s,m,c,ct,t,ref,lp,ts}, lt:{s,m,c,ct,t}, href }
//   Пишемо insert-only перше джерело по телефону (RPC record_first_touch).
//   ЗАВЖДИ повертаємо 204 без тіла — щоб публічний beacon ніколи не бачив
//   помилок, а сайт нічого не "відчував". Жодних даних назовні не віддаємо.
//
// GET /api/first-touch[?phone=...]
//   Службова перевірка (потребує DASHBOARD_TOKEN, якщо він налаштований):
//   без phone — total + останні 10 записів; з phone — нормалізація і рядок.
//
// CORS: відкритий (відбиваємо Origin) — ендпоінт атрибуційний, ключем
// служить лише телефон, що й так є у формі сайту.

const { getSupabase } = require("../lib/supabase");
const { checkDashboardToken } = require("../lib/auth");

const MAX_BODY = 8 * 1024; // 8 KB — більше не треба, захист від роздування

function setCors(req, res) {
  const origin = (req.headers && req.headers.origin) || "*";
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Dashboard-Token");
  res.setHeader("Access-Control-Max-Age", "86400");
}

// Читаємо тіло: Vercel не завжди парсить text/plain, тож беремо або вже
// розпарсений req.body, або сирий потік (з жорстким лімітом розміру).
function readBody(req) {
  return new Promise((resolve) => {
    try {
      if (req.body != null) {
        if (typeof req.body === "string") return resolve(req.body.slice(0, MAX_BODY));
        if (typeof req.body === "object") return resolve(JSON.stringify(req.body).slice(0, MAX_BODY));
      }
    } catch (e) { /* ignore */ }
    let data = "";
    try {
      req.on("data", (c) => {
        data += c;
        if (data.length > MAX_BODY) { try { req.destroy(); } catch (e) {} }
      });
      req.on("end", () => resolve(data.slice(0, MAX_BODY)));
      req.on("error", () => resolve(""));
    } catch (e) { resolve(""); }
  });
}

function clip(v, n) {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s.slice(0, n || 200) : null;
}

// Санітизуємо об'єкт "дотику" — лише відомі поля, обрізані по довжині.
function cleanTouch(t) {
  if (!t || typeof t !== "object") return {};
  const out = {
    s: clip(t.s), m: clip(t.m), c: clip(t.c), ct: clip(t.ct), t: clip(t.t),
    ref: clip(t.ref), lp: clip(t.lp),
  };
  if (typeof t.ts === "number" && isFinite(t.ts)) out.ts = Math.floor(t.ts);
  // прибираємо null-поля, щоб jsonb був компактний
  Object.keys(out).forEach((k) => { if (out[k] == null) delete out[k]; });
  return out;
}

module.exports = async (req, res) => {
  setCors(req, res);

  if (req.method === "OPTIONS") return res.status(204).end();

  // ── GET: службова перевірка ────────────────────────────
  if (req.method === "GET") {
    const auth = checkDashboardToken(req);
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });
    try {
      const supabase = getSupabase();
      const phone = req.query && req.query.phone;
      if (phone) {
        const norm = await supabase.rpc("normalize_phone", { _raw: String(phone) });
        if (norm.error) throw new Error(norm.error.message);
        const pn = norm.data;
        let row = null;
        if (pn) {
          const r = await supabase.from("first_touch").select("*").eq("phone_norm", pn).maybeSingle();
          row = r.data || null;
        }
        return res.status(200).json({ input: String(phone), normalized: pn, row });
      }
      const head = await supabase.from("first_touch").select("*", { count: "exact", head: true });
      const recent = await supabase
        .from("first_touch")
        .select("phone_norm, first_source, first_medium, first_campaign, last_source, hits, created_at")
        .order("created_at", { ascending: false })
        .limit(10);
      return res.status(200).json({ total: head.count || 0, recent: recent.data || [] });
    } catch (e) {
      return res.status(500).json({ error: String((e && e.message) || e) });
    }
  }

  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  // ── POST: приймаємо beacon ─────────────────────────────
  // З цього місця ЗАВЖДИ 204 — жодних помилок у публічний канал.
  const raw = await readBody(req);
  let body = null;
  try { body = JSON.parse(raw); } catch (e) { return res.status(204).end(); }
  if (!body || typeof body !== "object") return res.status(204).end();

  const phone = clip(body.phone, 40);
  if (!phone) return res.status(204).end();

  const ft = cleanTouch(body.ft);
  const lt = cleanTouch(body.lt);
  const rawStore = {
    ft, lt,
    href: clip(body.href, 300),
    origin: clip(req.headers && req.headers.origin, 120),
    ref: clip(req.headers && req.headers.referer, 300),
    ua: clip(req.headers && req.headers["user-agent"], 200),
    at: new Date().toISOString(),
  };

  try {
    const supabase = getSupabase();
    const { error } = await supabase.rpc("record_first_touch", {
      _phone: phone, _ft: ft, _lt: lt, _raw: rawStore,
    });
    if (error) throw new Error(error.message);
  } catch (e) {
    try { console.error("first-touch error:", (e && e.message) || e); } catch (_) {}
  }
  return res.status(204).end();
};
