const { createClient } = require("@supabase/supabase-js");

let client = null;

// fetch з таймаутом 30с для ВСІХ запитів до Supabase: завислий мережевий
// запит (конект/читання) не блокує функцію до її ліміту. 30с > найдовшого
// легального запиту (рефреш matview ~4-15с, marketing RPC ≤25с), тож нічого
// не ламає, але зависання обмежує.
function supabaseFetch(url, opts) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 30000);
  const merged = Object.assign({}, opts, { signal: (opts && opts.signal) || controller.signal });
  return fetch(url, merged).finally(() => clearTimeout(timer));
}

function getSupabase() {
  if (client) return client;
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    throw new Error("SUPABASE_URL та SUPABASE_SERVICE_ROLE_KEY мають бути налаштовані");
  }
  client = createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
    db: { schema: "public" },
    global: { fetch: supabaseFetch },
  });
  return client;
}

module.exports = { getSupabase };
