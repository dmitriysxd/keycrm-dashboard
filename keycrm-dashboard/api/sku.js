const { getSupabase } = require("../lib/supabase");
const { checkDashboardToken } = require("../lib/auth");

async function fetchAll(buildQuery, pageSize = 1000, hardCap = 20000) {
  const out = [];
  for (let from = 0; from < hardCap; from += pageSize) {
    const to = from + pageSize - 1;
    const { data, error } = await buildQuery().range(from, to);
    if (error) throw new Error(error.message);
    if (!data || !data.length) break;
    out.push(...data);
    if (data.length < pageSize) break;
  }
  return out;
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,PATCH,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();

  try {
    // PATCH — оновлення картки SKU вручну. Дозволяє пофіксити дату оприбуткування
    // якщо ingest пропустив снепшоти і detect_restocks встановив last_restock_at
    // на неправильну дату. Також править cost/manual_status/notes.
    // Після PATCH автоматично рефрешимо sku_metrics, щоб зміни одразу видно було.
    if (req.method === "PATCH") {
      let body = req.body;
      if (!body || typeof body !== "object") {
        body = await new Promise((resolve, reject) => {
          let raw = "";
          req.on("data", c => { raw += c; });
          req.on("end", () => {
            if (!raw) return resolve({});
            try { resolve(JSON.parse(raw)); } catch (e) { reject(new Error("invalid JSON")); }
          });
          req.on("error", reject);
        });
      }
      const offerId = parseInt(body.offer_id || (req.query && req.query.offer_id));
      if (!offerId) return res.status(400).json({ error: "offer_id required" });

      const patch = {};
      if (body.last_restock_at !== undefined) {
        const v = body.last_restock_at;
        if (v === null || v === "") patch.last_restock_at = null;
        else if (/^\d{4}-\d{2}-\d{2}$/.test(String(v))) patch.last_restock_at = String(v);
        else return res.status(400).json({ error: "last_restock_at must be YYYY-MM-DD or null" });
      }
      if (body.cost !== undefined) {
        if (body.cost === null || body.cost === "") patch.cost = null;
        else {
          const n = parseFloat(body.cost);
          if (isNaN(n)) return res.status(400).json({ error: "cost must be a number" });
          patch.cost = n;
        }
      }
      if (body.manual_status !== undefined) {
        if (body.manual_status === null || body.manual_status === "") patch.manual_status = null;
        else if (["hit", "good", "slow", "weak", "dead", "new", "archive"].includes(String(body.manual_status))) {
          patch.manual_status = String(body.manual_status);
        } else return res.status(400).json({ error: "invalid manual_status" });
      }
      if (body.notes !== undefined) {
        patch.notes = body.notes === null ? null : String(body.notes).slice(0, 2000);
      }

      if (!Object.keys(patch).length) return res.status(400).json({ error: "nothing to update" });

      const { error: upErr } = await supabase.from("skus").update(patch).eq("offer_id", offerId);
      if (upErr) throw new Error("skus update: " + upErr.message);

      // Зміни (особливо last_restock_at) впливають на age_days в matview. Рефреш
      // тут робимо CONCURRENTLY щоб не блокувати читачів і щоб виклик не виснув
      // довше за Vercel timeout. Якщо CONCURRENTLY не підтримується (unique index
      // нема) — fallback на звичайний.
      try {
        await supabase.rpc("refresh_sku_metrics");
      } catch (e) {
        // Не валимо PATCH через refresh — просто логуємо. Користувач побачить
        // зміни після наступного автоматичного рефрешу.
      }

      return res.status(200).json({ ok: true, patched: patch });
    }

    if (req.query && req.query.meta === "true") {
      const headCount = (build) => build().then((r) => r.count || 0);
      const head = (extra) => {
        let q = supabase.from("sku_metrics").select("offer_id", { count: "exact", head: true });
        if (extra) q = extra(q);
        return q;
      };
      const catRows = await fetchAll(
        () => supabase.from("skus").select("category_id, category_name").not("category_id", "is", null),
        1000,
        20000
      );
      const catMap = new Map();
      for (const r of catRows) {
        if (r.category_id != null && !catMap.has(r.category_id)) {
          catMap.set(r.category_id, r.category_name || ("id:" + r.category_id));
        }
      }
      const categories = Array.from(catMap.entries())
        .map(([id, name]) => ({ id, name }))
        .sort((a, b) => String(a.name).localeCompare(String(b.name)));

      const [snap, run, totalAll, totalActive, hits, good, slow, weak, dead, isnew, archive, inStock, season, star, cashCow, question, dog] = await Promise.all([
        supabase.from("stock_snapshots").select("snapshot_date").order("snapshot_date", { ascending: false }).limit(1),
        supabase.from("ingest_runs").select("id, kind, status, started_at, finished_at, error_message").order("started_at", { ascending: false }).limit(1),
        head(),
        head((q) => q.eq("is_active", true)),
        head((q) => q.eq("is_active", true).eq("status", "hit")),
        head((q) => q.eq("is_active", true).eq("status", "good")),
        head((q) => q.eq("is_active", true).eq("status", "slow")),
        head((q) => q.eq("is_active", true).eq("status", "weak")),
        head((q) => q.eq("is_active", true).eq("status", "dead")),
        head((q) => q.eq("is_active", true).eq("status", "new")),
        head((q) => q.eq("is_active", true).eq("status", "archive")),
        head((q) => q.eq("is_active", true).gt("current_stock", 0)),
        // current_season_info() RPC — повертає назву/мультиплікатор активного сезону.
        supabase.rpc("current_season_info"),
        head((q) => q.eq("is_active", true).eq("bcg_role", "star")),
        head((q) => q.eq("is_active", true).eq("bcg_role", "cash_cow")),
        head((q) => q.eq("is_active", true).eq("bcg_role", "question")),
        head((q) => q.eq("is_active", true).eq("bcg_role", "dog")),
      ]);
      return res.status(200).json({
        last_snapshot_date: (snap.data && snap.data[0] && snap.data[0].snapshot_date) || null,
        last_run: (run.data && run.data[0]) || null,
        total: totalAll.count || 0,
        active_total: totalActive.count || 0,
        active_in_stock: inStock.count || 0,
        active_by_status: {
          hit:     hits.count || 0,
          good:    good.count || 0,
          slow:    slow.count || 0,
          weak:    weak.count || 0,
          dead:    dead.count || 0,
          new:     isnew.count || 0,
          archive: archive.count || 0,
        },
        active_by_bcg: {
          star:     star.count || 0,
          cash_cow: cashCow.count || 0,
          question: question.count || 0,
          dog:      dog.count || 0,
        },
        current_season: (season && season.data && season.data[0]) || null,
        categories,
        now: new Date().toISOString(),
      });
    }

    const includeInactive = req.query && req.query.all === "true";
    const status = req.query && req.query.status;
    const category = req.query && req.query.category;
    const bcg = req.query && req.query.bcg;
    const inStockOnly = req.query && req.query.inStock === "true";
    const reorder = req.query && req.query.reorder === "true";
    const includeArchive = req.query && req.query.inclArchive === "true";

    const buildQuery = () => {
      let q = supabase.from("sku_metrics").select("*");
      if (!includeInactive) q = q.eq("is_active", true);
      if (status) q = q.eq("status", status);
      else if (!includeArchive) q = q.neq("status", "archive");
      if (category) q = q.eq("category_id", category);
      if (bcg) q = q.eq("bcg_role", bcg);
      if (inStockOnly) q = q.gt("current_stock", 0);
      if (reorder) q = q.eq("status", "hit").lt("days_of_supply", 60);
      q = q.order("velocity_30d", { ascending: false });
      return q;
    };

    const data = await fetchAll(buildQuery);

    const search = req.query && req.query.search ? String(req.query.search).toLowerCase().trim() : "";
    const filtered = search
      ? data.filter((r) => {
          const hay = ((r.name || "") + " " + (r.sku || "") + " " + (r.category_name || "")).toLowerCase();
          return hay.indexOf(search) !== -1;
        })
      : data;

    return res.status(200).json({ rows: filtered, count: filtered.length, total_in_db: data.length });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
