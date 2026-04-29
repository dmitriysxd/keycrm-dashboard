const { getSupabase } = require("../lib/supabase");
const { checkDashboardToken } = require("../lib/auth");

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();

  try {
    if (req.query && req.query.meta === "true") {
      const [snap, run, totalAll, totalActive, hits, slow, dead, inStock] = await Promise.all([
        supabase.from("stock_snapshots").select("snapshot_date").order("snapshot_date", { ascending: false }).limit(1),
        supabase.from("ingest_runs").select("id, kind, status, started_at, finished_at, error_message").order("started_at", { ascending: false }).limit(1),
        supabase.from("sku_metrics").select("offer_id", { count: "exact", head: true }),
        supabase.from("sku_metrics").select("offer_id", { count: "exact", head: true }).eq("is_active", true),
        supabase.from("sku_metrics").select("offer_id", { count: "exact", head: true }).eq("is_active", true).eq("status", "hit"),
        supabase.from("sku_metrics").select("offer_id", { count: "exact", head: true }).eq("is_active", true).eq("status", "slow"),
        supabase.from("sku_metrics").select("offer_id", { count: "exact", head: true }).eq("is_active", true).eq("status", "dead"),
        supabase.from("sku_metrics").select("offer_id", { count: "exact", head: true }).eq("is_active", true).gt("current_stock", 0),
      ]);
      return res.status(200).json({
        last_snapshot_date: (snap.data && snap.data[0] && snap.data[0].snapshot_date) || null,
        last_run: (run.data && run.data[0]) || null,
        total: totalAll.count || 0,
        active_total: totalActive.count || 0,
        active_in_stock: inStock.count || 0,
        active_by_status: {
          hit:  hits.count || 0,
          slow: slow.count || 0,
          dead: dead.count || 0,
        },
        now: new Date().toISOString(),
      });
    }

    let q = supabase.from("sku_metrics").select("*", { count: "exact" });

    const includeInactive = req.query && req.query.all === "true";
    if (!includeInactive) q = q.eq("is_active", true);

    if (req.query) {
      if (req.query.status) q = q.eq("status", req.query.status);
      if (req.query.category) q = q.eq("category_id", req.query.category);
      if (req.query.inStock === "true") q = q.gt("current_stock", 0);
      if (req.query.reorder === "true") {
        q = q.eq("status", "hit").lt("days_of_supply", 30);
      }
    }

    q = q.order("velocity_30d", { ascending: false }).range(0, 9999);

    const { data, count, error } = await q;
    if (error) throw new Error(error.message);

    const search = req.query && req.query.search ? String(req.query.search).toLowerCase().trim() : "";
    const filtered = search
      ? (data || []).filter((r) => {
          const hay = ((r.name || "") + " " + (r.sku || "") + " " + (r.category_name || "")).toLowerCase();
          return hay.indexOf(search) !== -1;
        })
      : (data || []);

    return res.status(200).json({ rows: filtered, count: filtered.length, total_in_db: count || (data || []).length });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
