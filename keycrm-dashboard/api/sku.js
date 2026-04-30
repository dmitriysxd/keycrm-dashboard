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
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();

  try {
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

      const [snap, run, totalAll, totalActive, hits, good, slow, weak, dead, isnew, archive, inStock] = await Promise.all([
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
        categories,
        now: new Date().toISOString(),
      });
    }

    const includeInactive = req.query && req.query.all === "true";
    const status = req.query && req.query.status;
    const category = req.query && req.query.category;
    const inStockOnly = req.query && req.query.inStock === "true";
    const reorder = req.query && req.query.reorder === "true";
    const includeArchive = req.query && req.query.inclArchive === "true";

    const buildQuery = () => {
      let q = supabase.from("sku_metrics").select("*");
      if (!includeInactive) q = q.eq("is_active", true);
      if (status) q = q.eq("status", status);
      else if (!includeArchive) q = q.neq("status", "archive");
      if (category) q = q.eq("category_id", category);
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
