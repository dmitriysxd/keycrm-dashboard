const { getSupabase } = require("../lib/supabase");
const { checkDashboardToken } = require("../lib/auth");

const EXCLUDED_STATUSES = new Set([
  "cancelled", "rejected", "canceled",
  "Повернули", "Відмовились",
  "incorrect_data", "underbid",
]);

function isMergerStatus(status) {
  // Catches both ASCII and typographic apostrophe in "Об'єднання замовлень"
  if (!status) return false;
  return /^Об.{1,2}єднання замовлень$/.test(status);
}

async function fetchAll(buildQuery, pageSize = 1000, hardCap = 50000) {
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

  const offerId = req.query && req.query.offer_id;
  if (!offerId) return res.status(400).json({ error: "offer_id required" });

  const supabase = getSupabase();

  try {
    // SKU summary from materialized view (latest metrics)
    const skuRes = await supabase
      .from("sku_metrics")
      .select("offer_id, product_id, sku, name, category_name, current_stock, sold_total, sold_30d, status, lifetime_velocity, buyers_count, age_days, last_cycle_start, last_restock_at, cost, manual_status, notes, first_stock_at")
      .eq("offer_id", offerId)
      .maybeSingle();
    if (skuRes.error) throw new Error("sku_metrics: " + skuRes.error.message);
    if (!skuRes.data) return res.status(404).json({ error: "SKU not found" });
    const sku = skuRes.data;

    // Daily sales — fetch all sales rows for product_id and aggregate per day in JS
    const salesRows = await fetchAll(
      () => supabase
        .from("sales")
        .select("ordered_at, quantity, order_status")
        .eq("product_id", sku.product_id)
        .order("ordered_at", { ascending: true })
    );

    const dailySales = new Map();
    for (const r of salesRows) {
      const status = r.order_status || "";
      if (EXCLUDED_STATUSES.has(status) || isMergerStatus(status)) continue;
      const day = r.ordered_at.slice(0, 10);
      const qty = parseFloat(r.quantity) || 0;
      dailySales.set(day, (dailySales.get(day) || 0) + qty);
    }
    const salesByDay = Array.from(dailySales.entries())
      .map(([date, qty]) => ({ date, qty }))
      .sort((a, b) => a.date.localeCompare(b.date));

    // Daily stock snapshots for this offer. total_quantity / reserved
    // з'явились з міграції 028 — для старих снепшотів NULL, тоді fallback на quantity (net).
    const snapsRows = await fetchAll(
      () => supabase
        .from("stock_snapshots")
        .select("snapshot_date, quantity, total_quantity, reserved")
        .eq("offer_id", offerId)
        .order("snapshot_date", { ascending: true })
    );
    const stockByDay = snapsRows.map((s) => ({
      date: s.snapshot_date,
      qty: parseFloat(s.quantity) || 0,
      total_qty: s.total_quantity != null ? parseFloat(s.total_quantity) : null,
      reserved: s.reserved != null ? parseFloat(s.reserved) : null,
    }));

    // Restock events. Пріоритет:
    //   1) Якщо last_restock_at в skus встановлено вручну через UI — це
    //      єдиний джерело правди. Графік показує саме цю дату.
    //   2) Якщо ні — детектимо переходи в total_quantity (не net), щоб
    //      ігнорувати "приріст" від звільнення резерву при відмові клієнта.
    //   3) Якщо total_quantity недоступне (старі снепшоти) — fallback на
    //      переходи 0→positive в quantity (net).
    let restockDates = [];
    if (sku.last_restock_at) {
      restockDates = [String(sku.last_restock_at).slice(0, 10)];
    } else {
      let prevTotal = null;
      let prevQty = null;
      for (const s of stockByDay) {
        if (s.total_qty != null) {
          // Реальний приход = total_quantity збільшилось значно (≥3, щоб
          // ігнорувати випадкові коливання типу 1 одиничного руху).
          if (prevTotal != null && s.total_qty - prevTotal >= 3) restockDates.push(s.date);
          else if (prevTotal === 0 && s.total_qty > 0) restockDates.push(s.date);
          prevTotal = s.total_qty;
        } else {
          // Fallback для історичних снепшотів: переход 0→positive в net.
          if (prevQty === 0 && s.qty > 0) restockDates.push(s.date);
        }
        prevQty = s.qty;
      }
    }

    return res.status(200).json({
      sku,
      sales_by_day: salesByDay,
      stock_by_day: stockByDay,
      restock_dates: restockDates,
      restock_source: sku.last_restock_at ? "manual_last_restock_at" : "auto_detected_from_snapshots",
    });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
