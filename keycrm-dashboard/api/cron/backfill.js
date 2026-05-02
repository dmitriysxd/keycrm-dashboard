const { getSupabase } = require("../../lib/supabase");
const { checkCronAuth } = require("../../lib/auth");
const { get, sleep } = require("../../lib/keycrm");

function lineQty(item) {
  const q = parseFloat(item.quantity);
  return isNaN(q) ? 0 : q;
}
function linePrice(item) {
  const p = parseFloat(item.price || item.unit_price || 0);
  return isNaN(p) ? 0 : p;
}
function pickBuyerId(order) {
  if (!order || typeof order !== "object") return null;
  const candidates = [
    order.buyer_id, order.buyerId,
    order.client_id, order.clientId,
    order.customer_id, order.customerId,
    order.buyer && order.buyer.id,
    order.client && order.client.id,
    order.customer && order.customer.id,
  ];
  for (const v of candidates) {
    if (v != null && v !== "" && !isNaN(parseInt(v))) return parseInt(v);
  }
  return null;
}

module.exports = async function handler(req, res) {
  const auth = checkCronAuth(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

  const apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

  const from = (req.query && req.query.from) || "";
  const to   = (req.query && req.query.to)   || "";
  if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
    return res.status(400).json({ error: "from/to обов'язкові у форматі YYYY-MM-DD" });
  }
  const fromTs = Date.parse(from + "T00:00:00Z");
  const toTs   = Date.parse(to   + "T23:59:59Z");
  if (toTs - fromTs > 35 * 24 * 60 * 60 * 1000) {
    return res.status(400).json({ error: "діапазон не може перевищувати 35 днів" });
  }

  const supabase = getSupabase();
  const ctx = { apiCalls: 0 };
  let runId = null;

  try {
    const ins = await supabase
      .from("ingest_runs")
      .insert({ kind: "backfill", status: "running", meta: { from, to } })
      .select("id")
      .single();
    if (ins.error) throw new Error("ingest_runs insert: " + ins.error.message);
    runId = ins.data.id;

    let page = 1;
    let total = 0;
    let upserted = 0;
    // Extend end boundary by +1 day so KeyCRM's possibly-exclusive end-date
    // interpretation doesn't drop events from the user-supplied 'to' day.
    // Adjacent backfill chunks can overlap safely — upsert by (order_id, line_idx)
    // makes the import idempotent.
    const toPlusOne = new Date(new Date(to + "T00:00:00Z").getTime() + 24 * 60 * 60 * 1000)
      .toISOString().slice(0, 10);
    const params = {
      include: "products.offer,status,buyer",
      limit: 50,
      sort: "id",
      "filter[created_between]": from + "," + toPlusOne,
    };

    while (page <= 200) {
      const resp = await get("/order", Object.assign({}, params, { page }), apiKey, ctx);
      const rows = resp.data || [];
      if (!rows.length) break;
      total += rows.length;

      const lines = [];
      for (const order of rows) {
        const status = (order.status && (order.status.name || order.status.title)) || null;
        const orderedAt = order.ordered_at || order.created_at;
        if (!orderedAt) continue;
        const buyerId = pickBuyerId(order);
        const items = order.products || [];
        items.forEach((item, idx) => {
          const qty = lineQty(item);
          if (qty <= 0) return;
          const price = linePrice(item);
          const offer = item.offer || {};
          lines.push({
            order_id: order.id,
            line_idx: idx,
            offer_id: offer.id || null,
            product_id: offer.product_id || null,
            name_snapshot: item.name || offer.name || null,
            quantity: qty,
            unit_price: price,
            revenue: price * qty,
            order_status: status,
            ordered_at: orderedAt,
            buyer_id: buyerId,
          });
        });
      }
      if (lines.length) {
        const { error } = await supabase
          .from("sales")
          .upsert(lines, { onConflict: "order_id,line_idx" });
        if (error) throw new Error("sales upsert: " + error.message);
        upserted += lines.length;
      }

      if (rows.length < 50) break;
      page++;
      await sleep(200);
    }

    await supabase
      .from("ingest_runs")
      .update({
        finished_at: new Date().toISOString(),
        status: "ok",
        orders_seen: total,
        sales_upserted: upserted,
        api_calls: ctx.apiCalls,
      })
      .eq("id", runId);

    return res.status(200).json({
      ok: true,
      run_id: runId,
      from,
      to,
      orders: total,
      sales_upserted: upserted,
      api_calls: ctx.apiCalls,
    });
  } catch (err) {
    const msg = (err && err.message) || String(err);
    if (runId) {
      await supabase
        .from("ingest_runs")
        .update({
          finished_at: new Date().toISOString(),
          status: "error",
          error_message: msg.substring(0, 500),
          api_calls: ctx.apiCalls,
        })
        .eq("id", runId);
    }
    return res.status(500).json({ error: msg });
  }
};
