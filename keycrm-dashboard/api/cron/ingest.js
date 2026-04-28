const { getSupabase } = require("../../lib/supabase");
const { checkCronAuth } = require("../../lib/auth");
const { get, sleep } = require("../../lib/keycrm");

function todayDate() {
  return new Date().toISOString().slice(0, 10);
}

function lineQty(item) {
  const q = parseFloat(item.quantity);
  return isNaN(q) ? 0 : q;
}

function linePrice(item) {
  const p = parseFloat(item.price || item.unit_price || 0);
  return isNaN(p) ? 0 : p;
}

async function ingestProducts(apiKey, supabase, ctx) {
  let page = 1;
  let total = 0;
  while (page <= 100) {
    const resp = await get("/products", { page, limit: 50 }, apiKey, ctx);
    const rows = resp.data || [];
    if (!rows.length) break;
    total += rows.length;
    const upserts = rows.map((p) => ({
      offer_id: p.id,
      product_id: p.id,
      sku: p.sku || null,
      name: p.name || ("Product " + p.id),
      category_id: p.category_id || (p.category && p.category.id) || null,
      category_name: (p.category && p.category.name) || null,
      price: parseFloat(p.price) || null,
      last_seen_at: new Date().toISOString(),
      is_active: true,
    }));
    const { error } = await supabase
      .from("skus")
      .upsert(upserts, { onConflict: "offer_id", ignoreDuplicates: false });
    if (error) throw new Error("skus upsert: " + error.message);
    if (rows.length < 50) break;
    page++;
    await sleep(200);
  }
  return total;
}

async function ingestOffers(apiKey, supabase, ctx) {
  const today = todayDate();
  let page = 1;
  let total = 0;

  while (page <= 100) {
    const resp = await get("/offers", { page, limit: 50 }, apiKey, ctx);
    const rows = resp.data || [];
    if (!rows.length) break;
    total += rows.length;

    const skuRows = [];
    const snapRows = [];
    for (const o of rows) {
      const offerId = o.id;
      const productId = o.product_id || o.product || offerId;
      const qty = parseFloat(o.quantity);
      const price = parseFloat(o.price);

      skuRows.push({
        offer_id: offerId,
        product_id: productId,
        sku: o.sku || null,
        name: o.product_name || o.name || ("Offer " + offerId),
        price: isNaN(price) ? null : price,
        last_seen_at: new Date().toISOString(),
        is_active: true,
      });
      snapRows.push({
        snapshot_date: today,
        offer_id: offerId,
        quantity: isNaN(qty) ? 0 : qty,
        price: isNaN(price) ? null : price,
      });
    }

    if (skuRows.length) {
      const { error: e1 } = await supabase
        .from("skus")
        .upsert(skuRows, { onConflict: "offer_id", ignoreDuplicates: false });
      if (e1) throw new Error("skus upsert (offers): " + e1.message);
    }
    if (snapRows.length) {
      const { error: e2 } = await supabase
        .from("stock_snapshots")
        .upsert(snapRows, { onConflict: "snapshot_date,offer_id" });
      if (e2) throw new Error("stock_snapshots upsert: " + e2.message);

      const positives = snapRows.filter((r) => r.quantity > 0).map((r) => r.offer_id);
      if (positives.length) {
        await supabase
          .from("skus")
          .update({ first_stock_at: new Date().toISOString() })
          .in("offer_id", positives)
          .is("first_stock_at", null);
      }
    }

    if (rows.length < 50) break;
    page++;
    await sleep(200);
  }

  return { offersSeen: total };
}

async function ingestSales(apiKey, supabase, ctx, sinceISO) {
  let page = 1;
  let total = 0;
  let upserted = 0;
  const params = {
    include: "products,status",
    limit: 50,
  };
  if (sinceISO) params["filter[updated_at_min]"] = sinceISO;

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
      const items = order.products || [];
      items.forEach((item, idx) => {
        const qty = lineQty(item);
        if (qty <= 0) return;
        const price = linePrice(item);
        lines.push({
          order_id: order.id,
          line_idx: idx,
          offer_id: item.offer_id || (item.offer && item.offer.id) || null,
          product_id: item.product_id || (item.product && item.product.id) || null,
          name_snapshot: item.name || (item.offer && item.offer.name) || null,
          quantity: qty,
          unit_price: price,
          revenue: price * qty,
          order_status: status,
          ordered_at: orderedAt,
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

  return { ordersSeen: total, salesUpserted: upserted };
}

async function deactivateMissing(supabase, runStartIso) {
  await supabase
    .from("skus")
    .update({ is_active: false })
    .lt("last_seen_at", runStartIso)
    .eq("is_active", true);
}

module.exports = async function handler(req, res) {
  const auth = checkCronAuth(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

  const apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

  const supabase = getSupabase();
  const ctx = { apiCalls: 0 };
  const runStartIso = new Date().toISOString();
  let runId = null;

  try {
    const ins = await supabase
      .from("ingest_runs")
      .insert({ kind: "daily", status: "running" })
      .select("id")
      .single();
    if (ins.error) throw new Error("ingest_runs insert: " + ins.error.message);
    runId = ins.data.id;

    const productsSeen = await ingestProducts(apiKey, supabase, ctx);
    const { offersSeen } = await ingestOffers(apiKey, supabase, ctx);

    await deactivateMissing(supabase, runStartIso);

    const since = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString();
    const { ordersSeen, salesUpserted } = await ingestSales(apiKey, supabase, ctx, since);

    const refresh = await supabase.rpc("refresh_sku_metrics");
    if (refresh.error) throw new Error("refresh_sku_metrics: " + refresh.error.message);

    await supabase
      .from("ingest_runs")
      .update({
        finished_at: new Date().toISOString(),
        status: "ok",
        products_seen: productsSeen,
        offers_seen: offersSeen,
        orders_seen: ordersSeen,
        sales_upserted: salesUpserted,
        api_calls: ctx.apiCalls,
      })
      .eq("id", runId);

    return res.status(200).json({
      ok: true,
      run_id: runId,
      products: productsSeen,
      offers: offersSeen,
      orders: ordersSeen,
      sales_upserted: salesUpserted,
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
    if (process.env.ALERT_WEBHOOK_URL) {
      try {
        await fetch(process.env.ALERT_WEBHOOK_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ text: "KEYCRM ingest failed: " + msg }),
        });
      } catch (_) {}
    }
    return res.status(500).json({ error: msg });
  }
};
