// Об'єднаний діагностичний endpoint.
//
// GET /api/cron/inspect?secret=...&target=keycrm
//   → стан KeyCRM API (custom-fields definitions, приклад order/buyer)
//
// GET /api/cron/inspect?secret=...&target=sku&sku=03430
// GET /api/cron/inspect?secret=...&target=sku&offer_id=5271
//   → все що знаємо про SKU + стан ingest
//
// Об'єднано в один файл щоб укластися в 12-функційний ліміт Hobby Vercel.

const { getSupabase } = require("../../lib/supabase");
const { checkCronAuth } = require("../../lib/auth");
const { get } = require("../../lib/keycrm");

// ────────────────────────────────────────────────────────────────────
// target=keycrm: інспекція KeyCRM API
// ────────────────────────────────────────────────────────────────────
async function tryFetchDefinitions(apiKey, ctx) {
  const candidates = [
    "/custom-fields",
    "/custom-fields/buyer",
    "/buyer/custom-fields",
    "/custom-fields?entity_type=buyer",
  ];
  const tried = [];
  for (const path of candidates) {
    try {
      const resp = await get(path, {}, apiKey, ctx);
      tried.push({ path, ok: true, sample: resp });
      return tried;
    } catch (e) {
      tried.push({ path, ok: false, error: (e && e.message) || String(e) });
    }
  }
  return tried;
}

async function inspectKeycrm(apiKey, supabase, ctx) {
  const definitionsAttempts = await tryFetchDefinitions(apiKey, ctx);

  const sampleBuyerResp = await get("/buyer", { limit: 1, include: "custom_fields" }, apiKey, ctx);
  const sampleBuyer = sampleBuyerResp && sampleBuyerResp.data && sampleBuyerResp.data[0];

  const sampleOrderResp = await get("/order", { limit: 1, include: "products.offer,status,buyer" }, apiKey, ctx);
  const sampleOrder = sampleOrderResp && sampleOrderResp.data && sampleOrderResp.data[0];

  const { data: customRows, error } = await supabase
    .from("buyers")
    .select("buyer_id, full_name, custom_fields_raw")
    .not("custom_fields_raw", "is", null)
    .limit(200);
  if (error) throw new Error("buyers select: " + error.message);

  const uuidValueMap = {};
  for (const row of customRows || []) {
    const cf = row.custom_fields_raw;
    if (!cf) continue;
    const list = Array.isArray(cf) ? cf : Object.entries(cf).map(([k, v]) => ({ uuid: k, value: v }));
    for (const item of list) {
      const uuid = item.uuid || item.id || item.key || item.name;
      if (!uuid) continue;
      const value = item.value;
      if (!uuidValueMap[uuid]) uuidValueMap[uuid] = { values: new Set(), samples: [] };
      if (value != null) uuidValueMap[uuid].values.add(String(value));
      if (uuidValueMap[uuid].samples.length < 3) {
        uuidValueMap[uuid].samples.push({ buyer_id: row.buyer_id, full_name: row.full_name, value });
      }
    }
  }
  const customFieldsBreakdown = Object.entries(uuidValueMap).map(([uuid, info]) => ({
    uuid,
    unique_values: Array.from(info.values),
    sample_buyers: info.samples,
  }));

  return {
    target: "keycrm",
    ok: true,
    custom_field_definitions_attempts: definitionsAttempts,
    custom_fields_breakdown: customFieldsBreakdown,
    sample_buyer: sampleBuyer,
    sample_order: sampleOrder,
    sample_order_top_level_keys: sampleOrder ? Object.keys(sampleOrder) : null,
    api_calls: ctx.apiCalls,
  };
}

// ────────────────────────────────────────────────────────────────────
// target=sku: інспекція конкретного товару
// ────────────────────────────────────────────────────────────────────
async function inspectSku(apiKey, supabase, sku, offerId) {
  let skusQ = supabase.from("skus").select("*");
  if (sku) skusQ = skusQ.eq("sku", sku);
  if (offerId) skusQ = skusQ.eq("offer_id", offerId);
  const skusRes = await skusQ;
  if (skusRes.error) throw new Error("skus query: " + skusRes.error.message);
  const skusRows = skusRes.data || [];

  let metricsQ = supabase.from("sku_metrics").select("*");
  if (sku) metricsQ = metricsQ.eq("sku", sku);
  if (offerId) metricsQ = metricsQ.eq("offer_id", offerId);
  const metricsRes = await metricsQ;
  const metricsRows = metricsRes.error ? [] : (metricsRes.data || []);

  const offerIds = skusRows.map(r => r.offer_id).filter(Boolean);
  let snapshotsByOffer = {};
  if (offerIds.length) {
    const { data: snaps } = await supabase
      .from("stock_snapshots")
      .select("offer_id, snapshot_date, quantity, price")
      .in("offer_id", offerIds)
      .order("snapshot_date", { ascending: false })
      .limit(50);
    for (const s of snaps || []) {
      if (!snapshotsByOffer[s.offer_id]) snapshotsByOffer[s.offer_id] = [];
      if (snapshotsByOffer[s.offer_id].length < 10) snapshotsByOffer[s.offer_id].push(s);
    }
  }

  const productIds = skusRows.map(r => r.product_id).filter(Boolean);
  let salesRows = [];
  if (productIds.length) {
    const { data: sl } = await supabase
      .from("sales")
      .select("order_id, ordered_at, order_status, offer_id, product_id, quantity, unit_price, revenue, buyer_id")
      .in("product_id", productIds)
      .order("ordered_at", { ascending: false })
      .limit(20);
    salesRows = sl || [];
  }

  const cutoff = new Date(Date.now() - 3 * 24 * 3600 * 1000).toISOString();
  const { data: runs } = await supabase
    .from("ingest_runs")
    .select("id, kind, status, started_at, finished_at, products_seen, orders_seen, error_message, meta")
    .gte("started_at", cutoff)
    .order("started_at", { ascending: false })
    .limit(20);

  const { data: stateRow } = await supabase
    .from("ingest_state")
    .select("*")
    .eq("id", 1)
    .maybeSingle();

  let keycrmProducts = null;
  let keycrmOffers = null;
  if (sku && apiKey) {
    try {
      const pResp = await get("/products", { "filter[sku]": sku, limit: 50 }, apiKey);
      keycrmProducts = (pResp && pResp.data) || [];
    } catch (e) {
      keycrmProducts = { error: e.message };
    }
    try {
      const oResp = await get("/offers", { "filter[sku]": sku, limit: 50 }, apiKey);
      keycrmOffers = (oResp && oResp.data) || [];
    } catch (e) {
      keycrmOffers = { error: e.message };
    }
  }

  return {
    target: "sku",
    query: { sku, offer_id: offerId },
    summary: {
      skus_rows: skusRows.length,
      metrics_rows: metricsRows.length,
      keycrm_products_with_sku: Array.isArray(keycrmProducts) ? keycrmProducts.length : null,
      keycrm_offers_with_sku: Array.isArray(keycrmOffers) ? keycrmOffers.length : null,
    },
    skus_rows: skusRows.map(r => ({
      offer_id: r.offer_id,
      product_id: r.product_id,
      sku: r.sku,
      name: r.name,
      category_name: r.category_name,
      price: r.price,
      is_active: r.is_active,
      first_stock_at: r.first_stock_at,
      last_restock_at: r.last_restock_at,
      last_seen_at: r.last_seen_at,
      keycrm_created_at: r.keycrm_created_at,
      is_variant: r.offer_id !== r.product_id,
    })),
    metrics_rows: metricsRows.map(r => ({
      offer_id: r.offer_id,
      product_id: r.product_id,
      sku: r.sku,
      name: r.name,
      current_stock: r.current_stock,
      sold_30d: r.sold_30d,
      sold_total: r.sold_total,
      age_days: r.age_days,
      status: r.status,
      is_active: r.is_active,
    })),
    latest_snapshots_by_offer: snapshotsByOffer,
    recent_sales: salesRows,
    ingest_runs_last_3d: runs || [],
    ingest_state: stateRow,
    keycrm_live: {
      products: keycrmProducts,
      offers: keycrmOffers,
      hint: "Якщо тут пусто, але в нашій БД є записи — товар у KeyCRM під іншим артикулом або видалений.",
    },
    hints: [
      "Якщо skus_rows > 1 — у БД дублі. Дивись поле is_variant: false=майстер-продукт, true=варіант. Зазвичай майстер з варіантами треба ховати.",
      "Якщо ingest_state.status = 'running' давно — щось зависло. Якщо 'done' з сьогоднішнім cycle_date — ОК.",
      "Якщо в metrics_rows немає рядка хоча в skus є — sku_metrics matview не освіжений. Викликай REFRESH MATERIALIZED VIEW sku_metrics; вручну.",
    ],
  };
}

// ────────────────────────────────────────────────────────────────────
// target=health: загальний health-check автоматичного оновлення
// ────────────────────────────────────────────────────────────────────
async function inspectHealth(supabase) {
  const now = new Date();
  const todayISO = now.toISOString().slice(0, 10);

  // 1. Останні ingest_runs (всі типи).
  const cutoffWeek = new Date(Date.now() - 7 * 24 * 3600 * 1000).toISOString();
  const { data: runs } = await supabase
    .from("ingest_runs")
    .select("id, kind, status, started_at, finished_at, products_seen, orders_seen, sales_upserted, error_message, meta")
    .gte("started_at", cutoffWeek)
    .order("started_at", { ascending: false })
    .limit(50);

  // ОКРЕМИЙ запит за останнім auto-run — щоб не загубитись в .limit(50)
  // у випадку, коли користувач робив багато ручних backfill'ів і вони
  // витіснили auto-record з вибірки.
  const { data: lastAutoArr } = await supabase
    .from("ingest_runs")
    .select("id, kind, status, started_at, finished_at, products_seen, orders_seen, sales_upserted, error_message, meta")
    .eq("kind", "auto")
    .order("started_at", { ascending: false })
    .limit(1);
  const lastAuto = (lastAutoArr && lastAutoArr[0]) || null;

  // Усі auto-runs за тиждень — порахуємо count, щоб знати, чи стабільно
  // cron виконується.
  const { count: autoRunsWeekCount } = await supabase
    .from("ingest_runs")
    .select("id", { count: "exact", head: true })
    .eq("kind", "auto")
    .gte("started_at", cutoffWeek);

  // 2. ingest_state.
  const { data: state } = await supabase
    .from("ingest_state").select("*").eq("id", 1).maybeSingle();

  // 3. Свіжість snapshots — по днях. Кожен день — окремий count, бо
  //    Supabase JS без явного .limit() обмежує вибірку 1000 рядків,
  //    а реально снепшотів за день стільки, скільки SKU (3000-4000).
  const days = [];
  for (let i = 0; i < 7; i++) {
    const d = new Date(Date.now() - i * 86400000).toISOString().slice(0, 10);
    const { count } = await supabase
      .from("stock_snapshots")
      .select("offer_id", { count: "exact", head: true })
      .eq("snapshot_date", d);
    days.push({ date: d, snapshots_count: count || 0 });
  }
  const missingSnapshotDays = days.filter(d => d.snapshots_count === 0).map(d => d.date);

  // 4. Скільки нових товарів за останні 7 днів і скільки з оприбуткуванням.
  const { data: newSkus, count: newSkusCount } = await supabase
    .from("skus")
    .select("offer_id, sku, name, keycrm_created_at, first_stock_at, last_restock_at", { count: "exact" })
    .gte("keycrm_created_at", cutoffWeek)
    .limit(20);

  // 5. Скільки рядків у sku_metrics і коли остання активність.
  const { count: metricsTotal } = await supabase
    .from("sku_metrics")
    .select("offer_id", { count: "exact", head: true });

  // 6. Buyer_rfm свіжість — коли остання last_synced_at в buyers.
  const { data: lastBuyer } = await supabase
    .from("buyers")
    .select("buyer_id, last_synced_at")
    .order("last_synced_at", { ascending: false })
    .limit(1);

  // 7. pg_cron статус — чи активні нічні refresh-задачі і коли остання
  //    спроба. Якщо pg_cron не доступний (extension не включена), функція
  //    повертає порожній масив (без помилки).
  const { data: pgCronJobs } = await supabase.rpc("pg_cron_status");

  // 7b. Wholesale-статус клієнтів — скільки опт, не-опт, невизначено
  const { data: wholesaleStatus } = await supabase.rpc("buyers_wholesale_status_breakdown");

  // 8. Свіжість sku_metrics — порівнюємо MAX(last_sold_at) в matview з
  //    MAX(ordered_at) в сирому sales. Якщо matview значно відстає,
  //    значить refresh не відпрацював.
  const { data: latestSale } = await supabase
    .from("sales").select("ordered_at").order("ordered_at", { ascending: false }).limit(1);
  const { data: latestMetricSold } = await supabase
    .from("sku_metrics").select("last_sold_at").not("last_sold_at", "is", null)
    .order("last_sold_at", { ascending: false }).limit(1);
  const lagSales = latestSale && latestSale[0] ? new Date(latestSale[0].ordered_at).getTime() : null;
  const lagMatview = latestMetricSold && latestMetricSold[0] ? new Date(latestMetricSold[0].last_sold_at).getTime() : null;
  const matviewLagHours = (lagSales && lagMatview)
    ? Math.round((lagSales - lagMatview) / 3600000)
    : null;

  // Аналіз і висновки.
  const hints = [];
  if (!lastAuto) {
    hints.push("⚠ Жодного 'auto' інгесту не знайдено в БД взагалі. Перевір Vercel cron schedule і CRON_SECRET.");
  } else {
    const lastAutoAge = (Date.now() - new Date(lastAuto.started_at).getTime()) / 3600000;
    if (lastAutoAge > 30) hints.push(`⚠ Останній auto-ingest був ${Math.round(lastAutoAge)} год тому — мав би пройти сьогодні в 03:00 UTC.`);
    if (lastAuto.status !== "ok") hints.push(`⚠ Останній auto-ingest завершився зі статусом '${lastAuto.status}': ${lastAuto.error_message || '—'}`);
  }
  if (autoRunsWeekCount != null && autoRunsWeekCount < 5) {
    hints.push(`⚠ За останній тиждень лише ${autoRunsWeekCount} auto-runs (очікувано ~7, по одному на день). Cron може пропускати дні.`);
  }
  if (matviewLagHours != null && matviewLagHours > 30) {
    hints.push(`⚠ sku_metrics відстає від sales на ${matviewLagHours} год. Refresh не відпрацював. Запусти REFRESH MATERIALIZED VIEW sku_metrics; вручну.`);
  }
  // Пomічаємо клієнтів з невизначеним wholesale-статусом
  if (wholesaleStatus && wholesaleStatus[0]) {
    const ws = wholesaleStatus[0];
    if (ws.pending_backfill > 0) {
      hints.push(`ℹ ${ws.pending_backfill} нових клієнтів чекають на бекфіл /buyer/{id} (нема повної картки). GitHub Actions обробляє автоматично щодня.`);
    }
    if (ws.wholesale_unknown > 0) {
      hints.push(`ℹ ${ws.wholesale_unknown} клієнтів з is_wholesale=NULL — нові, ще не отримали повну картку з KeyCRM.`);
    }
  }
  // pg_cron jobs зараз пусті ЗА ЗАДУМОМ — міграція 036 їх прибрала, бо вони
  // спрацьовували до того як ingest заллє свіжі дані (race condition).
  // Тепер matview освіжається прямо з ingest.js на кроці 'metrics'. Тому
  // ОЧІКУВАНО що pgCronJobs порожній — не варто на цьому ворнити.
  if (pgCronJobs && pgCronJobs.length > 0) {
    for (const job of pgCronJobs) {
      if (!job.active) hints.push(`⚠ pg_cron job '${job.jobname}' inactive — refresh не запускається.`);
      else if (job.last_status && job.last_status !== "succeeded") {
        hints.push(`⚠ pg_cron job '${job.jobname}' last run: ${job.last_status}`);
      }
    }
  }
  if (state && state.status === "running") {
    const stateAge = state.last_chunk_at ? (Date.now() - new Date(state.last_chunk_at).getTime()) / 3600000 : null;
    if (stateAge && stateAge > 2) hints.push(`⚠ ingest_state застряг у 'running' вже ${Math.round(stateAge)} год — щось зависло на step '${state.current_step}'.`);
  }
  if (missingSnapshotDays.length > 1) {
    hints.push(`⚠ Пропущені дні snapshots: ${missingSnapshotDays.join(", ")} — за ці дні немає даних про залишки.`);
  }
  if (hints.length === 0) hints.push("✓ Все працює нормально.");

  return {
    target: "health",
    today_utc: todayISO,
    last_auto_ingest: lastAuto,
    auto_runs_last_7d_count: autoRunsWeekCount,
    ingest_state: state,
    snapshots_per_day_last_7: days,
    missing_snapshot_days: missingSnapshotDays,
    new_skus_last_7d: {
      count: newSkusCount || 0,
      sample: (newSkus || []).slice(0, 10),
      not_yet_restocked: (newSkus || []).filter(s => !s.last_restock_at).length,
    },
    sku_metrics_row_count: metricsTotal || 0,
    sku_metrics_lag_from_sales_hours: matviewLagHours,
    last_sale_in_sales: latestSale && latestSale[0] ? latestSale[0].ordered_at : null,
    last_sale_in_sku_metrics: latestMetricSold && latestMetricSold[0] ? latestMetricSold[0].last_sold_at : null,
    pg_cron_jobs: pgCronJobs || [],
    wholesale_status: (wholesaleStatus && wholesaleStatus[0]) || null,
    last_buyer_sync: lastBuyer && lastBuyer[0] ? lastBuyer[0].last_synced_at : null,
    recent_runs_summary: (runs || []).slice(0, 10).map(r => ({
      kind: r.kind, status: r.status, started_at: r.started_at,
      products_seen: r.products_seen, orders_seen: r.orders_seen, error: r.error_message,
    })),
    hints,
  };
}

// ────────────────────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  const auth = checkCronAuth(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

  const apiKey = process.env.KEYCRM_API_KEY;
  const supabase = getSupabase();
  const ctx = { apiCalls: 0 };

  const target = (req.query && req.query.target) || "keycrm";

  try {
    if (target === "health") {
      const result = await inspectHealth(supabase);
      return res.status(200).json(result);
    }

    if (target === "sku") {
      const sku = req.query && req.query.sku ? String(req.query.sku).trim() : null;
      const offerId = req.query && req.query.offer_id ? parseInt(req.query.offer_id) : null;
      if (!sku && !offerId) {
        return res.status(400).json({ error: "потрібен ?sku=... або ?offer_id=..." });
      }
      const result = await inspectSku(apiKey, supabase, sku, offerId);
      return res.status(200).json(result);
    }

    if (target === "buyer") {
      // Інспекція конкретного покупця: наш запис в buyers + сирий KeyCRM /buyer/{id}
      // з ВСІМА полями (щоб бачити які реально віддає API).
      const buyerId = req.query && req.query.buyer_id ? parseInt(req.query.buyer_id) : null;
      if (!buyerId) return res.status(400).json({ error: "потрібен ?buyer_id=..." });
      if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

      const { data: ourRow } = await supabase
        .from("buyers")
        .select("*")
        .eq("buyer_id", buyerId)
        .maybeSingle();

      let raw = null;
      let rawError = null;
      try {
        const resp = await get("/buyer/" + buyerId, { include: "custom_fields" }, apiKey, ctx);
        raw = (resp && resp.data) || resp || null;
      } catch (e) {
        rawError = (e && e.message) || String(e);
      }

      // Знайдемо всі ключі першого рівня, щоб юзер бачив що приходить
      const topLevelKeys = raw && typeof raw === "object" ? Object.keys(raw).sort() : [];

      return res.status(200).json({
        target: "buyer",
        buyer_id: buyerId,
        our_db_record: ourRow,
        keycrm_raw_response: raw,
        keycrm_raw_error: rawError,
        keycrm_top_level_keys: topLevelKeys,
        hint: "Шукай поля з 'опт'/'wholesale' у будь-якому ключі. Якщо custom_fields порожній, переглянь tags, type, segment, comment, або topLevelKeys.",
      });
    }

    if (target === "order") {
      // Інспекція конкретного замовлення KeyCRM + список джерел в системі.
      // Корисно для дебага source-полів (source_id, source_name, source object).
      const orderId = req.query && req.query.order_id ? parseInt(req.query.order_id) : null;
      if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

      let raw = null;
      let rawError = null;
      let sourcesList = null;
      let sourcesError = null;

      if (orderId) {
        try {
          const resp = await get("/order/" + orderId, { include: "products.offer,status,buyer,manager" }, apiKey, ctx);
          raw = (resp && resp.data) || resp || null;
        } catch (e) {
          rawError = (e && e.message) || String(e);
        }
      }

      // Спробуємо різні ендпойнти списку джерел (KeyCRM має один з них).
      for (const path of ["/order/source", "/sources", "/order/sources", "/source"]) {
        try {
          const r = await get(path, { limit: 100 }, apiKey, ctx);
          if (r && (r.data || Array.isArray(r))) {
            sourcesList = { endpoint: path, items: r.data || r };
            break;
          }
        } catch (e) {
          sourcesError = (sourcesError ? sourcesError + " | " : "") + path + ": " + ((e && e.message) || String(e)).slice(0, 80);
        }
      }

      const topLevelKeys = raw && typeof raw === "object" ? Object.keys(raw).sort() : [];

      return res.status(200).json({
        target: "order",
        order_id: orderId,
        keycrm_order_raw: raw,
        keycrm_order_error: rawError,
        keycrm_top_level_keys: topLevelKeys,
        keycrm_sources_list: sourcesList,
        keycrm_sources_error_tried: sourcesError,
        hint: "Шукай поле з ID джерела (source_id / source.id). Якщо назви немає на замовленні — sources_list має містити мапу id→name.",
      });
    }

    // default: keycrm
    if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });
    const result = await inspectKeycrm(apiKey, supabase, ctx);
    return res.status(200).json(result);
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
