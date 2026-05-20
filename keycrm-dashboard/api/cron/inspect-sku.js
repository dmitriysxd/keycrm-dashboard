// Діагностичний endpoint — показує все що знаємо про SKU за артикулом
// або offer_id, плюс стан ingest'а. Допомагає зрозуміти:
//   - чому товар має 2 рядки в таблиці (дубль варіантів?)
//   - чому новий товар не з'явився після оприбуткування
//   - коли востаннє бігав ingest і чи дайшов до кінця
//
// GET /api/cron/inspect-sku?secret=...&sku=03430
// GET /api/cron/inspect-sku?secret=...&offer_id=5271

const { getSupabase } = require("../../lib/supabase");
const { checkCronAuth } = require("../../lib/auth");
const { get } = require("../../lib/keycrm");

module.exports = async function handler(req, res) {
  const auth = checkCronAuth(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

  const supabase = getSupabase();
  const apiKey = process.env.KEYCRM_API_KEY;

  const sku = req.query && req.query.sku ? String(req.query.sku).trim() : null;
  const offerId = req.query && req.query.offer_id ? parseInt(req.query.offer_id) : null;
  if (!sku && !offerId) {
    return res.status(400).json({ error: "потрібен ?sku=... або ?offer_id=..." });
  }

  try {
    // 1. Записи в skus за вказаним фільтром.
    let skusQ = supabase.from("skus").select("*");
    if (sku) skusQ = skusQ.eq("sku", sku);
    if (offerId) skusQ = skusQ.eq("offer_id", offerId);
    const skusRes = await skusQ;
    if (skusRes.error) throw new Error("skus query: " + skusRes.error.message);
    const skusRows = skusRes.data || [];

    // 2. Записи в sku_metrics (matview).
    let metricsQ = supabase.from("sku_metrics").select("*");
    if (sku) metricsQ = metricsQ.eq("sku", sku);
    if (offerId) metricsQ = metricsQ.eq("offer_id", offerId);
    const metricsRes = await metricsQ;
    const metricsRows = metricsRes.error ? [] : (metricsRes.data || []);

    // 3. Останні snapshots для всіх знайдених offer_id.
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

    // 4. Останні sales (по product_id з знайдених skus).
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

    // 5. Стан ingest_runs за останні 3 дні.
    const cutoff = new Date(Date.now() - 3 * 24 * 3600 * 1000).toISOString();
    const { data: runs } = await supabase
      .from("ingest_runs")
      .select("id, kind, status, started_at, finished_at, products_seen, orders_seen, error_message, meta")
      .gte("started_at", cutoff)
      .order("started_at", { ascending: false })
      .limit(20);

    // 6. Стан ingest_state (одиничний рядок).
    const { data: stateRow } = await supabase
      .from("ingest_state")
      .select("*")
      .eq("id", 1)
      .maybeSingle();

    // 7. Якщо передано sku — спробуємо також підтягнути актуальний стан з KeyCRM
    //    (тільки якщо apiKey є). Це покаже скільки реально товарів з таким sku
    //    у CRM зараз, без огляду на нашу БД.
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

    // Вивід.
    return res.status(200).json({
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
    });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
