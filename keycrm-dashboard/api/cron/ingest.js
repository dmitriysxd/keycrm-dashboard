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

async function fetchCategories(apiKey, ctx) {
  const map = {};
  let page = 1;
  while (page <= 20) {
    const resp = await get("/products/categories", { page, limit: 50 }, apiKey, ctx);
    const rows = resp.data || [];
    rows.forEach((c) => { if (c && c.id) map[String(c.id)] = c.name || null; });
    if (rows.length < 50) break;
    page++;
  }
  return map;
}

function pickCreatedAt(obj) {
  if (!obj || typeof obj !== "object") return null;
  const candidates = [
    "created_at", "createdAt", "date_created", "dateCreated",
    "created", "inserted_at", "insertedAt", "date_added", "dateAdded",
  ];
  for (const k of candidates) {
    const v = obj[k];
    if (v == null || v === "") continue;
    const d = new Date(v);
    if (!isNaN(d.getTime())) return d.toISOString();
  }
  return null;
}

async function ingestProducts(apiKey, supabase, ctx, fromPage, take) {
  const today = todayDate();
  const startPage = Math.max(1, parseInt(fromPage || 1));
  const limitPages = take ? parseInt(take) : 200;
  const categories = await fetchCategories(apiKey, ctx);
  let total = 0;
  let lastPageProcessed = startPage - 1;
  let sampleKeys = null;
  let createdAtHits = 0;

  // Fetch pages in parallel batches. KeyCRM API tolerates 4 concurrent
  // requests well within its 60 req/min limit. This is the main speed-up:
  // sequential fetch was ~500ms × 8 pages = 4s per chunk; parallel is
  // ~600ms for the whole batch (slowest of 4).
  const PARALLEL_FETCH = 4;
  let i = 0;
  while (i < limitPages) {
    const batchSize = Math.min(PARALLEL_FETCH, limitPages - i);
    const batchPages = [];
    for (let j = 0; j < batchSize; j++) batchPages.push(startPage + i + j);

    const responses = await Promise.all(
      batchPages.map((p) => get("/products", { page: p, limit: 50 }, apiKey, ctx))
    );

    let endOfData = false;
    for (let bIdx = 0; bIdx < responses.length; bIdx++) {
      const page = batchPages[bIdx];
      const rows = responses[bIdx].data || [];
      if (!rows.length) { endOfData = true; break; }
      if (!sampleKeys && rows[0]) sampleKeys = Object.keys(rows[0]);
      total += rows.length;
      lastPageProcessed = page;

      const skuRows = [];
      const snapRows = [];
      const positives = [];
      for (const p of rows) {
        const qty = parseFloat(p.quantity != null ? p.quantity : p.in_stock);
        const reserved = parseFloat(p.in_reserve != null ? p.in_reserve : 0);
        const totalQty = isNaN(qty) ? 0 : qty;
        const reservedQty = isNaN(reserved) ? 0 : reserved;
        const safeQty = Math.max(0, totalQty - reservedQty);
        const price = parseFloat(p.price);
        const safePrice = isNaN(price) ? null : price;
        const catId = p.category_id || (p.category && p.category.id) || null;
        const catName = (p.category && p.category.name) || (catId ? categories[String(catId)] : null) || null;
        const keycrmCreatedAt = pickCreatedAt(p);
        if (keycrmCreatedAt) createdAtHits++;
        skuRows.push({
          offer_id: p.id,
          product_id: p.id,
          sku: p.sku || null,
          name: p.name || ("Product " + p.id),
          category_id: catId,
          category_name: catName,
          price: safePrice,
          // Фото товару для AI-аналізу дизайну.
          // KeyCRM віддає thumbnail_url або attachments_data.
          image_url: p.thumbnail_url
            || (Array.isArray(p.attachments_data) && p.attachments_data[0]
                && (typeof p.attachments_data[0] === "string"
                    ? p.attachments_data[0]
                    : (p.attachments_data[0].url || p.attachments_data[0].path)))
            || null,
          keycrm_created_at: keycrmCreatedAt,
          last_seen_at: new Date().toISOString(),
          is_active: true,
        });
        snapRows.push({
          snapshot_date: today,
          offer_id: p.id,
          quantity: safeQty,
          total_quantity: totalQty,
          reserved: reservedQty,
          price: safePrice,
        });
        if (safeQty > 0) positives.push(p.id);
      }
      const e1 = await supabase
        .from("skus")
        .upsert(skuRows, { onConflict: "offer_id", ignoreDuplicates: false });
      if (e1.error) throw new Error("skus upsert: " + e1.error.message);
      const e2 = await supabase
        .from("stock_snapshots")
        .upsert(snapRows, { onConflict: "snapshot_date,offer_id" });
      if (e2.error) throw new Error("stock_snapshots upsert: " + e2.error.message);
      if (positives.length) {
        await supabase
          .from("skus")
          .update({ first_stock_at: new Date().toISOString() })
          .in("offer_id", positives)
          .is("first_stock_at", null);
      }
      if (rows.length < 50) { endOfData = true; break; }
    }

    if (endOfData) {
      return { total, lastPage: lastPageProcessed, more: false, sampleKeys, createdAtHits };
    }
    i += batchSize;
  }
  return { total, lastPage: lastPageProcessed, more: true, sampleKeys, createdAtHits };
}

async function ingestOffers(apiKey, supabase, ctx, fromPage, take) {
  const today = todayDate();
  const startPage = Math.max(1, parseInt(fromPage || 1));
  const limitPages = take ? parseInt(take) : 200;
  let total = 0;
  let lastPageProcessed = startPage - 1;

  for (let i = 0; i < limitPages; i++) {
    const page = startPage + i;
    const resp = await get("/offers", { page, limit: 50, include: "product" }, apiKey, ctx);
    const rows = resp.data || [];
    if (!rows.length) return { offersSeen: total, lastPage: lastPageProcessed, more: false };
    total += rows.length;
    lastPageProcessed = page;

    const skuRows = [];
    const snapRows = [];
    for (const o of rows) {
      const offerId = o.id;
      const product = o.product || {};
      const productId = o.product_id || product.id || offerId;
      const qtyTotal = parseFloat(o.quantity);
      const qtyReserved = parseFloat(o.in_reserve != null ? o.in_reserve : 0);
      const qty = Math.max(0, (isNaN(qtyTotal) ? 0 : qtyTotal) - (isNaN(qtyReserved) ? 0 : qtyReserved));
      const price = parseFloat(o.price);
      const productName = product.name || o.product_name || o.name;
      const variantSuffix = o.sku && productName && !productName.includes(o.sku)
        ? " · " + o.sku
        : "";
      const displayName = productName
        ? productName + variantSuffix
        : ("Offer " + offerId);
      const categoryId = product.category_id
        || (product.category && product.category.id)
        || null;
      const categoryName = (product.category && product.category.name) || null;

      const keycrmCreatedAt = pickCreatedAt(product) || pickCreatedAt(o);
      skuRows.push({
        offer_id: offerId,
        product_id: productId,
        sku: o.sku || null,
        name: displayName,
        category_id: categoryId,
        category_name: categoryName,
        price: isNaN(price) ? null : price,
        keycrm_created_at: keycrmCreatedAt,
        last_seen_at: new Date().toISOString(),
        is_active: true,
      });
      snapRows.push({
        snapshot_date: today,
        offer_id: offerId,
        quantity: isNaN(qty) ? 0 : qty,
        total_quantity: isNaN(qtyTotal) ? 0 : qtyTotal,
        reserved: isNaN(qtyReserved) ? 0 : qtyReserved,
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

    if (rows.length < 50) return { offersSeen: total, lastPage: page, more: false };
    await sleep(20);
  }

  return { offersSeen: total, lastPage: lastPageProcessed, more: true };
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

function pickBuyerObject(order) {
  if (!order || typeof order !== "object") return null;
  return order.buyer || order.client || order.customer || null;
}

function pickPhone(b) {
  if (!b) return null;
  const candidates = [b.phone, b.phone_number, b.mobile, b.tel];
  for (const v of candidates) {
    if (v != null && String(v).trim() !== "") return String(v).trim();
  }
  if (Array.isArray(b.phones) && b.phones.length) {
    const f = b.phones[0];
    if (typeof f === "string") return f;
    if (f && typeof f === "object") return f.value || f.phone || f.number || null;
  }
  return null;
}

function pickEmail(b) {
  if (!b) return null;
  if (b.email && String(b.email).trim() !== "") return String(b.email).trim();
  if (Array.isArray(b.emails) && b.emails.length) {
    const f = b.emails[0];
    if (typeof f === "string") return f;
    if (f && typeof f === "object") return f.value || f.email || null;
  }
  return null;
}

function pickFullName(b) {
  if (!b) return null;
  if (b.full_name) return String(b.full_name).trim();
  if (b.name) return String(b.name).trim();
  const fn = b.first_name || b.firstName || "";
  const ln = b.last_name || b.lastName || "";
  const composed = (fn + " " + ln).trim();
  return composed || null;
}

// KeyCRM custom field "Опт/Роздріб" має UUID="CT_1005", тип select.
// Значення приходить як масив, наприклад ["Опт"] або ["Системні"].
// is_wholesale = true ⟺ масив містить значення "Опт" (case-insensitive).
// Inспекція /api/cron/inspect-keycrm підтвердила цей UUID на бойових даних.
const WHOLESALE_FIELD_UUID = "CT_1005";
const WHOLESALE_FIELD_VALUE = "опт";

function parseWholesaleFlag(buyer) {
  if (!buyer) return false;
  const cf = buyer.custom_fields || buyer.customFields || buyer.fields;
  if (!cf) return false;
  const list = Array.isArray(cf)
    ? cf
    : Object.entries(cf).map(([k, v]) => ({ uuid: k, value: v }));
  for (const item of list) {
    const uuid = item && (item.uuid || item.id || item.key);
    if (uuid !== WHOLESALE_FIELD_UUID) continue;
    const v = item.value;
    if (v == null) return false;
    const values = Array.isArray(v) ? v : [v];
    for (const val of values) {
      if (val == null) continue;
      if (String(val).trim().toLowerCase() === WHOLESALE_FIELD_VALUE) return true;
    }
    return false;
  }
  return false;
}

async function upsertBuyersFromOrders(supabase, orders) {
  const byId = new Map();
  for (const order of orders) {
    const id = pickBuyerId(order);
    if (!id) continue;
    const b = pickBuyerObject(order) || {};
    const orderedAt = order.ordered_at || order.created_at;
    const customFields = b.custom_fields || b.customFields || b.fields || null;
    const row = {
      buyer_id: id,
      full_name: pickFullName(b),
      phone: pickPhone(b),
      email: pickEmail(b),
      is_wholesale: parseWholesaleFlag(b),
      custom_fields_raw: customFields,
      first_seen_at: orderedAt || null,
    };
    const prev = byId.get(id);
    if (!prev) byId.set(id, row);
    else {
      prev.full_name = prev.full_name || row.full_name;
      prev.phone = prev.phone || row.phone;
      prev.email = prev.email || row.email;
      prev.is_wholesale = prev.is_wholesale || row.is_wholesale;
      prev.custom_fields_raw = prev.custom_fields_raw || row.custom_fields_raw;
      if (row.first_seen_at && (!prev.first_seen_at || row.first_seen_at < prev.first_seen_at)) {
        prev.first_seen_at = row.first_seen_at;
      }
    }
  }
  const rows = Array.from(byId.values());
  if (!rows.length) return 0;

  // Merge-UPSERT через RPC: COALESCE на стороні SQL не дає затерти існуючі
  // повні дані пустим order.buyer. Без RPC простий upsert переписував
  // is_wholesale=true → false щоразу, як клієнт робив новий заказ і
  // KeyCRM повертав мінімальний buyer-об'єкт без custom_fields.
  for (const r of rows) {
    const { error } = await supabase.rpc("upsert_buyer_merge", {
      _buyer_id: r.buyer_id,
      _full_name: r.full_name,
      _phone: r.phone,
      _email: r.email,
      _is_wholesale: r.is_wholesale,
      _custom_fields_raw: r.custom_fields_raw,
      _first_seen_at: r.first_seen_at,
    });
    if (error) throw new Error("upsert_buyer_merge: " + error.message);
  }
  return rows.length;
}

async function fetchOrdersWithFilter(apiKey, supabase, ctx, filterKey, fromDate, toDate) {
  let page = 1;
  let total = 0;
  let upserted = 0;
  const params = {
    include: "products.offer,status,buyer",
    limit: 50,
    sort: "id",
  };
  params["filter[" + filterKey + "]"] = fromDate + "," + toDate;

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
      // Сума замовлення з урахуванням знижок (KeyCRM накладає знижки на весь
      // заказ, не на позицію). Записуємо однакове значення для всіх рядків
      // одного order_id; в buyer_rfm беремо MAX per order_id.
      const grandTotal = order.grand_total != null && !isNaN(parseFloat(order.grand_total))
        ? parseFloat(order.grand_total) : null;
      const orderDiscount = order.total_discount != null && !isNaN(parseFloat(order.total_discount))
        ? parseFloat(order.total_discount)
        : (order.discount_amount != null && !isNaN(parseFloat(order.discount_amount))
            ? parseFloat(order.discount_amount) : null);
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
          order_grand_total: grandTotal,
          order_discount: orderDiscount,
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

    // Покупатели — отдельный UPSERT, чтобы карточка клиента подтягивалась
    // ежедневно из новых/обновлённых заказов (имя, телефон, флаг "опт").
    try {
      await upsertBuyersFromOrders(supabase, rows);
    } catch (e) {
      // Не валим инжест из-за проблем со схемой клиентов — таблица buyers
      // может ещё не существовать (миграция не накатана). Логируем и едем
      // дальше.
      if (ctx) ctx.buyersUpsertError = (e && e.message) || String(e);
    }

    if (rows.length < 50) break;
    page++;
    await sleep(20);
  }

  return { ordersSeen: total, salesUpserted: upserted };
}

async function ingestSales(apiKey, supabase, ctx, sinceISO) {
  // Two-pass strategy to guarantee we capture both newly-created and recently-
  // updated orders. KeyCRM can be inconsistent: brand-new orders may not match
  // updated_between, and historical order edits may not match created_between.
  // We always +1 day on the end boundary so today's events aren't cut off by
  // exclusive-end-date interpretations on the API side. upsert by
  // (order_id, line_idx) deduplicates the overlap between passes.
  const fromDate = (sinceISO || new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString()).slice(0, 10);
  const toDate = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  // Run both passes in parallel — they hit different filters so KeyCRM
  // returns disjoint sets and the upsert dedupes anyway.
  const [a, b] = await Promise.all([
    fetchOrdersWithFilter(apiKey, supabase, ctx, "created_between", fromDate, toDate),
    fetchOrdersWithFilter(apiKey, supabase, ctx, "updated_between", fromDate, toDate),
  ]);
  return {
    ordersSeen: a.ordersSeen + b.ordersSeen,
    salesUpserted: a.salesUpserted + b.salesUpserted,
  };
}

async function deactivateMissing(supabase, runStartIso) {
  await supabase
    .from("skus")
    .update({ is_active: false })
    .lt("last_seen_at", runStartIso)
    .eq("is_active", true);
}

const PRODUCTS_CHUNK_PAGES = 8;

async function readState(supabase) {
  const { data, error } = await supabase
    .from("ingest_state")
    .select("*")
    .eq("id", 1)
    .maybeSingle();
  if (error) throw new Error("ingest_state read: " + error.message);
  return data;
}

async function writeState(supabase, patch) {
  const row = Object.assign({ id: 1, updated_at: new Date().toISOString() }, patch);
  const { error } = await supabase
    .from("ingest_state")
    .upsert(row, { onConflict: "id" });
  if (error) throw new Error("ingest_state write: " + error.message);
}

function todayUTC() {
  return new Date().toISOString().slice(0, 10);
}

function selfTriggerUrl(req) {
  // Prefer explicit env vars to ensure we hit the public production URL,
  // not whatever internal hostname Vercel passed in the cron request.
  let base;
  if (process.env.PUBLIC_URL) {
    base = process.env.PUBLIC_URL.replace(/\/+$/, "");
  } else if (process.env.VERCEL_PROJECT_PRODUCTION_URL) {
    base = "https://" + process.env.VERCEL_PROJECT_PRODUCTION_URL;
  } else if (process.env.VERCEL_URL) {
    base = "https://" + process.env.VERCEL_URL;
  } else {
    const proto = (req.headers["x-forwarded-proto"] || "https").split(",")[0].trim();
    base = proto + "://" + req.headers["host"];
  }
  const headerAuth = req.headers["authorization"] || req.headers["Authorization"] || "";
  const headerSecret = headerAuth.startsWith("Bearer ") ? headerAuth.slice(7) : "";
  const secret = (req.query && req.query.secret) || headerSecret || process.env.CRON_SECRET || "";
  return base + "/api/cron/ingest?step=auto&secret=" + encodeURIComponent(secret);
}

async function fireSelf(req) {
  const url = selfTriggerUrl(req);
  // Generous timeout: cold-start of the chained function can take 3-5s on
  // Vercel; aborting too early means the receiver never gets the request
  // and the chain dies after the first chunk.
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    await fetch(url, { method: "GET", signal: controller.signal, keepalive: true });
  } catch (_) {
    // Abort or transient error is fine — request already left this instance.
  } finally {
    clearTimeout(timer);
  }
}

async function runAutoChunk(req, supabase, apiKey, ctx) {
  let state = await readState(supabase);
  const today = todayUTC();

  // Decide whether to start a fresh cycle.
  const isStaleDone = !state || (state.status === "done" && state.cycle_date !== today);
  const isStaleIdle = state && state.status === "idle";
  // Recover automatically from an old error: if the previous run failed
  // (typically metrics refresh timeout) and >12h have passed, reset the
  // state and start a fresh cycle instead of getting stuck forever.
  const lastChunkMs = state && state.last_chunk_at
    ? new Date(state.last_chunk_at).getTime()
    : 0;
  const isStaleError = state && state.status === "error"
    && (Date.now() - lastChunkMs) > 12 * 3600 * 1000;
  // Recover from stuck "running" state: якщо стан "running" і last_chunk_at
  // старіший за 1 годину — значить Vercel функція впала по таймауту (60s)
  // не встигнувши оновити статус. Скидаємо на свіжий цикл — наступний
  // тригер (cron / manual / GitHub Actions backup) почне з products.
  const isStaleRunning = state && state.status === "running"
    && lastChunkMs > 0
    && (Date.now() - lastChunkMs) > 60 * 60 * 1000;
  if (isStaleDone || isStaleIdle || isStaleError || isStaleRunning) {
    const fresh = {
      cycle_date: today,
      cycle_started_at: new Date().toISOString(),
      current_step: "products",
      current_page: 1,
      status: "running",
      last_error: isStaleRunning ? "stale-running auto-recovered" : null,
    };
    await writeState(supabase, fresh);
    state = Object.assign({ id: 1 }, fresh);
  } else if (state.status === "done") {
    return { phase: "noop", reason: "already_done_today", state };
  }
  // status === "running" з свіжим last_chunk_at → resume from saved cursor.

  const stepName = state.current_step;
  const cycleStartIso = state.cycle_started_at;
  const result = { phase: stepName, page: state.current_page };

  try {
    if (stepName === "products") {
      const r = await ingestProducts(apiKey, supabase, ctx, state.current_page, PRODUCTS_CHUNK_PAGES);
      result.products = r.total;
      result.lastPage = r.lastPage;
      result.sample_keys = r.sampleKeys;
      result.created_at_hits = r.createdAtHits;
      if (r.more) {
        await writeState(supabase, {
          current_page: r.lastPage + 1,
          last_chunk_at: new Date().toISOString(),
        });
      } else {
        // Products done — deactivate stale SKUs, flag restocks, advance to sales.
        await deactivateMissing(supabase, cycleStartIso);
        try {
          const rs = await supabase.rpc("detect_restocks", { target_date: todayUTC() });
          if (!rs.error) result.restocks_flagged = rs.data;
        } catch (_) {}
        await writeState(supabase, {
          current_step: "sales",
          current_page: 1,
          last_chunk_at: new Date().toISOString(),
        });
        result.advanced = "sales";
      }
    } else if (stepName === "sales") {
      const since = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString();
      const r = await ingestSales(apiKey, supabase, ctx, since);
      result.orders = r.ordersSeen;
      result.sales_upserted = r.salesUpserted;
      await writeState(supabase, {
        current_step: "metrics",
        current_page: 1,
        last_chunk_at: new Date().toISOString(),
      });
      result.advanced = "metrics";
    } else if (stepName === "metrics") {
      // Soft refresh: try once with a short local timeout. If REFRESH
      // MATERIALIZED VIEW doesn't complete in 8s (typical at our scale
      // is 30-90s), we don't fail the cycle — pg_cron has the heavy
      // refresh scheduled for 03:10 UTC and will finish it server-side.
      try {
        await Promise.race([
          (async () => {
            const r = await supabase.rpc("refresh_sku_metrics");
            if (r && r.error) throw new Error(r.error.message);
          })(),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("local refresh timeout, deferring to pg_cron")), 8000)
          ),
        ]);
        result.metrics = true;
      } catch (err) {
        result.metrics_deferred = (err && err.message) || String(err);
      }
      await writeState(supabase, {
        current_step: "done",
        current_page: 1,
        status: "done",
        last_chunk_at: new Date().toISOString(),
      });
      result.advanced = "done";
    } else {
      // Unknown step — reset.
      await writeState(supabase, { current_step: "products", current_page: 1, status: "running" });
      result.reset = true;
    }
  } catch (err) {
    await writeState(supabase, {
      status: "error",
      last_error: ((err && err.message) || String(err)).substring(0, 500),
      last_chunk_at: new Date().toISOString(),
    });
    throw err;
  }

  return result;
}

module.exports = async function handler(req, res) {
  const auth = checkCronAuth(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

  const apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

  const supabase = getSupabase();
  const ctx = { apiCalls: 0 };
  const runStartIso = new Date().toISOString();
  const step = (req.query && req.query.step) || "auto";
  const fromPage = req.query && req.query.from;
  const take = req.query && req.query.take;
  let runId = null;

  try {
    const ins = await supabase
      .from("ingest_runs")
      .insert({ kind: step === "auto" ? "auto" : "manual", status: "running", meta: { step, fromPage, take } })
      .select("id")
      .single();
    if (ins.error) throw new Error("ingest_runs insert: " + ins.error.message);
    runId = ins.data.id;

    if (step === "auto") {
      // Process chunks in a tight loop within a single function invocation.
      // Avoids the previous self-trigger pattern, which was flaky on cron
      // cold-starts (chain dropped after 1-5 chunks instead of completing
      // ~12 chunks). Time budget 50s leaves headroom under Vercel's 60s
      // function limit; state is persisted per-chunk so next-day cron
      // resumes if today's invocation runs out of time.
      const startMs = Date.now();
      const TIME_BUDGET_MS = 55 * 1000;
      let chunksProcessed = 0;
      let totalProducts = 0, totalOrders = 0, totalSalesUpserted = 0;
      let lastResult = null;

      while (Date.now() - startMs < TIME_BUDGET_MS) {
        lastResult = await runAutoChunk(req, supabase, apiKey, ctx);
        chunksProcessed += 1;
        totalProducts += lastResult.products || 0;
        totalOrders += lastResult.orders || 0;
        totalSalesUpserted += lastResult.sales_upserted || 0;
        const stateAfter = await readState(supabase);
        if (!stateAfter || stateAfter.status !== "running") break;
      }

      const finalState = await readState(supabase);
      const completed = finalState && finalState.status === "done";

      await supabase
        .from("ingest_runs")
        .update({
          finished_at: new Date().toISOString(),
          status: "ok",
          products_seen: totalProducts,
          orders_seen: totalOrders,
          sales_upserted: totalSalesUpserted,
          api_calls: ctx.apiCalls,
          meta: { step: "auto", chunks_processed: chunksProcessed, last_chunk: lastResult, completed },
        })
        .eq("id", runId);

      return res.status(200).json({
        ok: true,
        step: "auto",
        run_id: runId,
        chunks_processed: chunksProcessed,
        completed,
        state: finalState,
        last_chunk: lastResult,
        api_calls: ctx.apiCalls,
      });
    }

    // Manual one-shot modes (for debugging / forced runs).
    let productsSeen = 0, offersSeen = 0, ordersSeen = 0, salesUpserted = 0;
    let didMetrics = false;
    let nextPage = null;

    let restocksFlagged = null;
    let productsSampleKeys = null;
    let createdAtHits = null;
    if (step === "products" || step === "all") {
      const r = await ingestProducts(apiKey, supabase, ctx, fromPage, take);
      productsSeen = r.total;
      productsSampleKeys = r.sampleKeys;
      createdAtHits = r.createdAtHits;
      if (r.more) nextPage = r.lastPage + 1;
      if (!r.more) {
        try {
          const rs = await supabase.rpc("detect_restocks", { target_date: todayUTC() });
          if (!rs.error) restocksFlagged = rs.data;
        } catch (_) {}
      }
    }
    if (step === "offers" || step === "all") {
      const r = await ingestOffers(apiKey, supabase, ctx, fromPage, take);
      offersSeen = r.offersSeen;
      if (r.more) nextPage = r.lastPage + 1;
      if (!r.more && step === "offers") {
        await deactivateMissing(supabase, runStartIso);
      } else if (step === "all") {
        await deactivateMissing(supabase, runStartIso);
      }
    }
    if (step === "sales" || step === "all") {
      const since = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString();
      const r = await ingestSales(apiKey, supabase, ctx, since);
      ordersSeen = r.ordersSeen;
      salesUpserted = r.salesUpserted;
    }
    if (step === "metrics" || step === "all") {
      const refresh = await supabase.rpc("refresh_sku_metrics");
      if (refresh.error) throw new Error("refresh_sku_metrics: " + refresh.error.message);
      didMetrics = true;
    }

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
      step,
      run_id: runId,
      products: productsSeen,
      offers: offersSeen,
      orders: ordersSeen,
      sales_upserted: salesUpserted,
      metrics_refreshed: didMetrics,
      restocks_flagged: restocksFlagged,
      sample_keys: productsSampleKeys,
      created_at_hits: createdAtHits,
      api_calls: ctx.apiCalls,
      next_page: nextPage,
      more: nextPage !== null,
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
