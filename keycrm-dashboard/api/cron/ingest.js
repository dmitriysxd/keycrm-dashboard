const { getSupabase } = require("../../lib/supabase");
const { checkCronAuth, checkDashboardToken } = require("../../lib/auth");
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
  // Підтягуємо ВЕСЬ список категорій один раз — щоб для offer'ів у яких
  // /offers?include=product не повернув category.name, можна було підставити
  // назву через product.category_id. Без цього category_name=NULL і в
  // дашборді "Без назви категорії" роздувається.
  const categoriesMap = await fetchCategories(apiKey, ctx);
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
      // Закупівельна ціна з KeyCRM — синхронізуємо в skus.cost щоб рахувати
      // заморожений капітал (cost × current_stock) і маржу. KeyCRM зберігає
      // це поле на рівні offer (варіанту), а не master-продукту.
      const purchasedPrice = parseFloat(o.purchased_price);
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
      // Fallback на категорійний кеш — KeyCRM в /offers?include=product не
      // завжди повертає product.category.name (особливо для нових категорій).
      const categoryName = (product.category && product.category.name)
        || (categoryId ? categoriesMap[String(categoryId)] : null)
        || null;

      const keycrmCreatedAt = pickCreatedAt(product) || pickCreatedAt(o);
      skuRows.push({
        offer_id: offerId,
        product_id: productId,
        sku: o.sku || null,
        name: displayName,
        category_id: categoryId,
        category_name: categoryName,
        price: isNaN(price) ? null : price,
        cost: isNaN(purchasedPrice) || purchasedPrice <= 0 ? null : purchasedPrice,
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

// KeyCRM /buyer/{id} віддає phone та email як МАСИВ (напр. ["+380.."]).
// Беремо перший непорожній елемент, а не String(array) — інакше
// багатоелементний масив склеювався у "a@b.com,c@d.net".
function firstFromArrayOrScalar(v) {
  if (v == null) return null;
  if (Array.isArray(v)) {
    for (const item of v) {
      if (item == null) continue;
      if (typeof item === "string") { if (item.trim() !== "") return item.trim(); }
      else if (typeof item === "object") {
        const inner = item.value || item.phone || item.number || item.email;
        if (inner != null && String(inner).trim() !== "") return String(inner).trim();
      }
    }
    return null;
  }
  return String(v).trim() !== "" ? String(v).trim() : null;
}

function pickPhone(b) {
  if (!b) return null;
  const candidates = [b.phone, b.phone_number, b.mobile, b.tel, b.phones];
  for (const v of candidates) {
    const got = firstFromArrayOrScalar(v);
    if (got) return got;
  }
  return null;
}

function pickEmail(b) {
  if (!b) return null;
  const candidates = [b.email, b.emails];
  for (const v of candidates) {
    const got = firstFromArrayOrScalar(v);
    if (got) return got;
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

// Кеш джерел замовлень. Завантажуємо один раз на виклик ingestSales і
// підставляємо назви для замовлень де KeyCRM повернув тільки source_id.
async function fetchOrderSources(apiKey, ctx) {
  const map = {};
  for (const path of ["/order/source", "/sources", "/order/sources", "/source"]) {
    try {
      const r = await get(path, { limit: 100 }, apiKey, ctx);
      const list = (r && r.data) || (Array.isArray(r) ? r : null);
      if (list && list.length) {
        for (const s of list) {
          if (s && s.id != null) map[String(s.id)] = s.name || null;
        }
        return map;
      }
    } catch (_) { /* пробуємо наступний */ }
  }
  return map;
}

async function fetchOrdersWithFilter(apiKey, supabase, ctx, filterKey, fromDate, toDate, sourcesMap) {
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
      // Джерело замовлення з KeyCRM. Поле буває як source: {id, name}
      // або на верхньому рівні source_id + source_name (рідше).
      const srcObj = order.source && typeof order.source === "object" ? order.source : null;
      const sourceId = srcObj && srcObj.id != null ? parseInt(srcObj.id)
        : (order.source_id != null ? parseInt(order.source_id) : null);
      let sourceName = srcObj && srcObj.name ? String(srcObj.name)
        : (order.source_name ? String(order.source_name) : null);
      // Якщо в самому замовленні нема назви — беремо зі словника джерел.
      if (!sourceName && sourceId != null && sourcesMap && sourcesMap[String(sourceId)]) {
        sourceName = sourcesMap[String(sourceId)];
      }
      const items = order.products || [];
      items.forEach((item, idx) => {
        const qty = lineQty(item);
        if (qty <= 0) return;
        const price = linePrice(item);
        const offer = item.offer || {};
        // Точна закупка на момент створення замовлення (KeyCRM фіксує snapshot).
        // Якщо нема — null, у звітах буде fallback на skus.cost.
        const lineCost = item.purchased_price != null && !isNaN(parseFloat(item.purchased_price))
          ? parseFloat(item.purchased_price)
          : null;
        lines.push({
          order_id: order.id,
          line_idx: idx,
          offer_id: offer.id || null,
          product_id: offer.product_id || null,
          name_snapshot: item.name || offer.name || null,
          quantity: qty,
          unit_price: price,
          revenue: price * qty,
          line_cost: lineCost,
          order_status: status,
          ordered_at: orderedAt,
          buyer_id: buyerId,
          order_grand_total: grandTotal,
          order_discount: orderDiscount,
          source_id: sourceId && !isNaN(sourceId) ? sourceId : null,
          source_name: sourceName,
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

  // Завантажуємо словник джерел один раз. /order повертає тільки source_id
  // без назви — підставляємо name з цього словника.
  const sourcesMap = await fetchOrderSources(apiKey, ctx);
  // Run both passes in parallel — they hit different filters so KeyCRM
  // returns disjoint sets and the upsert dedupes anyway.
  const [a, b] = await Promise.all([
    fetchOrdersWithFilter(apiKey, supabase, ctx, "created_between", fromDate, toDate, sourcesMap),
    fetchOrdersWithFilter(apiKey, supabase, ctx, "updated_between", fromDate, toDate, sourcesMap),
  ]);
  return {
    ordersSeen: a.ordersSeen + b.ordersSeen,
    salesUpserted: a.salesUpserted + b.salesUpserted,
  };
}

// ─── Reconcile: розпізнавання об'єднань покупців (merge) в KeyCRM ────────
//
// Коли в KeyCRM об'єднують дублі покупця, старий buyer_id видаляється
// (наш backfill ставить йому full_name="(видалено в KeyCRM)"), а всі його
// замовлення перепривʼязуються на survivor. Але в нашій sales вони лишаються
// висіти на мертвому id. Цей крок:
//   1. бере "мертві" профілі, що ще мають замовлення (merged_buyer_candidates),
//   2. перечитує кожне їх замовлення з KeyCRM /order/{id} → знаходить survivor,
//   3. UPDATE sales SET buyer_id = survivor,
//   4. перечитує картку survivor /buyer/{id} → відновлює опт/ім'я/email,
//   5. видаляє порожні мертві профілі (cleanup_orphan_deleted_buyers).
//
// Дешевий у звичайні дні (кандидатів зазвичай 0 — швидкий no-op RPC).
async function reconcileMergedBuyers(apiKey, supabase, ctx, opts) {
  opts = opts || {};
  const startMs = Date.now();
  const timeBudgetMs = opts.timeBudgetMs || 45000;
  const maxBuyers = opts.maxBuyers || 25;
  const maxOrderFetches = opts.maxOrderFetches || 120;

  const reattributed = [];        // {order_id, from, to}
  const deletedOrphans = [];
  const survivors = new Set();
  let orderFetches = 0;

  const cand = await supabase.rpc("merged_buyer_candidates", { _limit: maxBuyers });
  if (cand.error) throw new Error("merged_buyer_candidates: " + cand.error.message);
  const candidates = cand.data || [];

  for (const c of candidates) {
    if (Date.now() - startMs > timeBudgetMs) break;
    if (orderFetches >= maxOrderFetches) break;
    const deadId = c.buyer_id;

    // Усі замовлення, що ще висять на мертвому id.
    const ordRes = await supabase.from("sales").select("order_id").eq("buyer_id", deadId);
    if (ordRes.error) throw new Error("sales select for reconcile: " + ordRes.error.message);
    const orderIds = Array.from(new Set((ordRes.data || []).map((r) => r.order_id)));

    for (const oid of orderIds) {
      if (orderFetches >= maxOrderFetches) break;
      if (Date.now() - startMs > timeBudgetMs) break;
      orderFetches++;
      let survivor = null;
      try {
        const resp = await get("/order/" + oid, { include: "buyer" }, apiKey, ctx);
        const order = (resp && resp.data) || resp;
        survivor = pickBuyerId(order);
      } catch (e) {
        // /order/{id} 404 → саме замовлення видалене в KeyCRM. Пропускаємо.
        await sleep(120);
        continue;
      }
      if (survivor && survivor !== deadId) {
        const upd = await supabase
          .from("sales")
          .update({ buyer_id: survivor })
          .eq("order_id", oid)
          .eq("buyer_id", deadId);
        if (!upd.error) {
          reattributed.push({ order_id: oid, from: deadId, to: survivor });
          survivors.add(survivor);
        }
      }
      await sleep(120);
    }

    // Якщо на мертвому id більше немає замовлень — видаляємо профіль.
    const rem = await supabase
      .from("sales")
      .select("order_id", { count: "exact", head: true })
      .eq("buyer_id", deadId);
    if (!rem.error && (rem.count || 0) === 0) {
      await supabase.from("buyers").delete().eq("buyer_id", deadId);
      deletedOrphans.push(deadId);
    }
  }

  // Перечитуємо картки survivor'ів — відновлюємо опт-прапорець / ім'я / email,
  // які могли не підтягнутись (survivor синкався ДО об'єднання).
  for (const sid of survivors) {
    if (Date.now() - startMs > timeBudgetMs) break;
    try {
      const resp = await get("/buyer/" + sid, { include: "custom_fields" }, apiKey, ctx);
      const b = (resp && resp.data) || resp;
      if (b && b.id) {
        await supabase.rpc("upsert_buyer_merge", {
          _buyer_id: sid,
          _full_name: pickFullName(b),
          _phone: pickPhone(b),
          _email: pickEmail(b),
          _is_wholesale: parseWholesaleFlag(b),
          _custom_fields_raw: b.custom_fields || b.customFields || b.fields || null,
          _first_seen_at: null,
        });
      }
    } catch (_) { /* survivor зник — рідкісно, ігноруємо */ }
    await sleep(120);
  }

  // Прибираємо порожні мертві профілі (включно з тими, що очистились
  // автоматично через updated_between-пас у sales).
  let orphanStubsCleaned = 0;
  try {
    const cl = await supabase.rpc("cleanup_orphan_deleted_buyers");
    if (!cl.error && cl.data != null) orphanStubsCleaned = parseInt(cl.data) || 0;
  } catch (_) {}

  // Якщо щось рухали — освіжаємо buyer_rfm.
  if (reattributed.length || survivors.size || deletedOrphans.length || orphanStubsCleaned) {
    try { await supabase.rpc("refresh_buyer_rfm"); } catch (_) {}
  }

  return {
    candidates_seen: candidates.length,
    order_fetches: orderFetches,
    reattributed_count: reattributed.length,
    reattributed_sample: reattributed.slice(0, 30),
    survivors_resynced: Array.from(survivors),
    deleted_orphan_buyers: deletedOrphans,
    orphan_stubs_cleaned: orphanStubsCleaned,
    more: candidates.length >= maxBuyers || orderFetches >= maxOrderFetches,
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
const OFFERS_CHUNK_PAGES = 6; // offers важчі (більше полів) — менше сторінок за чанк

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
        //
        // ВАЖЛИВО: крок "offers" БІЛЬШЕ НЕ виконується в авто-циклі — він
        // занадто важкий (~96 сторінок × 50 = 4800 записів) і не вкладається
        // в одиничний денний виклик Vercel (60s). Synchronization
        // purchased_price (cost) винесена в окремий weekly workflow:
        // .github/workflows/weekly-sync-offers.yml — там вона спокійно
        // прокачується послідовними викликами раз на тиждень.
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
    } else if (stepName === "offers") {
      // Старий крок — викликається ТІЛЬКИ при ручному запиті ?step=offers
      // (наприклад з weekly GitHub Actions). У авто-циклі сюди не потрапляємо.
      // Залишений для backwards-compatibility з застряглими state-ами.
      const r = await ingestOffers(apiKey, supabase, ctx, state.current_page, OFFERS_CHUNK_PAGES);
      result.offers = r.offersSeen;
      result.lastPage = r.lastPage;
      if (r.more) {
        await writeState(supabase, {
          current_page: r.lastPage + 1,
          last_chunk_at: new Date().toISOString(),
        });
      } else {
        // offers закінчились — переходимо до sales щоб не залишити state в
        // 'offers' назавжди. Якщо це викликали з weekly workflow після
        // основного auto-циклу, sales вже зробився, шкоди не буде.
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
      // Фінальний крок: рефреш обох matview. pg_cron більше не запускається
      // на фіксованому розкладі (міграція 036) — щоб уникнути гонки з ingest.
      // Тепер це єдине місце де matview оновлюються автоматично.
      //
      // Реальні тривалості за історією: sku_metrics 4-15s, buyer_rfm 3-5s.
      // PG statement_timeout для цих функцій піднято до 180s (міграція 040),
      // тому наш JS-таймаут НЕ має різати раніше за PG. Даємо sku 45s
      // (з запасом), rfm 12s — сумарно вкладаємось у 60s Vercel ліміт, бо
      // цей чанк тільки рефрешить, нічого важкого до нього не робить.
      const refreshWith = async (rpcName, timeoutMs) => {
        return Promise.race([
          (async () => {
            const r = await supabase.rpc(rpcName);
            if (r && r.error) throw new Error(r.error.message);
            return true;
          })(),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error(rpcName + " timeout " + timeoutMs + "ms")), timeoutMs)
          ),
        ]);
      };

      try {
        await refreshWith("refresh_sku_metrics", 45000);
        result.metrics_sku = true;
      } catch (err) {
        result.metrics_sku_error = (err && err.message) || String(err);
      }

      try {
        await refreshWith("refresh_buyer_rfm", 15000);
        result.metrics_rfm = true;
      } catch (err) {
        result.metrics_rfm_error = (err && err.message) || String(err);
      }

      // Після метрик — крок reconcile (розпізнавання об'єднань покупців).
      await writeState(supabase, {
        current_step: "reconcile",
        current_page: 1,
        last_chunk_at: new Date().toISOString(),
      });
      result.advanced = "reconcile";
    } else if (stepName === "reconcile") {
      // Розпізнавання merge'ів. У звичайні дні кандидатів 0 — швидкий no-op.
      // Тримаємо tight cap, щоб не вибити денний бюджет якщо KeyCRM повільний.
      // Помилки тут НЕ валять цикл — завжди завершуємо 'done'.
      try {
        result.reconcile = await reconcileMergedBuyers(apiKey, supabase, ctx, {
          timeBudgetMs: 30000,
          maxBuyers: 10,
          maxOrderFetches: 60,
        });
      } catch (err) {
        result.reconcile_error = (err && err.message) || String(err);
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
  // Дозволяємо обидва способи авторизації:
  // - CRON_SECRET (для Vercel cron / GitHub Actions / curl)
  // - DASHBOARD_TOKEN (для кнопки "Підтягнути свіже" в UI)
  const cronAuth = checkCronAuth(req);
  if (!cronAuth.ok) {
    const dashAuth = checkDashboardToken(req);
    if (!dashAuth.ok) return res.status(cronAuth.status).json({ error: cronAuth.error });
  }

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
    let reconcileResult = null;
    if (step === "reconcile") {
      // Ручний/повний прогін розпізнавання об'єднань. Більший бюджет і ліміти,
      // ніж у авто-циклі — щоб за один-два виклики добити весь backlog.
      reconcileResult = await reconcileMergedBuyers(apiKey, supabase, ctx, {
        timeBudgetMs: 50000,
        maxBuyers: 40,
        maxOrderFetches: 150,
      });
    }
    let backfillSource = null;
    if (step === "backfill_source") {
      // One-off бекфіл: підтягує source_id/source_name для всіх історичних
      // замовлень з KeyCRM /order і UPDATE'ить sales WHERE source_id IS NULL.
      // Не state-machine — приймає ?from=N, повертає next_page.
      const startMs = Date.now();
      const TIME_BUDGET_MS = 50 * 1000;
      const fromP = Math.max(1, parseInt(fromPage || 1));

      // Завантажуємо мапу id→name джерел з KeyCRM один раз.
      // KeyCRM має ендпойнт /order/source — список усіх джерел замовлень.
      // /order повертає source_id у самому замовленні, але без назви, тому
      // підставляємо назву зі словника.
      const sourcesMap = {};
      let sourcesEndpoint = null;
      for (const path of ["/order/source", "/sources", "/order/sources", "/source"]) {
        try {
          const r = await get(path, { limit: 100 }, apiKey, ctx);
          const list = (r && r.data) || (Array.isArray(r) ? r : null);
          if (list && list.length) {
            for (const s of list) {
              if (s && s.id != null) sourcesMap[String(s.id)] = s.name || null;
            }
            sourcesEndpoint = path;
            break;
          }
        } catch (_) { /* пробуємо наступний */ }
      }

      let page = fromP;
      let pagesDone = 0;
      let ordersSeenBf = 0;
      let ordersUpdated = 0;
      let linesUpdated = 0;
      let endReached = false;
      while (Date.now() - startMs < TIME_BUDGET_MS) {
        // include=products.offer — щоб у відповіді була і структура позицій
        // з purchased_price для бекфілу sales.line_cost (точна закупка на
        // момент створення замовлення).
        const resp = await get("/order", { page, limit: 50, include: "products.offer" }, apiKey, ctx);
        const rows = (resp && resp.data) || [];
        if (!rows.length) { endReached = true; break; }
        ordersSeenBf += rows.length;
        for (const order of rows) {
          // 1) source_id / source_name
          const srcObj = order.source && typeof order.source === "object" ? order.source : null;
          const sId = srcObj && srcObj.id != null ? parseInt(srcObj.id)
            : (order.source_id != null ? parseInt(order.source_id) : null);
          let sName = srcObj && srcObj.name ? String(srcObj.name)
            : (order.source_name ? String(order.source_name) : null);
          if (!sName && sId != null && sourcesMap[String(sId)]) sName = sourcesMap[String(sId)];
          if (sId != null || sName) {
            const upd = await supabase
              .from("sales")
              .update({
                source_id: sId && !isNaN(sId) ? sId : null,
                source_name: sName,
              })
              .eq("order_id", order.id);
            if (!upd.error) ordersUpdated++;
          }

          // 2) line_cost для кожної позиції з item.purchased_price (snapshot)
          const products = order.products || [];
          const costJobs = [];
          products.forEach((item, idx) => {
            const c = item.purchased_price;
            if (c == null || isNaN(parseFloat(c))) return;
            costJobs.push({ idx, cost: parseFloat(c) });
          });
          if (costJobs.length) {
            // Паралельний UPDATE по (order_id, line_idx) — швидко (~50ms/line).
            const results = await Promise.all(costJobs.map(j =>
              supabase
                .from("sales")
                .update({ line_cost: j.cost })
                .eq("order_id", order.id)
                .eq("line_idx", j.idx)
            ));
            for (const r of results) if (!r.error) linesUpdated++;
          }
        }
        pagesDone++;
        page++;
        if (rows.length < 50) { endReached = true; break; }
      }
      backfillSource = {
        from_page: fromP,
        pages_processed: pagesDone,
        orders_seen: ordersSeenBf,
        lines_updated: linesUpdated,
        orders_updated: ordersUpdated,
        next_page: endReached ? null : page,
        done: endReached,
        sources_endpoint: sourcesEndpoint,
        sources_count: Object.keys(sourcesMap).length,
      };
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
      backfill_source: backfillSource,
      reconcile: reconcileResult,
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
