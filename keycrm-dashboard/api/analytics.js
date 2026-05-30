// Аналітика: тренди обороту і retention-когорти.
//
// GET /api/analytics?type=trends&bucket=week|month|year
//   Часовий ряд: orders / buyers / revenue / aov по бакетам.
//
// GET /api/analytics?type=cohorts
//   Retention-матриця: рядки = місяць першої покупки, колонки = місяців
//   з тої події, клітинки = % когорти, які купили у тому місяці.
//
// З розрахунку виключаються скасовані / повернуті замовлення.

const { getSupabase } = require("../lib/supabase");
const { checkDashboardToken } = require("../lib/auth");

// Pagination wrapper — Supabase повертає max 1000 рядків за запит.
async function fetchAll(buildQuery, pageSize = 1000, hardCap = 500000) {
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

const EXCLUDED_STATUSES = new Set([
  "cancelled", "rejected", "canceled",
  "Повернули", "Повернення", "Відмовились",
  "incorrect_data", "underbid",
]);

function isExcluded(orderStatus) {
  if (!orderStatus) return false;
  if (EXCLUDED_STATUSES.has(orderStatus)) return true;
  // LIKE 'Об%єднання замовлень'
  if (typeof orderStatus === "string" && orderStatus.indexOf("Об") === 0 && orderStatus.indexOf("єднання замовлень") !== -1) return true;
  return false;
}

// ── Бакет дати ─────────────────────────────────────────
function bucketKey(date, bucket) {
  const d = new Date(date);
  if (bucket === "year") return String(d.getUTCFullYear());
  if (bucket === "week") {
    // ISO-тиждень (понеділок = 1). Простіше: повертаємо ISO дату початку
    // понеділкового тижня UTC.
    const tmp = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    const day = tmp.getUTCDay() || 7;
    tmp.setUTCDate(tmp.getUTCDate() - (day - 1));
    return tmp.toISOString().slice(0, 10);
  }
  // month
  return d.toISOString().slice(0, 7); // YYYY-MM
}

async function handleTrends(req, res, supabase) {
  const bucket = (req.query && req.query.bucket) || "month";
  if (!["week", "month", "year"].includes(bucket)) {
    return res.status(400).json({ error: "bucket must be week|month|year" });
  }

  // Виключаємо роздрібних клієнтів — сторінка про опт-бізнес.
  const wholesaleBuyers = await fetchAll(() =>
    supabase.from("buyers").select("buyer_id").eq("is_wholesale", true)
  );
  const wholesaleSet = new Set(wholesaleBuyers.map(b => b.buyer_id));

  // Беремо grand_total з sales (як в /api/clients) — це сума замовлення з
  // урахуванням знижок. Group by order_id робимо вже у Node, бо sales —
  // рядки замовлень.
  const since = new Date();
  since.setUTCFullYear(since.getUTCFullYear() - 2); // 2 роки історії — достатньо

  const rows = await fetchAll(() =>
    supabase
      .from("sales")
      .select("order_id, ordered_at, revenue, buyer_id, order_status, order_grand_total")
      .gte("ordered_at", since.toISOString())
  );

  // Дедуплікуємо по order_id, бо grand_total — на замовлення.
  const seenOrders = new Map();
  for (const r of rows) {
    if (isExcluded(r.order_status)) continue;
    if (!r.ordered_at) continue;
    // Фільтр по опт-клієнтах. Замовлення без buyer_id (анонімні / тільки що
    // створені) виключаємо — статистика опт-бізнесу.
    if (!r.buyer_id || !wholesaleSet.has(r.buyer_id)) continue;
    if (!seenOrders.has(r.order_id)) {
      seenOrders.set(r.order_id, {
        ordered_at: r.ordered_at,
        revenue: r.order_grand_total != null ? parseFloat(r.order_grand_total) : 0,
        buyer_id: r.buyer_id,
        has_grand_total: r.order_grand_total != null,
        line_revenue: 0,
      });
    }
    // Fallback: якщо grand_total NULL — сумуємо line-revenue.
    if (r.order_grand_total == null) {
      const o = seenOrders.get(r.order_id);
      o.line_revenue += r.revenue != null ? parseFloat(r.revenue) : 0;
    }
  }

  const buckets = new Map(); // key → { orders: Set, buyers: Set, revenue: number }
  for (const [orderId, o] of seenOrders) {
    const key = bucketKey(o.ordered_at, bucket);
    if (!buckets.has(key)) buckets.set(key, { orders: 0, buyers: new Set(), revenue: 0 });
    const b = buckets.get(key);
    b.orders += 1;
    if (o.buyer_id) b.buyers.add(o.buyer_id);
    b.revenue += o.has_grand_total ? o.revenue : o.line_revenue;
  }

  const series = Array.from(buckets.entries())
    .sort((a, b) => (a[0] < b[0] ? -1 : 1))
    .map(([key, b]) => ({
      bucket: key,
      orders: b.orders,
      buyers: b.buyers.size,
      revenue: Math.round(b.revenue),
      aov: b.orders > 0 ? Math.round(b.revenue / b.orders) : 0,
    }));

  return res.status(200).json({ bucket, series, total_orders: seenOrders.size });
}

async function handleCohorts(req, res, supabase) {
  // Параметр periodMonths: 12 / 24 / 36 / all. Скільки місячних когорт показати.
  const periodParam = (req.query && req.query.months) || "24";
  const periodMonths = periodParam === "all" ? null : parseInt(periodParam);

  // Тільки опт-клієнти (як на сторінці клієнтів).
  const buyers = await fetchAll(() =>
    supabase.from("buyers").select("buyer_id").eq("is_wholesale", true)
  );
  const wholesaleSet = new Set(buyers.map(b => b.buyer_id));

  // Завантажуємо продажі з buyer_id. Якщо обрано period — обмежуємо вікно.
  const since = new Date();
  if (periodMonths) {
    // +12 міс запасу щоб поміряти retention для самих ранніх когорт
    since.setUTCMonth(since.getUTCMonth() - periodMonths - 12);
  } else {
    since.setUTCFullYear(since.getUTCFullYear() - 5);
  }
  const sales = await fetchAll(() =>
    supabase
      .from("sales")
      .select("buyer_id, ordered_at, order_status, order_id")
      .gte("ordered_at", since.toISOString())
      .not("buyer_id", "is", null)
  );

  // Дедуплікуємо: один рядок на buyer × month активності.
  const buyerFirst = new Map(); // buyer_id → first month
  const activity = new Map();   // buyer_id → Set<month>

  for (const r of sales) {
    if (isExcluded(r.order_status)) continue;
    if (!wholesaleSet.has(r.buyer_id)) continue;
    const m = (r.ordered_at || "").slice(0, 7); // YYYY-MM
    if (!m) continue;
    if (!activity.has(r.buyer_id)) activity.set(r.buyer_id, new Set());
    activity.get(r.buyer_id).add(m);
    const prev = buyerFirst.get(r.buyer_id);
    if (!prev || m < prev) buyerFirst.set(r.buyer_id, m);
  }

  // Будуємо когорти.
  // cohort_size[YYYY-MM] = скільки buyer'ів першого разу купили у цьому місяці.
  // matrix[cohort][monthsAfter] = скільки з тих buyer'ів активні через X місяців.
  const cohortSize = new Map();
  const matrix = new Map();

  for (const [buyerId, firstMonth] of buyerFirst) {
    cohortSize.set(firstMonth, (cohortSize.get(firstMonth) || 0) + 1);
    const acts = activity.get(buyerId);
    if (!acts) continue;
    for (const m of acts) {
      const monthsAfter = monthDiff(firstMonth, m);
      if (monthsAfter < 0) continue;
      const key = firstMonth + "|" + monthsAfter;
      matrix.set(key, (matrix.get(key) || 0) + 1);
    }
  }

  // Формат для UI: масив когорт, кожна — { cohort: 'YYYY-MM', size, retention: [{month_after, active, pct}, ...] }
  const cohortKeys = Array.from(cohortSize.keys()).sort();
  const maxMonths = monthDiff(cohortKeys[0] || "2000-01", new Date().toISOString().slice(0, 7));
  const out = cohortKeys.map(cohort => {
    const size = cohortSize.get(cohort);
    const maxMonthsForThis = monthDiff(cohort, new Date().toISOString().slice(0, 7));
    const retention = [];
    for (let i = 0; i <= maxMonthsForThis; i++) {
      const active = matrix.get(cohort + "|" + i) || 0;
      retention.push({
        month_after: i,
        active,
        pct: size > 0 ? Math.round((active / size) * 1000) / 10 : 0, // 1 decimal
      });
    }
    return { cohort, size, retention };
  });

  // Якщо обрано period — обрізаємо до останніх N когорт.
  const trimmed = periodMonths ? out.slice(-periodMonths) : out;

  return res.status(200).json({
    cohorts: trimmed,
    total_buyers: buyerFirst.size,
    max_months_window: maxMonths,
    period_months: periodMonths,
  });
}

function monthDiff(from, to) {
  // 'YYYY-MM' formats
  const [y1, m1] = from.split("-").map(Number);
  const [y2, m2] = to.split("-").map(Number);
  return (y2 - y1) * 12 + (m2 - m1);
}


module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();
  const type = (req.query && req.query.type) || "";

  try {
    if (type === "trends") return await handleTrends(req, res, supabase);
    if (type === "cohorts") return await handleCohorts(req, res, supabase);
    return res.status(400).json({ error: "type required: ?type=trends | ?type=cohorts" });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
