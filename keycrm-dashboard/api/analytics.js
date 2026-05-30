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

// Оборот по категоріях з КОРЕКТНИМ урахуванням знижок на замовлення.
//
// GET /api/analytics?type=category_revenue&from=YYYY-MM-DD&to=YYYY-MM-DD
//                   [&source_id=N]
//
// ПРОБЛЕМА: KeyCRM накладає знижки на ВЕСЬ заказ, а не на позиції.
// "Аналітика по товарах" в KeyCRM має ту ж ваду — завищує оборот.
//
// РІШЕННЯ: знижку на заказ розподіляємо пропорційно вартості позицій.
// Цілочисленна математика на копійках + точний розподіл "залишку":
//   для першого N-1 рядків заказу: cents = round(rev × coef)
//   для останнього: cents = grand_cents − Σ попередніх
// Це гарантує Σ adjusted per order = grand_total ДО КОПІЙКИ.
//
// Safety: якщо grand_total NULL (історичні) або > суми позицій (доставка
// тощо) → coef = 1.0 (не роздуваємо категорію).
async function handleCategoryRevenue(req, res, supabase) {
  const q = req.query || {};
  const now = new Date();
  const defFrom = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1))
    .toISOString().slice(0, 10);
  const from = (q.from && /^\d{4}-\d{2}-\d{2}$/.test(q.from)) ? q.from : defFrom;
  const to = (q.to && /^\d{4}-\d{2}-\d{2}$/.test(q.to)) ? q.to : now.toISOString().slice(0, 10);
  // Фільтр часу — у Київському часовому поясі. KeyCRM в UI рахує по
  // Europe/Kyiv (UTC+3 літом, UTC+2 зимою). Якщо ми пропустимо ТЗ і
  // фільтруємо в UTC, замовлення зроблені у Києві о 00:00-02:59 (= UTC
  // попередній день 21:00-23:59) випадають із "нашого" місяця, тоді як
  // у KeyCRM UI вони включені. Звідси різниця ±2-5 замовлень за місяць.
  //
  // На травень 2026 Україна = UTC+3 (DST). Для зимових місяців offset
  // буде UTC+2 — дрібна неточність, фіксанемо коли стане критично.
  const KYIV_OFFSET = "+03:00";
  const fromIso = from + "T00:00:00" + KYIV_OFFSET;
  const toExclusiveDate = new Date(to + "T00:00:00" + KYIV_OFFSET);
  toExclusiveDate.setUTCDate(toExclusiveDate.getUTCDate() + 1);
  const toExclusiveIso = toExclusiveDate.toISOString();
  const sourceId = q.source_id ? parseInt(q.source_id) : null;

  // 1. Sales за період. line_cost — точна закупка на момент створення
  // замовлення (snapshot з KeyCRM). Якщо null — fallback на skus.cost.
  let buildSales = () => {
    let qq = supabase
      .from("sales")
      .select("order_id, product_id, quantity, revenue, line_cost, order_grand_total, order_status, ordered_at, source_id, source_name")
      .gte("ordered_at", fromIso)
      .lt("ordered_at", toExclusiveIso);
    if (sourceId) qq = qq.eq("source_id", sourceId);
    return qq;
  };
  let sales;
  try {
    sales = await fetchAll(buildSales);
  } catch (err) {
    // Fallback: якщо колонки source_*/line_cost ще не накатано (міграції 041/042),
    // повторюємо запит без них.
    if (/column.*source_id|source_name|line_cost/i.test((err && err.message) || "")) {
      sales = await fetchAll(() =>
        supabase
          .from("sales")
          .select("order_id, product_id, quantity, revenue, order_grand_total, order_status, ordered_at")
          .gte("ordered_at", fromIso)
          .lt("ordered_at", toExclusiveIso)
      );
    } else throw err;
  }

  // 2. Мапа product_id → {category_id, category_name, cost} зі skus.
  // cost — поточна закупка з KeyCRM (синхронізується щотижня через workflow).
  // Це наближення: для історичних замовлень за період, коли закупка була
  // іншою, маржа може бути неточна. Точніше було б зберігати item.purchased_price
  // в sales (TODO якщо потрібно).
  const skuRows = await fetchAll(() =>
    supabase.from("skus").select("product_id, category_id, category_name, cost")
  );
  const catByProduct = new Map();
  for (const s of skuRows) {
    if (s.product_id == null) continue;
    if (catByProduct.has(s.product_id)) continue;
    const rawName = s.category_name && String(s.category_name).trim();
    catByProduct.set(s.product_id, {
      category_id: s.category_id,
      category_name: rawName || "🔍 Без назви категорії",
      cost: s.cost != null ? parseFloat(s.cost) : null,
    });
  }

  // 3. Групуємо рядки по замовленню. Конвертуємо у копійки одразу.
  const toCents = (v) => Math.round((v != null ? parseFloat(v) : 0) * 100);
  const orders = new Map();
  for (const r of sales) {
    if (isExcluded(r.order_status)) continue;
    if (!orders.has(r.order_id)) {
      orders.set(r.order_id, {
        grandCents: r.order_grand_total != null ? toCents(r.order_grand_total) : null,
        lineSumCents: 0,
        lines: [],
      });
    }
    const o = orders.get(r.order_id);
    const revCents = toCents(r.revenue);
    const qty = r.quantity != null ? parseFloat(r.quantity) : 0;
    // Собівартість позиції. Пріоритет:
    //   1) sales.line_cost (snapshot з KeyCRM на момент створення) — ТОЧНО
    //   2) skus.cost (поточна закупка) — НАБЛИЖЕНО, для історичних
    //      замовлень де line_cost не записано
    //   3) null — невідомо, не рахуємо маржу для цього рядка
    const catInfo = catByProduct.get(r.product_id);
    let costCents = null;
    let costSource = null; // 'exact' | 'estimated' | null
    if (r.line_cost != null && qty > 0) {
      costCents = Math.round(parseFloat(r.line_cost) * 100 * qty);
      costSource = "exact";
    } else if (catInfo && catInfo.cost != null && qty > 0) {
      costCents = Math.round(catInfo.cost * 100 * qty);
      costSource = "estimated";
    }
    o.lines.push({ product_id: r.product_id, revCents, costCents, costSource });
    o.lineSumCents += revCents;
  }

  // 4. Розподіляємо знижки по позиціях. Внутрішня математика — копійки-int.
  // Структура категорії: revenueCents (з урахуванням знижок), rawCents
  // (без знижок), costCents (собівартість), ordersSet, costKnownLines
  // (скільки позицій з відомим cost — для прозорості).
  const cats = new Map();
  let totalAdjustedCents = 0;
  let totalRawCents = 0;
  let totalCostCents = 0;
  let totalExactLines = 0;     // позиції з точним line_cost
  let totalEstimatedLines = 0; // позиції з naближенням через skus.cost
  let totalLines = 0;

  for (const [orderId, o] of orders) {
    // Safety: якщо grand або lineSum нема — coef=1.0, не чіпаємо.
    let useGrandCents = null;
    if (o.grandCents != null && o.lineSumCents > 0 && o.grandCents < o.lineSumCents) {
      useGrandCents = o.grandCents;
    }

    // Розподіляємо: перші N-1 рядків через round, останній отримує залишок.
    let allocatedSum = 0;
    for (let i = 0; i < o.lines.length; i++) {
      const ln = o.lines[i];
      let adjustedCents;
      if (useGrandCents == null) {
        adjustedCents = ln.revCents;
      } else if (i < o.lines.length - 1) {
        adjustedCents = Math.round(ln.revCents * useGrandCents / o.lineSumCents);
        allocatedSum += adjustedCents;
      } else {
        // Остання позиція — закриваємо grand рівно (захист від float-помилок)
        adjustedCents = useGrandCents - allocatedSum;
      }
      const cat = catByProduct.get(ln.product_id)
        || { category_id: null, category_name: "🔍 Без назви категорії" };
      const key = cat.category_name;
      if (!cats.has(key)) cats.set(key, {
        revenueCents: 0, rawCents: 0, costCents: 0,
        orders: new Set(), totalLines: 0, exactLines: 0, estimatedLines: 0,
      });
      const c = cats.get(key);
      c.revenueCents += adjustedCents;
      c.rawCents += ln.revCents;
      c.totalLines += 1;
      totalLines += 1;
      if (ln.costSource === "exact") {
        c.costCents += ln.costCents;
        c.exactLines += 1;
        totalCostCents += ln.costCents;
        totalExactLines += 1;
      } else if (ln.costSource === "estimated") {
        c.costCents += ln.costCents;
        c.estimatedLines += 1;
        totalCostCents += ln.costCents;
        totalEstimatedLines += 1;
      }
      c.orders.add(orderId);
      totalAdjustedCents += adjustedCents;
      totalRawCents += ln.revCents;
    }
  }

  const fromCents = (c) => Math.round(c) / 100;

  const categories = Array.from(cats.entries())
    .map(([name, c]) => {
      const margin = c.revenueCents - c.costCents;
      const known = c.exactLines + c.estimatedLines;
      const coverage = c.totalLines > 0
        ? Math.round((known / c.totalLines) * 1000) / 10
        : 0;
      const exactPct = c.totalLines > 0
        ? Math.round((c.exactLines / c.totalLines) * 1000) / 10
        : 0;
      return {
        category: name,
        revenue: fromCents(c.revenueCents),
        raw_revenue: fromCents(c.rawCents),
        cost_exact_pct: exactPct,
        cost: fromCents(c.costCents),
        margin: fromCents(margin),
        margin_pct: c.revenueCents > 0
          ? Math.round((margin / c.revenueCents) * 1000) / 10
          : 0,
        cost_coverage_pct: coverage,
        discount_pct: c.rawCents > 0
          ? Math.round((1 - c.revenueCents / c.rawCents) * 1000) / 10
          : 0,
        orders: c.orders.size,
        share_pct: totalAdjustedCents > 0
          ? Math.round(c.revenueCents / totalAdjustedCents * 1000) / 10
          : 0,
      };
    })
    .sort((a, b) => b.revenue - a.revenue);

  // Список доступних джерел (тільки якщо колонки існують + хоч щось у даних)
  const sourcesMap = new Map();
  for (const r of sales) {
    if (r && r.source_id != null) {
      if (!sourcesMap.has(r.source_id)) {
        sourcesMap.set(r.source_id, r.source_name || ("id:" + r.source_id));
      }
    }
  }
  const sources = Array.from(sourcesMap.entries())
    .map(([id, name]) => ({ id, name }))
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));

  const totalMarginCents = totalAdjustedCents - totalCostCents;
  const totalKnownLines = totalExactLines + totalEstimatedLines;
  const totalCoverage = totalLines > 0
    ? Math.round((totalKnownLines / totalLines) * 1000) / 10
    : 0;
  const totalExactPct = totalLines > 0
    ? Math.round((totalExactLines / totalLines) * 1000) / 10
    : 0;

  return res.status(200).json({
    from,
    to,
    source_id: sourceId,
    total_revenue: fromCents(totalAdjustedCents),
    total_raw_revenue: fromCents(totalRawCents),
    total_discount: fromCents(totalRawCents - totalAdjustedCents),
    total_cost: fromCents(totalCostCents),
    total_margin: fromCents(totalMarginCents),
    total_margin_pct: totalAdjustedCents > 0
      ? Math.round((totalMarginCents / totalAdjustedCents) * 1000) / 10
      : 0,
    cost_coverage_pct: totalCoverage,    // % позицій з будь-яким cost (exact + estimated)
    cost_exact_pct: totalExactPct,       // з них % точно (sales.line_cost з KeyCRM snapshot)
    orders_count: orders.size,
    categories,
    sources,
  });
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
    if (type === "category_revenue") return await handleCategoryRevenue(req, res, supabase);
    return res.status(400).json({ error: "type required: ?type=trends | ?type=cohorts | ?type=category_revenue" });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
