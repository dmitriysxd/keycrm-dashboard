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
  "Перенос (дублікат)", // дубль незабраної посилки, скопійований у новий місяць
]);

function isExcluded(orderStatus) {
  if (!orderStatus) return false;
  if (EXCLUDED_STATUSES.has(orderStatus)) return true;
  // LIKE 'Об%єднання замовлень'
  if (typeof orderStatus === "string" && orderStatus.indexOf("Об") === 0 && orderStatus.indexOf("єднання замовлень") !== -1) return true;
  return false;
}

// Статус-групи для маркетингу (варіант 3): done / progress / cancelled.
// ПІДТВЕРДЖЕНА КАРТА СТАТУСІВ (узгоджено з власником, 2026-11):
//   ВИКОНАНІ (продажа): completed
//   В РОБОТІ (ще не продажа): in_transit, delivered («відправлено, ще не
//     забрано»), new, «Можна збирати», delivered_to_delivery, «Очікує оплату»,
//     «Зв'язатись повторно», + будь-який НОВИЙ/невідомий статус (безпечний
//     дефолт — не роздуває продажі).
//   СКАСОВАНІ (не продажа): Повернули, canceled, Відмовились,
//     «Об'єднання замовлень», underbid, «Перенос (дублікат)», incorrect_data,
//     Повернення (усі вони в EXCLUDED_STATUSES / isExcluded).
// Нові success-статуси додавати СЮДИ (а не в progress) свідомо.
const DONE_STATUSES = new Set(["completed", "виконано", "виконан"]);
function statusGroup(s) {
  if (isExcluded(s)) return "cancelled";
  if (s && DONE_STATUSES.has(s)) return "done";
  return "progress";
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


// ══════════════════════════════════════════════════════════
// type=marketing: канали / сегменти / Нові-Повторні / ROAS / first-touch LTV
// (окремим файлом був api/marketing.js, але Hobby Vercel має ліміт 12
//  serverless-функцій, тож складено сюди як ще один type.)
// ══════════════════════════════════════════════════════════

// utm_source → канал. Fallback на «Джерело» (source_name) з KeyCRM.
function normChannel(utmSource, utmMedium, sourceName) {
  const s = (utmSource || "").toLowerCase().trim();
  const med = (utmMedium || "").toLowerCase().trim();
  const paidMedium = /cpc|ppc|paid/.test(med); // платний сигнал
  if (s) {
    // Застосунок (PWA) / прямі
    if (/homescreen|b2c_app|standalone|\bpwa\b/.test(s)) return "Застосунок / прямі";
    if (s === "(direct)" || /^direct$|^прям/.test(s)) return "Прямі";
    // Голий "google": medium=cpc → платний Google Ads; інакше — органіка/реферал.
    // (Платний контекст тегується як lama, але частина платних кліків втрачає
    //  тег і прилітає як google — розводимо за medium, а не на око.)
    if (s === "google") return paidMedium ? "Google Ads" : "Google (органіка/реферал)";
    // Пошук / органіка / реферали (НЕ реклама) — перевіряємо ДО google-ads,
    // щоб google-пошук/застосунки не потрапили в платний Google Ads.
    if (/googlequicksearchbox|accounts\.google|gemini\.google|google\.com|android\.gm|google\.android|bing\.|duckduckgo|yandex/.test(s)) return "Пошук / органіка";
    if (/^organic$|^seo$/.test(s)) return "Пошук / органіка";
    // Google Ads (платний тег = lama за вашим тегуванням; + pmax/adwords/…)
    if (/lama|adwords|\bgads\b|pmax|gclid|google_/.test(s)) return "Google Ads";
    // Meta Ads
    if (/facebook|instagram|instarosso|\bfb\b|\big\b|^meta$|site_source_name/.test(s)) return "Meta Ads";
    // Email / CRM-розсилки
    if (/sendpulse|esputnik|^email$|e-mail|^mail$|ukr\.net/.test(s)) return "Email (розсилка)";
    if (/\bsms\b/.test(s)) return "SMS";
    if (/viber/.test(s)) return "Viber";
    if (/telegram|\btg\b/.test(s)) return "Telegram";
    if (/tiktok/.test(s)) return "TikTok";
    if (/^prom/.test(s)) return "Prom.ua";
    if (/rtbhouse|criteo|\brtb\b/.test(s)) return "Ретаргетинг (RTB/Criteo)";
    if (/kim|affiliate|partner/.test(s)) return "Партнерки / афіліати";
    if (/chatgpt|openai|perplexity|gemini/.test(s)) return "AI-референси";
    return "Інше: " + s;
  }
  const n = (sourceName || "").toLowerCase();
  if (!n) return "Прямі / не визначено";
  if (/viber/.test(n)) return "Viber";
  if (/telegram/.test(n)) return "Telegram";
  if (/instagram|facebook|meta/.test(n)) return "Meta (Джерело)";
  if (/bijboom|horoshop|сайт|site|opt\.com/.test(n)) return "Сайт / прямі";
  if (/prom/.test(n)) return "Prom.ua";
  return sourceName ? ("Джерело: " + sourceName) : "Прямі / не визначено";
}

// Місяць заказу рахуємо в КИЇВСЬКОМУ часовому поясі (як KeyCRM «Дата
// оформлення»), а не UTC — інакше заказы біля опівночі на межі місяця
// потрапляють не в той місяць (серпень +1 / вересень −1 проти KeyCRM).
const _kyivYM = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Kyiv", year: "numeric", month: "2-digit" });
function ymKey(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return String(dateStr).slice(0, 7);
  let y = "", m = "";
  for (const p of _kyivYM.formatToParts(d)) {
    if (p.type === "year") y = p.value;
    else if (p.type === "month") m = p.value;
  }
  return y + "-" + m;
}

// Дзеркалить SQL normalize_phone: канонічний UA-формат '380XXXXXXXXX'.
function normPhone(raw) {
  const d = (raw || "").replace(/\D/g, "");
  if (d.length < 9) return null;
  if (d.length === 12 && d.slice(0, 3) === "380") return d;
  if (d.length === 11 && d.slice(0, 2) === "80") return "3" + d;
  if (d.length === 10 && d[0] === "0") return "38" + d;
  if (d.length === 9) return "380" + d;
  return "380" + d.slice(-9);
}

async function handleMarketing(req, res, supabase) {
  const period = (req.query && req.query.period) || "all";
  const segment = (req.query && req.query.segment) || "all";

  // Уся історія заказів (не 2 роки) — для коректного «новий/повторний».
  const since = new Date("2015-01-01T00:00:00Z");

  // marketing_order_facts(RPC) — дедуп і фільтр (сайт + період) робить Postgres
  // ДО агрегації (по індексах), одним запитом без пагінації. Вью v_order_facts
  // фільтрував після GROUP BY → агрегував усю sales щоразу → statement timeout.
  const [orderRows, buyerRows, ftRows, costRows] = await Promise.all([
    // Пагінуємо RPC (як fetchAll) — інакше PostgREST обрізає відповідь по
    // db-max-rows і найсвіжіші заказы випадають (місяці обривались на серпні).
    // .order(order_id) — щоб сторінки .range були стабільні.
    fetchAll(() => supabase
      .rpc("marketing_order_facts", {
        _since: since.toISOString(),
        _source: "bijboom-opt.com.ua", // лише замовлення з сайту (Horoshop)
      })
      .order("order_id", { ascending: true })),
    fetchAll(() => supabase.from("buyers").select("buyer_id, client_type, phone")),
    fetchAll(() => supabase.from("first_touch").select("phone_norm, first_source")),
    fetchAll(() => supabase.from("channel_costs").select("channel, period, amount")),
  ]);

  const typeMap = new Map();
  for (const b of buyerRows) typeMap.set(b.buyer_id, b.client_type || null);
  // first-touch матчимо в Node по нормалізованому телефону — без важкого JOIN
  // через v_buyer_first_touch (normalize_phone по всіх покупцях давав timeout).
  const ftByPhone = new Map();
  for (const f of ftRows) if (f.phone_norm && !ftByPhone.has(f.phone_norm)) ftByPhone.set(f.phone_norm, f.first_source || null);
  const ftMap = new Map();
  for (const b of buyerRows) {
    const pn = normPhone(b.phone);
    if (pn && ftByPhone.has(pn)) ftMap.set(b.buyer_id, ftByPhone.get(pn));
  }

  const orderList = [];
  for (const r of orderRows) {
    if (!r.ordered_at) continue;
    orderList.push({
      order_id: r.order_id,
      buyer_id: r.buyer_id || null,
      ordered_at: r.ordered_at,
      utm_source: r.utm_source || null,
      utm_medium: r.utm_medium || null,
      source_name: r.source_name || null,
      revenue: r.revenue != null ? parseFloat(r.revenue) : 0,
      grp: statusGroup(r.order_status),
    });
  }

  // «Новий» клієнт = перший заказ клієнта серед НЕ-скасованих (будь-який
  // статус, крім скасування), по ВСІЙ історії.
  const firstActiveAt = new Map();
  for (const o of orderList) {
    if (o.grp === "cancelled" || !o.buyer_id) continue;
    const cur = firstActiveAt.get(o.buyer_id);
    if (!cur || o.ordered_at < cur) firstActiveAt.set(o.buyer_id, o.ordered_at);
  }

  function inSegment(buyerId) {
    const t = buyerId ? typeMap.get(buyerId) : null;
    if (t === "системні") return false;
    if (segment === "opt") return t === "опт";
    if (segment === "retail") return t !== "опт";
    return true;
  }

  const chan = new Map();
  function bucket(ch) {
    let b = chan.get(ch);
    if (!b) { b = { channel: ch, done: 0, prog: 0, canc: 0, revenue: 0, newO: 0, repO: 0, buyers: new Set() }; chan.set(ch, b); }
    return b;
  }

  const monthsSet = new Set();
  let tDone = 0, tProg = 0, tCanc = 0, tRev = 0, tNew = 0, tRep = 0;
  for (const o of orderList) {
    const m = ymKey(o.ordered_at);
    if (m) monthsSet.add(m);
    if (!inSegment(o.buyer_id)) continue;
    if (period !== "all" && m !== period) continue;
    const ch = normChannel(o.utm_source, o.utm_medium, o.source_name);
    const b = bucket(ch);
    if (o.grp === "cancelled") { b.canc++; tCanc++; continue; }
    // активний заказ (виконано АБО в роботі) — рахуємо ВСІ показники
    if (o.grp === "done") { b.done++; tDone++; } else { b.prog++; tProg++; }
    b.revenue += o.revenue; tRev += o.revenue;
    if (o.buyer_id) b.buyers.add(o.buyer_id);
    const isNew = !o.buyer_id ? true : (o.ordered_at === firstActiveAt.get(o.buyer_id));
    if (isNew) { b.newO++; tNew++; } else { b.repO++; tRep++; }
  }

  const costMap = new Map();
  for (const c of costRows) {
    if (period !== "all" && c.period !== period) continue;
    costMap.set(c.channel, (costMap.get(c.channel) || 0) + (parseFloat(c.amount) || 0));
  }
  let totCost = 0;
  const channels = Array.from(chan.values()).map((b) => {
    const cost = costMap.get(b.channel) || 0;
    totCost += cost;
    return {
      channel: b.channel,
      done: b.done, progress: b.prog, cancelled: b.canc,
      new_orders: b.newO, repeat_orders: b.repO,
      revenue: Math.round(b.revenue), buyers: b.buyers.size,
      aov: (b.done + b.prog) ? Math.round(b.revenue / (b.done + b.prog)) : 0,
      cost: Math.round(cost), roas: cost > 0 ? +(b.revenue / cost).toFixed(2) : null,
    };
  }).sort((a, b) => b.revenue - a.revenue);
  for (const [ch, amount] of costMap.entries()) {
    if (!chan.has(ch) && amount > 0) {
      channels.push({ channel: ch, done: 0, progress: 0, cancelled: 0, new_orders: 0, repeat_orders: 0,
        revenue: 0, buyers: 0, aov: 0, cost: Math.round(amount), roas: 0 });
      totCost += amount;
    }
  }

  // LTV first-touch — по НЕ-скасованих заказах (виконані + в роботі).
  const ltvByBuyer = new Map();
  for (const o of orderList) {
    if (o.grp === "cancelled" || !o.buyer_id || !inSegment(o.buyer_id)) continue;
    ltvByBuyer.set(o.buyer_id, (ltvByBuyer.get(o.buyer_id) || 0) + o.revenue);
  }
  const ftAgg = new Map();
  let ftMatched = 0;
  for (const [buyerId, ltv] of ltvByBuyer.entries()) {
    const src = ftMap.get(buyerId);
    const ch = src ? normChannel(src, null, null) : "— немає first-touch —";
    if (src) ftMatched++;
    let a = ftAgg.get(ch);
    if (!a) { a = { channel: ch, buyers: 0, ltv: 0 }; ftAgg.set(ch, a); }
    a.buyers++; a.ltv += ltv;
  }
  const firstTouch = Array.from(ftAgg.values()).map((a) => ({
    channel: a.channel, buyers: a.buyers,
    ltv_total: Math.round(a.ltv), ltv_avg: a.buyers ? Math.round(a.ltv / a.buyers) : 0,
  })).sort((a, b) => b.ltv_total - a.ltv_total);

  const months = Array.from(monthsSet).sort().reverse();
  return res.status(200).json({
    period, segment, months, channels,
    totals: {
      done: tDone, progress: tProg, cancelled: tCanc,
      revenue: Math.round(tRev), new_orders: tNew, repeat_orders: tRep,
      cost: Math.round(totCost), roas: totCost > 0 ? +(tRev / totCost).toFixed(2) : null,
    },
    first_touch: firstTouch,
    first_touch_coverage: { matched_buyers: ftMatched, total_buyers: ltvByBuyer.size },
  });
}

async function handleMarketingPost(req, res, supabase) {
  let body = req.body;
  if (typeof body === "string") { try { body = JSON.parse(body); } catch (e) { body = {}; } }
  body = body || {};
  const action = body.action;
  if (action === "set_cost" || action === "delete_cost") {
    const channel = (body.channel || "").toString().trim().slice(0, 120);
    const period = (body.period || "").toString().trim();
    if (!channel) return res.status(400).json({ error: "channel обовʼязковий" });
    if (!/^\d{4}-\d{2}$/.test(period)) return res.status(400).json({ error: "period має бути 'YYYY-MM'" });
    if (action === "delete_cost") {
      const { error } = await supabase.from("channel_costs").delete().eq("channel", channel).eq("period", period);
      if (error) return res.status(500).json({ error: error.message });
      return res.status(200).json({ ok: true });
    }
    const amount = parseFloat(body.amount);
    if (isNaN(amount) || amount < 0) return res.status(400).json({ error: "amount має бути числом ≥ 0" });
    const { error } = await supabase.from("channel_costs")
      .upsert({ channel, period, amount, updated_at: new Date().toISOString() }, { onConflict: "channel,period" });
    if (error) return res.status(500).json({ error: error.message });
    return res.status(200).json({ ok: true });
  }
  return res.status(400).json({ error: "невідомий action" });
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();
  const type = (req.query && req.query.type) || "";

  try {
    if (type === "marketing") {
      if (req.method === "POST") return await handleMarketingPost(req, res, supabase);
      return await handleMarketing(req, res, supabase);
    }
    if (type === "trends") return await handleTrends(req, res, supabase);
    if (type === "cohorts") return await handleCohorts(req, res, supabase);
    return res.status(400).json({ error: "type required: ?type=trends | ?type=cohorts | ?type=marketing" });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
