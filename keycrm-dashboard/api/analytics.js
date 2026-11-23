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
//   СКАСОВАНІ (не продажа): Повернули, Повернення, canceled, Відмовились,
//     «Об'єднання замовлень», underbid, «Перенос (дублікат)», incorrect_data
//     (усі в EXCLUDED_STATUSES / isExcluded).
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

async function handleCohorts(req, res, supabase) {
  // Параметр periodMonths: 12 / 24 / 36 / all. Скільки місячних когорт показати.
  const periodParam = (req.query && req.query.months) || "24";
  const periodMonths = periodParam === "all" ? null : parseInt(periodParam);

  // RPC cohort_retention повертає «довгий» формат у розрізі сегмента
  // (segment, cohort, month_after, size, active). Розбиваємо на опт/розницю,
  // заповнюємо нулі й обрізаємо до останніх N когорт у Node.
  const { data, error } = await supabase.rpc("cohort_retention", { _months: periodMonths });
  if (error) return res.status(500).json({ error: error.message });

  const bySeg = { "опт": new Map(), "розниця": new Map() };
  for (const r of (data || [])) {
    const segMap = bySeg[r.segment === "опт" ? "опт" : "розниця"];
    const c = r.cohort;
    if (!segMap.has(c)) segMap.set(c, { size: Number(r.size) || 0, active: new Map() });
    segMap.get(c).active.set(Number(r.month_after), Number(r.active) || 0);
  }

  const nowMonth = new Date().toISOString().slice(0, 7);
  function build(segMap) {
    const cohortKeys = Array.from(segMap.keys()).sort();
    const maxMonths = cohortKeys.length ? monthDiff(cohortKeys[0], nowMonth) : 0;
    let total_buyers = 0;
    for (const c of cohortKeys) total_buyers += segMap.get(c).size;
    const out = cohortKeys.map(cohort => {
      const { size, active } = segMap.get(cohort);
      const maxMonthsForThis = monthDiff(cohort, nowMonth);
      const retention = [];
      for (let i = 0; i <= maxMonthsForThis; i++) {
        const a = active.get(i) || 0;
        retention.push({
          month_after: i,
          active: a,
          pct: size > 0 ? Math.round((a / size) * 1000) / 10 : 0, // 1 decimal
        });
      }
      return { cohort, size, retention };
    });
    const trimmed = periodMonths ? out.slice(-periodMonths) : out;
    return { cohorts: trimmed, total_buyers, max_months_window: maxMonths };
  }

  return res.status(200).json({
    opt: build(bySeg["опт"]),
    retail: build(bySeg["розниця"]),
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

// utm_source → канал. Згруповано за реальними мітками з заказів (див. аналіз
// сирих utm_source/medium). Fallback на «Джерело» (source_name) з KeyCRM.
function normChannel(utmSource, utmMedium, sourceName, utmCampaign) {
  const s = (utmSource || "").toLowerCase().trim();
  const med = (utmMedium || "").toLowerCase().trim();
  const cmp = (utmCampaign || "").toLowerCase().trim();
  const paidMedium = /cpc|ppc|paid/.test(med);        // платний сигнал
  const socialMedium = /^social$|referral/.test(med); // органічний/реферальний

  // Листи за utm_medium (пріоритетно — не залежить від utm_source).
  // Horoshop-тригери розводимо за utm_campaign.
  if (med === "horoshop_trigger") {
    if (/^hs_email_(password|registration|purchase)/.test(cmp)) return "Системні (Технічні)";
    if (/^hs_email_(abandoned_cart|product_avaliable|product_available|review)/.test(cmp)) return "Авто-тригери CMS";
    return "Інші тригери CMS";
  }
  if (med === "email") return "Email-маркетинг";

  // Наш тег Google Ads: utm_campaign=cid_{campaign.id}. Визначальний сигнал,
  // незалежно від source/medium — потрібно для first-touch (де збережена лише
  // кампанія, без medium, тож google-реклама інакше губиться в «органіці»).
  if (/^cid_/.test(cmp)) return "Google Ads";

  if (s) {
    // 1) Прямі (direct + PWA homescreen + порожньо) — разом.
    if (/homescreen|b2c_app|standalone|\bpwa\b/.test(s)) return "Прямі";
    if (s === "(direct)" || /^direct$|^прям/.test(s)) return "Прямі";

    // 2) AI-референси (до google — бо gemini.google.com).
    if (/chatgpt|openai|perplexity|gemini|copilot|claude\.ai/.test(s)) return "AI (ChatGPT/Gemini)";

    // 3) Meta — органіка/реферал: домени Meta (l.instagram.com, m.facebook.com…)
    //    або макрос-джерело з medium social/referral (ig/social = органіка).
    const metaDomain = /facebook\.com|instagram\.com|l\.instagram|l\.facebook|m\.facebook|lm\.facebook|fb\.com/.test(s);
    const metaMacro = /^fb$|^ig$|^meta$|^th$|^an$|^msg$|fb-sitelink|site_source_name|instarosso|^instagram$|^facebook$/.test(s);
    if (metaDomain) return "Meta (органіка/реферал)";
    if (metaMacro && socialMedium) return "Meta (органіка/реферал)";

    // 4) Meta Ads (платна): макрос-джерела з рекламним/плейсмент medium.
    if (metaMacro) return "Meta Ads";

    // 5) Google Ads (платний тег = lama; + google/cpc, pmax/adwords…).
    if (/lama|adwords|\bgads\b|pmax|gclid|google_/.test(s)) return "Google Ads";
    if (s === "google") return paidMedium ? "Google Ads" : "Пошук / органіка";

    // 6) Пошук / органіка (Google organic, сервіси Google, Bing…).
    if (/googlequicksearchbox|accounts\.google|android\.gm|google\.android|google\.com|bing\.|duckduckgo|yandex/.test(s)) return "Пошук / органіка";
    if (/^organic$|^seo$/.test(s)) return "Пошук / органіка";

    // 7) Email / CRM-розсилки (решта email-джерел без medium=email).
    if (/sendpulse|esputnik|^email$|e-mail|^mail$|ukr\.net/.test(s)) return "Email-маркетинг";

    // 8) Месенджери (реферал) — переходи по посиланнях з Viber/Telegram.
    if (/viber|telegram|\btg\b/.test(s)) return "Месенджери (реферал)";

    // 9) Решта (рідкісні).
    if (/\bsms\b/.test(s)) return "SMS";
    if (/tiktok/.test(s)) return "TikTok";
    if (/^prom/.test(s)) return "Prom.ua";
    if (/rtbhouse|criteo|\brtb\b/.test(s)) return "Ретаргетинг (RTB/Criteo)";
    if (/kim|affiliate|partner/.test(s)) return "Партнерки / афіліати";

    return "Інше: " + s;
  }
  // Без utm — fallback на source_name з KeyCRM.
  const n = (sourceName || "").toLowerCase();
  if (!n) return "Прямі";
  if (/viber|telegram/.test(n)) return "Месенджери (реферал)";
  if (/instagram|facebook|meta/.test(n)) return "Meta (органіка/реферал)";
  if (/bijboom|horoshop|сайт|site|opt\.com/.test(n)) return "Прямі";
  if (/prom/.test(n)) return "Prom.ua";
  return sourceName ? ("Джерело: " + sourceName) : "Прямі";
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

// День у Києві (YYYY-MM-DD) — для фільтра по довільному діапазону дат.
const _kyivYMD = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Kyiv", year: "numeric", month: "2-digit", day: "2-digit" });
function ymdKey(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return String(dateStr).slice(0, 10);
  return _kyivYMD.format(d); // "YYYY-MM-DD"
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

  // Довільний діапазон дат (YYYY-MM-DD) — пріоритетний. Якщо не заданий,
  // підтримуємо старий period=YYYY-MM (виводимо межі місяця). period=all → весь час.
  const reDate = /^\d{4}-\d{2}-\d{2}$/;
  let from = (req.query && req.query.from) || null;
  let to = (req.query && req.query.to) || null;
  if (!reDate.test(String(from))) from = null;
  if (!reDate.test(String(to))) to = null;
  if ((!from || !to) && /^\d{4}-\d{2}$/.test(period)) {
    from = period + "-01";
    to = period + "-31"; // порівняння рядків — "-31" покриває будь-який місяць
  }
  const hasRange = !!(from && to);
  const fromM = from ? from.slice(0, 7) : null; // місяць "від" (для ручних витрат)
  const toM = to ? to.slice(0, 7) : null;

  // Уся історія заказів (не 2 роки) — для коректного «новий/повторний».
  const since = new Date("2015-01-01T00:00:00Z");

  // marketing_order_facts(RPC) — дедуп і фільтр (сайт + період) робить Postgres
  // ДО агрегації (по індексах), одним запитом без пагінації. Вью v_order_facts
  // фільтрував після GROUP BY → агрегував усю sales щоразу → statement timeout.
  const [orderRows, buyerRows, ftRows, costRows, metaRows, googleRows] = await Promise.all([
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
    fetchAll(() => supabase.from("first_touch").select("phone_norm, first_source, first_medium, first_campaign")),
    fetchAll(() => supabase.from("channel_costs").select("channel, period, amount")),
    fetchAll(() => supabase.from("meta_spend").select("date, spend, spend_uah, impressions, clicks")),
    // google_spend може ще не існувати (міграція 057) — не валимо сторінку.
    fetchAll(() => supabase.from("google_spend").select("date, spend, spend_uah, impressions, clicks")).catch(() => []),
  ]);

  const typeMap = new Map();
  for (const b of buyerRows) typeMap.set(b.buyer_id, b.client_type || null);
  // first-touch матчимо в Node по нормалізованому телефону — без важкого JOIN
  // через v_buyer_first_touch (normalize_phone по всіх покупцях давав timeout).
  const ftByPhone = new Map();
  for (const f of ftRows) if (f.phone_norm && !ftByPhone.has(f.phone_norm)) ftByPhone.set(f.phone_norm, { source: f.first_source || null, medium: f.first_medium || null, campaign: f.first_campaign || null });
  const ftMap = new Map(); // buyer_id -> { source, medium, campaign }
  for (const b of buyerRows) {
    const pn = normPhone(b.phone);
    if (pn && ftByPhone.has(pn)) ftMap.set(b.buyer_id, ftByPhone.get(pn));
  }
  // Канал першого дотику: по source + campaign (cid_ → Google Ads). Існує лише
  // якщо є source АБО кампанія. Інакше «— немає first-touch —».
  const ftChannel = (ft) => {
    if (!ft || (!ft.source && !ft.campaign)) return null;
    return normChannel(ft.source || "", ft.medium || null, null, ft.campaign || "");
  };

  const orderList = [];
  for (const r of orderRows) {
    if (!r.ordered_at) continue;
    orderList.push({
      order_id: r.order_id,
      buyer_id: r.buyer_id || null,
      ordered_at: r.ordered_at,
      utm_source: r.utm_source || null,
      utm_medium: r.utm_medium || null,
      utm_campaign: r.utm_campaign || null,
      source_name: r.source_name || null,
      revenue: r.revenue != null ? parseFloat(r.revenue) : 0,
      cogs: r.cogs != null ? parseFloat(r.cogs) : null,          // собівартість замовлення
      cogs_known: r.cogs_known === true,                          // усі позиції мають закупку
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
    if (!b) {
      b = { channel: ch, done: 0, prog: 0, canc: 0, revenue: 0, newO: 0, repO: 0, buyers: new Set(),
            // розріз опт/розниця (виручка й активні заказы) — розхід ОДИН на обидва
            rev_opt: 0, rev_retail: 0, act_opt: 0, act_retail: 0, new_opt: 0, new_retail: 0,
            // собівартість лише по замовленнях з повним line_cost (для маржі)
            crev_opt: 0, crev_retail: 0, cogs_opt: 0, cogs_retail: 0 };
      chan.set(ch, b);
    }
    return b;
  }

  const monthsSet = new Set();
  let tDone = 0, tProg = 0, tCanc = 0, tRev = 0, tNew = 0, tRep = 0;
  let tRevOpt = 0, tRevRetail = 0, tActOpt = 0, tActRetail = 0, tNewOpt = 0, tNewRetail = 0;
  let tCrevOpt = 0, tCogsOpt = 0, tCrevRetail = 0, tCogsRetail = 0;
  for (const o of orderList) {
    const m = ymKey(o.ordered_at);
    if (m) monthsSet.add(m);
    if (!inSegment(o.buyer_id)) continue;
    if (hasRange) {
      const dk = ymdKey(o.ordered_at);
      if (!dk || dk < from || dk > to) continue;
    }
    const ch = normChannel(o.utm_source, o.utm_medium, o.source_name, o.utm_campaign);
    const b = bucket(ch);
    if (o.grp === "cancelled") { b.canc++; tCanc++; continue; }
    // активний заказ (виконано АБО в роботі) — рахуємо ВСІ показники
    if (o.grp === "done") { b.done++; tDone++; } else { b.prog++; tProg++; }
    b.revenue += o.revenue; tRev += o.revenue;
    if (o.buyer_id) b.buyers.add(o.buyer_id);
    const isNew = !o.buyer_id ? true : (o.ordered_at === firstActiveAt.get(o.buyer_id));
    if (isNew) { b.newO++; tNew++; } else { b.repO++; tRep++; }
    // Розріз опт/розниця (системні вже відсіяні inSegment). Опт = client_type "опт".
    const isOpt = o.buyer_id && typeMap.get(o.buyer_id) === "опт";
    const hasCogs = o.cogs_known && o.cogs != null;
    if (isOpt) {
      b.rev_opt += o.revenue; b.act_opt++; tRevOpt += o.revenue; tActOpt++;
      if (isNew) { b.new_opt++; tNewOpt++; }
      if (hasCogs) { b.crev_opt += o.revenue; b.cogs_opt += o.cogs; tCrevOpt += o.revenue; tCogsOpt += o.cogs; }
    } else {
      b.rev_retail += o.revenue; b.act_retail++; tRevRetail += o.revenue; tActRetail++;
      if (isNew) { b.new_retail++; tNewRetail++; }
      if (hasCogs) { b.crev_retail += o.revenue; b.cogs_retail += o.cogs; tCrevRetail += o.revenue; tCogsRetail += o.cogs; }
    }
  }

  const costMap = new Map();
  for (const c of costRows) {
    // Ручні витрати зберігаються по місяцях → беремо місяці в межах діапазону.
    if (hasRange && (c.period < fromM || c.period > toM)) continue;
    costMap.set(c.channel, (costMap.get(c.channel) || 0) + (parseFloat(c.amount) || 0));
  }

  // Авто-розхід Meta (meta_spend) за період → каналу «Meta Ads».
  let metaSpend = 0, metaImpr = 0, metaClicks = 0;
  for (const r of (metaRows || [])) {
    if (hasRange && (!r.date || r.date < from || r.date > to)) continue;
    // spend_uah (конвертовано по курсу НБУ на дату розходу); fallback на spend.
    metaSpend += parseFloat(r.spend_uah != null ? r.spend_uah : (r.spend || 0)) || 0;
    metaImpr += parseInt(r.impressions || 0, 10) || 0;
    metaClicks += parseInt(r.clicks || 0, 10) || 0;
  }
  // Авто-розхід Google Ads (google_spend) за період → каналу «Google Ads».
  let googleSpend = 0, googleImpr = 0, googleClicks = 0;
  for (const r of (googleRows || [])) {
    if (hasRange && (!r.date || r.date < from || r.date > to)) continue;
    googleSpend += parseFloat(r.spend_uah != null ? r.spend_uah : (r.spend || 0)) || 0;
    googleImpr += parseInt(r.impressions || 0, 10) || 0;
    googleClicks += parseInt(r.clicks || 0, 10) || 0;
  }
  const META_CH = "Meta Ads";
  const GOOGLE_CH = "Google Ads";
  // Розхід каналу: Meta/Google — авто (meta_spend/google_spend); решта — ручний channel_costs.
  function channelSpend(ch) {
    if (ch === META_CH && metaSpend > 0) return { cost: metaSpend, impr: metaImpr, clicks: metaClicks, auto: true };
    if (ch === GOOGLE_CH && googleSpend > 0) return { cost: googleSpend, impr: googleImpr, clicks: googleClicks, auto: true };
    return { cost: costMap.get(ch) || 0, impr: 0, clicks: 0, auto: false };
  }
  // Маржа/беззбитковий ROAS/прибуток. Маржа — ФАКТИЧНА, лише по замовленнях з
  // повним line_cost (crev/cogs); застосовуємо її до ВСІЄЇ виручки сегмента для
  // оцінки валового прибутку. Беззбитковий ROAS = 1/маржа. Чистий прибуток =
  // валовий(заг) − розхід (розхід один на обидва сегменти).
  function marginBlock(crevO, cogsO, crevR, cogsR, revO, revR, spend) {
    const mOpt = crevO > 0 ? (crevO - cogsO) / crevO : null;
    const mRet = crevR > 0 ? (crevR - cogsR) / crevR : null;
    const crevT = crevO + crevR, cogsT = cogsO + cogsR;
    const mTot = crevT > 0 ? (crevT - cogsT) / crevT : null;
    const be = (m) => (m != null && m > 0) ? +(1 / m).toFixed(2) : null;
    const gpOpt = mOpt != null ? revO * mOpt : null;
    const gpRet = mRet != null ? revR * mRet : null;
    const gpTot = mTot != null ? (revO + revR) * mTot : null;
    return {
      margin: mTot != null ? +(mTot * 100).toFixed(1) : null,
      margin_opt: mOpt != null ? +(mOpt * 100).toFixed(1) : null,
      margin_retail: mRet != null ? +(mRet * 100).toFixed(1) : null,
      breakeven_roas: be(mTot), breakeven_roas_opt: be(mOpt), breakeven_roas_retail: be(mRet),
      gross_profit_opt: gpOpt != null ? Math.round(gpOpt) : null,
      gross_profit_retail: gpRet != null ? Math.round(gpRet) : null,
      net_profit: gpTot != null ? Math.round(gpTot - (spend || 0)) : null,
      cogs_coverage: (revO + revR) > 0 ? Math.round(100 * crevT / (revO + revR)) : null,
    };
  }

  function buildChannel(b, s) {
    const active = b.done + b.prog;
    const revSeg = b.rev_opt + b.rev_retail;
    const pb = marginBlock(b.crev_opt, b.cogs_opt, b.crev_retail, b.cogs_retail, b.rev_opt, b.rev_retail, s.cost);
    return Object.assign(pb, {
      channel: b.channel,
      done: b.done, progress: b.prog, cancelled: b.canc,
      new_orders: b.newO, repeat_orders: b.repO,
      revenue: Math.round(b.revenue), buyers: b.buyers.size,
      aov: active ? Math.round(b.revenue / active) : 0,
      // Розріз опт/розниця: виручка й активні заказы (розхід ОДИН на обидва).
      revenue_opt: Math.round(b.rev_opt), revenue_retail: Math.round(b.rev_retail),
      orders_opt: b.act_opt, orders_retail: b.act_retail,
      opt_share: revSeg > 0 ? Math.round(100 * b.rev_opt / revSeg) : null,
      roas_opt: s.cost > 0 ? +(b.rev_opt / s.cost).toFixed(2) : null,
      roas_retail: s.cost > 0 ? +(b.rev_retail / s.cost).toFixed(2) : null,
      cost: Math.round(s.cost), auto_cost: s.auto,
      impressions: s.impr, clicks: s.clicks,
      cpm: s.impr > 0 ? +(s.cost / s.impr * 1000).toFixed(2) : null,
      ctr: s.impr > 0 ? +(s.clicks / s.impr * 100).toFixed(2) : null,
      roas: s.cost > 0 ? +(b.revenue / s.cost).toFixed(2) : null,
      cps: (s.cost > 0 && active > 0) ? Math.round(s.cost / active) : null, // ціна замовлення
      cac: (s.cost > 0 && b.newO > 0) ? Math.round(s.cost / b.newO) : null,  // ціна нового клієнта
    });
  }

  let totCost = 0;
  const channels = Array.from(chan.values()).map((b) => {
    const s = channelSpend(b.channel);
    totCost += s.cost;
    return buildChannel(b, s);
  }).sort((a, b) => b.revenue - a.revenue);

  // Канали з витратами, але без заказів у періоді (ручні або Meta-авто).
  const spendOnly = new Set(costMap.keys());
  if (metaSpend > 0) spendOnly.add(META_CH);
  if (googleSpend > 0) spendOnly.add(GOOGLE_CH);
  for (const ch of spendOnly) {
    if (chan.has(ch)) continue;
    const s = channelSpend(ch);
    if (s.cost <= 0) continue;
    channels.push({
      channel: ch, done: 0, progress: 0, cancelled: 0, new_orders: 0, repeat_orders: 0,
      revenue: 0, buyers: 0, aov: 0,
      revenue_opt: 0, revenue_retail: 0, orders_opt: 0, orders_retail: 0, opt_share: null,
      roas_opt: null, roas_retail: null,
      cost: Math.round(s.cost), auto_cost: s.auto,
      impressions: s.impr, clicks: s.clicks,
      cpm: s.impr > 0 ? +(s.cost / s.impr * 1000).toFixed(2) : null,
      ctr: s.impr > 0 ? +(s.clicks / s.impr * 100).toFixed(2) : null,
      roas: 0,
    });
    totCost += s.cost;
  }

  // ── Когортний LTV:CAC по МІСЯЦЮ ПРИВЛЕЧЕННЯ (blended, зі 100% даних) ──
  // Правильний спосіб (методички): LTV і CAC — по ОДНІЙ когорті й одному місяці.
  //   CAC(M)  = ВЕСЬ рекламний розхід місяця M ÷ нові клієнти місяця M (blended).
  //   LTV(M)  = валовий прибуток на клієнта когорти M — «на сьогодні» і на
  //             ФІКСОВАНИХ горизонтах 3/6/12 міс від першого заказу (щоб когорти
  //             порівнювались чесно). Горизонт показуємо лише коли когорта
  //             дозріла (вік ≥ горизонту), інакше — «—».

  // Прибуток кожного клієнта на горизонтах 3/6/12 міс від ЙОГО першого заказу.
  const buyerHz = new Map(); // buyerId -> {p3,p6,p12,pt}
  for (const o of orderList) {
    if (o.grp === "cancelled" || !o.buyer_id || !inSegment(o.buyer_id)) continue;
    const first = firstActiveAt.get(o.buyer_id);
    if (!first) continue;
    const gp = o.revenue - (o.cogs != null ? o.cogs : 0);
    const days = (new Date(o.ordered_at).getTime() - new Date(first).getTime()) / 86400000;
    let h = buyerHz.get(o.buyer_id);
    if (!h) { h = { p3: 0, p6: 0, p12: 0, pt: 0 }; buyerHz.set(o.buyer_id, h); }
    h.pt += gp;
    if (days <= 90) h.p3 += gp;
    if (days <= 180) h.p6 += gp;
    if (days <= 365) h.p12 += gp;
  }

  // Розхід реклами по календарних місяцях (Meta + Google + ручні витрати).
  const spendByMonth = new Map();
  const addSpendM = (ym, v) => { if (ym) spendByMonth.set(ym, (spendByMonth.get(ym) || 0) + v); };
  for (const r of (metaRows || [])) addSpendM((r.date || "").slice(0, 7), parseFloat(r.spend_uah != null ? r.spend_uah : (r.spend || 0)) || 0);
  for (const r of (googleRows || [])) addSpendM((r.date || "").slice(0, 7), parseFloat(r.spend_uah != null ? r.spend_uah : (r.spend || 0)) || 0);
  for (const c of costRows) addSpendM(c.period, parseFloat(c.amount) || 0);

  // Когорти: місяць ПЕРШОГО активного заказу клієнта.
  const coh = new Map();
  for (const [buyerId, firstAt] of firstActiveAt.entries()) {
    if (!inSegment(buyerId)) continue;
    const m = ymKey(firstAt);
    if (!m) continue;
    const h = buyerHz.get(buyerId) || { p3: 0, p6: 0, p12: 0, pt: 0 };
    const isOpt = typeMap.get(buyerId) === "опт";
    let a = coh.get(m);
    if (!a) { a = { month: m, n: 0, n_opt: 0, n_retail: 0, pt: 0, pt_opt: 0, pt_retail: 0, p3: 0, p6: 0, p12: 0 }; coh.set(m, a); }
    a.n++; a.pt += h.pt; a.p3 += h.p3; a.p6 += h.p6; a.p12 += h.p12;
    if (isOpt) { a.n_opt++; a.pt_opt += h.pt; } else { a.n_retail++; a.pt_retail += h.pt; }
  }
  const nowYM = ymKey(new Date().toISOString());
  const ymAge = (m) => { const [y1, m1] = m.split("-").map(Number); const [y2, m2] = nowYM.split("-").map(Number); return (y2 * 12 + m2) - (y1 * 12 + m1); };
  const cohorts = Array.from(coh.values()).sort((a, b) => b.month.localeCompare(a.month)).slice(0, 36).map((a) => {
    const spend = spendByMonth.get(a.month) || 0;
    const cac = (spend > 0 && a.n > 0) ? Math.round(spend / a.n) : null;
    const ltv = a.n ? Math.round(a.pt / a.n) : 0;
    const age = ymAge(a.month);
    const monthlyProfit = age > 0 ? (a.pt / a.n) / age : null; // груба оцінка прибутку/міс на клієнта
    const hz = (sum, minAge) => (age >= minAge && a.n > 0) ? Math.round(sum / a.n) : null;
    return {
      month: a.month, age_months: age,
      new_customers: a.n, new_opt: a.n_opt, new_retail: a.n_retail,
      spend: Math.round(spend), cac,
      ltv, ltv_opt: a.n_opt ? Math.round(a.pt_opt / a.n_opt) : 0, ltv_retail: a.n_retail ? Math.round(a.pt_retail / a.n_retail) : 0,
      ltv_3mo: hz(a.p3, 3), ltv_6mo: hz(a.p6, 6), ltv_12mo: hz(a.p12, 12),
      ltv_cac: (cac && cac > 0) ? +(ltv / cac).toFixed(1) : null,
      payback_months: (cac && monthlyProfit && monthlyProfit > 0) ? +(cac / monthlyProfit).toFixed(1) : null,
    };
  });

  const months = Array.from(monthsSet).sort().reverse();
  return res.status(200).json({
    period, segment, months, channels,
    from: hasRange ? from : null, to: hasRange ? to : null,
    totals: Object.assign(
      marginBlock(tCrevOpt, tCogsOpt, tCrevRetail, tCogsRetail, tRevOpt, tRevRetail, totCost),
      {
        done: tDone, progress: tProg, cancelled: tCanc,
        revenue: Math.round(tRev), new_orders: tNew, repeat_orders: tRep,
        revenue_opt: Math.round(tRevOpt), revenue_retail: Math.round(tRevRetail),
        orders_opt: tActOpt, orders_retail: tActRetail,
        opt_share: (tRevOpt + tRevRetail) > 0 ? Math.round(100 * tRevOpt / (tRevOpt + tRevRetail)) : null,
        new_opt: tNewOpt, new_retail: tNewRetail,
        cost: Math.round(totCost), roas: totCost > 0 ? +(tRev / totCost).toFixed(2) : null,
        roas_opt: totCost > 0 ? +(tRevOpt / totCost).toFixed(2) : null,
        roas_retail: totCost > 0 ? +(tRevRetail / totCost).toFixed(2) : null,
        cps: (totCost > 0 && (tDone + tProg) > 0) ? Math.round(totCost / (tDone + tProg)) : null,
        cac: (totCost > 0 && tNew > 0) ? Math.round(totCost / tNew) : null,
      }),
    cohorts,
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

// ── Звіт «Реклама»: ROAS/CPS по кампаніях (Meta+Google) і оголошеннях (Meta) ──
// Виручка береться лише з заказів із новою розміткою: Meta utm_source=meta
// (utm_campaign=назва, utm_content=оголошення); Google utm_source=google +
// utm_campaign=cid_<id>_... Розхід — з meta_ad_spend (кампанія+оголошення) і
// google_spend (кампанія). Сходиться на рівні кампанії.
function _decodeRaw(v) {
  if (v == null) return "";
  let s = String(v);
  try { s = decodeURIComponent(s.replace(/\+/g, " ")); } catch (e) { s = s.replace(/\+/g, " "); }
  return s.replace(/\s+/g, " ").trim();
}
function _normName(v) { return _decodeRaw(v).toLowerCase(); }
function _gCid(camp) { const m = /cid_(\d+)/i.exec(camp || ""); return m ? m[1] : null; }

async function handleCampaigns(req, res, supabase) {
  const segment = (req.query && req.query.segment) || "all";
  const reDate = /^\d{4}-\d{2}-\d{2}$/;
  let from = (req.query && req.query.from) || null;
  let to = (req.query && req.query.to) || null;
  if (!reDate.test(String(from))) from = null;
  if (!reDate.test(String(to))) to = null;
  const hasRange = !!(from && to);

  const since = new Date("2015-01-01T00:00:00Z");
  const [orderRows, buyerRows, metaAdRows, googleRows] = await Promise.all([
    fetchAll(() => supabase.rpc("marketing_order_facts", { _since: since.toISOString(), _source: "bijboom-opt.com.ua" }).order("order_id", { ascending: true })),
    fetchAll(() => supabase.from("buyers").select("buyer_id, client_type")),
    fetchAll(() => supabase.from("meta_ad_spend").select("date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, spend_uah, spend, impressions, clicks")).catch(() => []),
    fetchAll(() => supabase.from("google_spend").select("date, campaign_id, campaign_name, spend_uah, spend, impressions, clicks")).catch(() => []),
  ]);

  const typeMap = new Map();
  for (const b of buyerRows) typeMap.set(b.buyer_id, b.client_type || null);
  function inSegment(buyerId) {
    const t = buyerId ? typeMap.get(buyerId) : null;
    if (t === "системні") return false;
    if (segment === "opt") return t === "опт";
    if (segment === "retail") return t !== "опт";
    return true;
  }

  // Перший активний (не скасований) заказ клієнта — для «нових».
  const orders = [];
  const firstActiveAt = new Map();
  let metaFrom = null, googleFrom = null;
  for (const r of orderRows) {
    if (!r.ordered_at) continue;
    const o = {
      buyer_id: r.buyer_id || null,
      ordered_at: r.ordered_at,
      src: (r.utm_source || "").toLowerCase().trim(),
      medium: r.utm_medium || null,
      campaign: r.utm_campaign || null,
      content: r.utm_content || null,
      source_name: r.source_name || null,
      revenue: r.revenue != null ? parseFloat(r.revenue) : 0,
      cogs: r.cogs != null ? parseFloat(r.cogs) : null,
      cogs_known: r.cogs_known === true,
      grp: statusGroup(r.order_status),
    };
    orders.push(o);
    if (o.grp !== "cancelled" && o.buyer_id) {
      const cur = firstActiveAt.get(o.buyer_id);
      if (!cur || o.ordered_at < cur) firstActiveAt.set(o.buyer_id, o.ordered_at);
    }
    const dk = ymdKey(o.ordered_at);
    if (o.src === "meta" && dk) { if (!metaFrom || dk < metaFrom) metaFrom = dk; }
    if (o.src === "google" && /^cid_/.test((o.campaign || "").toLowerCase()) && dk) { if (!googleFrom || dk < googleFrom) googleFrom = dk; }
  }

  const cObj = () => ({ done: 0, prog: 0, canc: 0, rev: 0, newO: 0 });
  function applyRev(acc, o, isNew) {
    if (o.grp === "cancelled") { acc.canc++; return; }
    if (o.grp === "done") acc.done++; else acc.prog++;
    acc.rev += o.revenue;
    if (isNew) acc.newO++;
  }

  // Бекбон = реальні кампанії Meta зі spend (meta_ad_spend): дерево
  // кампанія→група→оголошення з реальними назвами/ID. Виручку з заказів
  // чіпляємо сюди по іменах; нерозпізнане (напр. нерозкритий {{campaign.name}})
  // не створює фейкових кампаній — воно просто не знаходить збіг і відкидається.
  const SEP = "\u0001";
  const mTree = new Map();       // campId -> campNode
  const campLookup = new Map();  // normCampName -> campNode
  const adLookup = new Map();    // normCampName+SEP+normAdName -> [adNode] (fallback по імені)
  const adById = new Map();      // ad_id -> adNode (точний матч, коли utm_content={{ad.id}})
  const rz = () => ({ rev: 0, done: 0, prog: 0, canc: 0, newO: 0, rev_opt: 0, rev_retail: 0, act_opt: 0, act_retail: 0,
    crev_opt: 0, cogs_opt: 0, crev_retail: 0, cogs_retail: 0 });
  for (const r of (metaAdRows || [])) {
    if (hasRange && (!r.date || r.date < from || r.date > to)) continue;
    const sp = parseFloat(r.spend_uah != null ? r.spend_uah : (r.spend || 0)) || 0;
    const im = parseInt(r.impressions || 0, 10) || 0, cl = parseInt(r.clicks || 0, 10) || 0;
    const campId = String(r.campaign_id || "unknown");
    let cn = mTree.get(campId);
    if (!cn) { cn = Object.assign({ id: campId, name: r.campaign_name || "(кампанія)", spend: 0, impr: 0, clicks: 0, adsets: new Map() }, rz()); mTree.set(campId, cn); }
    cn.spend += sp; cn.impr += im; cn.clicks += cl;
    campLookup.set(_normName(r.campaign_name), cn);
    const asId = String(r.adset_id || "unknown");
    let an = cn.adsets.get(asId);
    if (!an) { an = Object.assign({ id: asId, name: r.adset_name || "(група)", spend: 0, impr: 0, clicks: 0, ads: new Map(), _camp: cn }, rz()); cn.adsets.set(asId, an); }
    an.spend += sp; an.impr += im; an.clicks += cl;
    const adId = String(r.ad_id || "unknown");
    let dn = an.ads.get(adId);
    if (!dn) { dn = Object.assign({ id: adId, name: r.ad_name || "(оголошення)", spend: 0, impr: 0, clicks: 0, _adset: an, _camp: cn }, rz()); an.ads.set(adId, dn); }
    dn.spend += sp; dn.impr += im; dn.clicks += cl;
    adById.set(adId, dn); // точний матч по ad.id
    // Одне ім'я оголошення може бути в кількох групах → зберігаємо СПИСОК вузлів.
    const akey = _normName(r.campaign_name) + SEP + _normName(r.ad_name);
    let arr = adLookup.get(akey); if (!arr) { arr = []; adLookup.set(akey, arr); }
    if (arr.indexOf(dn) === -1) arr.push(dn);
  }
  const gCampSpend = new Map();
  for (const r of (googleRows || [])) {
    if (hasRange && (!r.date || r.date < from || r.date > to)) continue;
    const cid = String(r.campaign_id || "");
    let c = gCampSpend.get(cid);
    if (!c) { c = Object.assign({ spend: 0, impr: 0, clicks: 0, disp: r.campaign_name || null }, rz()); gCampSpend.set(cid, c); }
    const sp = parseFloat(r.spend_uah != null ? r.spend_uah : (r.spend || 0)) || 0;
    c.spend += sp; c.impr += parseInt(r.impressions || 0, 10) || 0; c.clicks += parseInt(r.clicks || 0, 10) || 0;
    if (!c.disp && r.campaign_name) c.disp = r.campaign_name;
  }

  // Виручка з заказів → у дерево по іменах (ad.name, інакше campaign.name).
  const isOptOrder = (o) => !!(o.buyer_id && typeMap.get(o.buyer_id) === "опт");
  function addRev(node, o, isNew) {
    if (!node) return;
    if (o.grp === "cancelled") { node.canc++; return; }
    if (o.grp === "done") node.done++; else node.prog++;
    node.rev += o.revenue;
    if (isNew) node.newO++;
    const hc = o.cogs_known && o.cogs != null;
    if (isOptOrder(o)) { node.rev_opt += o.revenue; node.act_opt++; if (hc) { node.crev_opt += o.revenue; node.cogs_opt += o.cogs; } }
    else { node.rev_retail += o.revenue; node.act_retail++; if (hc) { node.crev_retail += o.revenue; node.cogs_retail += o.cogs; } }
  }
  // Хвіст: виручка каналу, яку не прив'язати до кампанії (стара розмітка lama,
  // google без cid, старі fb/ig). Щоб сума по каналу сходилась із «загальним».
  const mOther = rz(), gOther = rz();
  for (const o of orders) {
    if (hasRange) { const dk = ymdKey(o.ordered_at); if (!dk || dk < from || dk > to) continue; }
    if (!inSegment(o.buyer_id)) continue;
    const isNew = !o.buyer_id ? true : (o.ordered_at === firstActiveAt.get(o.buyer_id));
    const ch = normChannel(o.src, o.medium, o.source_name, o.campaign);
    if (ch === "Meta Ads") {
      const ck = _normName(o.campaign);
      // 1) Точний матч по ad.id (utm_content={{ad.id}}) — однозначна група/оголошення.
      const byId = o.content ? adById.get(String(o.content).trim()) : null;
      if (byId) { addRev(byId, o, isNew); addRev(byId._adset, o, isNew); addRev(byId._camp, o, isNew); continue; }
      // 2) Інакше — старий матч по імені оголошення (може бути в кількох групах).
      const arr = adLookup.get(ck + SEP + _normName(o.content));
      if (arr && arr.length) {
        // Кампанія — повний заказ (один раз). Виручку по групах/оголошеннях
        // розподіляємо пропорційно розходу (заказ не несе групу); лічильники
        // замовлень — цілими у групу з найбільшим розходом цього оголошення.
        addRev(arr[0]._camp, o, isNew);
        let top = arr[0]; for (const n of arr) if (n.spend > top.spend) top = n;
        const total = arr.reduce((s, n) => s + n.spend, 0);
        const oOpt = isOptOrder(o);
        for (const n of arr) {
          const share = total > 0 ? n.spend / total : 1 / arr.length;
          if (o.grp !== "cancelled") {
            const rPart = o.revenue * share;
            const cPart = (o.cogs_known && o.cogs != null) ? o.cogs * share : null;
            n.rev += rPart; n._adset.rev += rPart;
            if (oOpt) {
              n.rev_opt += rPart; n._adset.rev_opt += rPart;
              if (cPart != null) { n.crev_opt += rPart; n._adset.crev_opt += rPart; n.cogs_opt += cPart; n._adset.cogs_opt += cPart; }
            } else {
              n.rev_retail += rPart; n._adset.rev_retail += rPart;
              if (cPart != null) { n.crev_retail += rPart; n._adset.crev_retail += rPart; n.cogs_retail += cPart; n._adset.cogs_retail += cPart; }
            }
          }
          if (n === top) {
            if (o.grp === "cancelled") { n.canc++; n._adset.canc++; }
            else {
              if (o.grp === "done") { n.done++; n._adset.done++; } else { n.prog++; n._adset.prog++; }
              if (isNew) { n.newO++; n._adset.newO++; }
              if (oOpt) { n.act_opt++; n._adset.act_opt++; } else { n.act_retail++; n._adset.act_retail++; }
            }
          }
        }
      } else {
        const cn = campLookup.get(ck);
        if (cn) addRev(cn, o, isNew); else addRev(mOther, o, isNew); // кампанія не зіставилась
      }
    } else if (ch === "Google Ads") {
      const cid = _gCid(o.campaign);
      if (cid) {
        let c = gCampSpend.get(cid);
        if (!c) { c = Object.assign({ spend: 0, impr: 0, clicks: 0, disp: null }, rz()); gCampSpend.set(cid, c); }
        addRev(c, o, isNew);
      } else { addRev(gOther, o, isNew); } // google без cid / стара lama
    }
  }

  const metric = (spend, impr, clicks, active, rev, newO) => ({
    spend: Math.round(spend), impressions: impr, clicks,
    cpm: impr > 0 ? +(spend / impr * 1000).toFixed(2) : null,
    ctr: impr > 0 ? +(clicks / impr * 100).toFixed(2) : null,
    roas: spend > 0 ? +(rev / spend).toFixed(2) : null,
    cps: (spend > 0 && active > 0) ? Math.round(spend / active) : null,
    cac: (spend > 0 && newO > 0) ? Math.round(spend / newO) : null,
  });

  // Маржа/беззбитковий ROAS/прибуток (як у «Маркетинг»): маржа ФАКТИЧНА, лише
  // по замовленнях з повним line_cost (crev/cogs); застосовуємо до всієї
  // виручки сегмента. Чистий прибуток = валовий(заг) − розхід.
  function marginBlock(crevO, cogsO, crevR, cogsR, revO, revR, spend) {
    const mOpt = crevO > 0 ? (crevO - cogsO) / crevO : null;
    const mRet = crevR > 0 ? (crevR - cogsR) / crevR : null;
    const crevT = crevO + crevR, cogsT = cogsO + cogsR;
    const mTot = crevT > 0 ? (crevT - cogsT) / crevT : null;
    const be = (m) => (m != null && m > 0) ? +(1 / m).toFixed(2) : null;
    const gpOpt = mOpt != null ? revO * mOpt : null, gpRet = mRet != null ? revR * mRet : null;
    const gpTot = mTot != null ? (revO + revR) * mTot : null;
    return {
      margin: mTot != null ? +(mTot * 100).toFixed(1) : null,
      margin_opt: mOpt != null ? +(mOpt * 100).toFixed(1) : null,
      margin_retail: mRet != null ? +(mRet * 100).toFixed(1) : null,
      breakeven_roas: be(mTot), breakeven_roas_opt: be(mOpt), breakeven_roas_retail: be(mRet),
      gross_profit_opt: gpOpt != null ? Math.round(gpOpt) : null,
      gross_profit_retail: gpRet != null ? Math.round(gpRet) : null,
      net_profit: gpTot != null ? Math.round(gpTot - (spend || 0)) : null,
      cogs_coverage: (revO + revR) > 0 ? Math.round(100 * crevT / (revO + revR)) : null,
      _crevO: crevO, _cogsO: cogsO, _crevR: crevR, _cogsR: cogsR, // сирі — для підсумку каналу
    };
  }

  // Показуємо лише активні (spend>0) або ті, що принесли конверсії (active>0).
  const keep = (spend, active) => spend > 0 || active > 0;
  const bySpend = (a, b) => b.spend - a.spend || b.revenue - a.revenue;
  const node2row = (n, extra) => {
    const ro = Math.round(n.rev_opt || 0), rr = Math.round(n.rev_retail || 0);
    const mb = marginBlock(n.crev_opt || 0, n.cogs_opt || 0, n.crev_retail || 0, n.cogs_retail || 0, n.rev_opt || 0, n.rev_retail || 0, n.spend || 0);
    return Object.assign(mb, {
      name: n.name, done: n.done, progress: n.prog, cancelled: n.canc,
      orders: n.done + n.prog, revenue: Math.round(n.rev), new_orders: n.newO,
      revenue_opt: ro, revenue_retail: rr,
      orders_opt: n.act_opt || 0, orders_retail: n.act_retail || 0,
      opt_share: (ro + rr) > 0 ? Math.round(100 * ro / (ro + rr)) : null,
      roas_opt: n.spend > 0 ? +(n.rev_opt / n.spend).toFixed(2) : null,
      roas_retail: n.spend > 0 ? +(n.rev_retail / n.spend).toFixed(2) : null,
    }, extra || {}, metric(n.spend, n.impr, n.clicks, n.done + n.prog, n.rev, n.newO));
  };

  const metaCampaigns = [];
  for (const cn of mTree.values()) {
    if (!keep(cn.spend, cn.done + cn.prog)) continue;
    const adsets = [];
    for (const an of cn.adsets.values()) {
      if (!keep(an.spend, an.done + an.prog)) continue;
      const ads = [];
      for (const dn of an.ads.values()) {
        if (!keep(dn.spend, dn.done + dn.prog)) continue;
        ads.push(node2row(dn, { ad_id: dn.id }));
      }
      ads.sort(bySpend);
      adsets.push(node2row(an, { adset_id: an.id, ads }));
    }
    adsets.sort(bySpend);
    metaCampaigns.push(node2row(cn, { campaign_id: cn.id, adsets }));
  }
  metaCampaigns.sort(bySpend);

  const gCampaigns = [];
  for (const [cid, c] of gCampSpend) {
    if (!keep(c.spend, c.done + c.prog)) continue;
    gCampaigns.push(node2row(Object.assign({ name: c.disp || ("cid_" + cid) }, c), { campaign_id: cid, adsets: null }));
  }
  gCampaigns.sort(bySpend);

  // Рядок-«хвіст» (виручка каналу без прив'язки до кампанії) — внизу списку.
  const otherRow = (o, adsets) => {
    if ((o.done + o.prog + o.canc) <= 0) return null;
    return node2row(Object.assign({ name: "(без кампанії / стара розмітка)", spend: 0, impr: 0, clicks: 0 }, o), { campaign_id: null, adsets, is_other: true });
  };
  const mo = otherRow(mOther, null); if (mo) metaCampaigns.push(mo);
  const go = otherRow(gOther, null); if (go) gCampaigns.push(go);

  function channelTotals(camps) {
    let spend = 0, impr = 0, clicks = 0, done = 0, prog = 0, canc = 0, rev = 0, nw = 0, rOpt = 0, rRet = 0, oOpt = 0, oRet = 0;
    let cO = 0, kO = 0, cR = 0, kR = 0; // сирі crev/cogs опт/розниця
    for (const c of camps) { spend += c.spend; impr += c.impressions; clicks += c.clicks; done += c.done; prog += c.progress; canc += c.cancelled; rev += c.revenue; nw += c.new_orders; rOpt += c.revenue_opt || 0; rRet += c.revenue_retail || 0; oOpt += c.orders_opt || 0; oRet += c.orders_retail || 0; cO += c._crevO || 0; kO += c._cogsO || 0; cR += c._crevR || 0; kR += c._cogsR || 0; }
    const active = done + prog;
    return Object.assign(
      marginBlock(cO, kO, cR, kR, rOpt, rRet, spend),
      {
        done, progress: prog, cancelled: canc, orders: active, revenue: rev, new_orders: nw,
        revenue_opt: rOpt, revenue_retail: rRet, orders_opt: oOpt, orders_retail: oRet,
        opt_share: (rOpt + rRet) > 0 ? Math.round(100 * rOpt / (rOpt + rRet)) : null,
        roas_opt: spend > 0 ? +(rOpt / spend).toFixed(2) : null,
        roas_retail: spend > 0 ? +(rRet / spend).toFixed(2) : null,
      }, metric(spend, impr, clicks, active, rev, nw));
  }

  const channels = [
    { channel: "Meta Ads", from: metaFrom, has_ads: true, totals: channelTotals(metaCampaigns), campaigns: metaCampaigns },
    { channel: "Google Ads", from: googleFrom, has_ads: false, totals: channelTotals(gCampaigns), campaigns: gCampaigns },
  ];

  return res.status(200).json({
    from: hasRange ? from : null, to: hasRange ? to : null, segment,
    meta_from: metaFrom, google_from: googleFrom,
    channels, totals: channelTotals([].concat(metaCampaigns, gCampaigns)),
  });
}

// Чесна свіжість даних по ВСІХ джерелах: коли кожне востаннє реально
// синхронізувалось. Дашборд показує найстаріше → «актуально» тільки якщо
// актуальне ВСЕ (а не лише заказы).
async function handleFreshness(req, res, supabase) {
  const sources = {};
  async function latest(tbl, col, missingOk) {
    try {
      const r = await supabase.from(tbl).select(col).order(col, { ascending: false }).limit(1);
      if (r.error) throw new Error(r.error.message);
      return r.data && r.data[0] ? r.data[0][col] : null;
    } catch (e) {
      return missingOk ? "__missing__" : null;
    }
  }
  // Заказы: останній успішний авто-прогін ingest.
  try {
    const r = await supabase.from("ingest_runs").select("finished_at")
      .eq("kind", "auto").not("finished_at", "is", null)
      .order("finished_at", { ascending: false }).limit(1);
    sources.orders = { synced_at: r.data && r.data[0] ? r.data[0].finished_at : null };
  } catch (e) { sources.orders = { synced_at: null }; }
  // Рекламний розхід: коли оновлювали (updated_at) і за яку останню дату (date).
  for (const [key, tbl] of [["meta_spend", "meta_spend"], ["meta_ad_spend", "meta_ad_spend"], ["google_spend", "google_spend"]]) {
    const u = await latest(tbl, "updated_at", true);
    if (u === "__missing__") { sources[key] = { missing: true }; continue; }
    const d = await latest(tbl, "date", true);
    sources[key] = { synced_at: u, last_date: d === "__missing__" ? null : d };
  }
  // Matview (sku_metrics / buyer_rfm) + стан циклу з ingest_state.
  try {
    const s = await supabase.from("ingest_state")
      .select("cycle_date,status,current_step,last_chunk_at,sku_metrics_at,buyer_rfm_at")
      .eq("id", 1).maybeSingle();
    const st = s.data || {};
    sources.cycle = { cycle_date: st.cycle_date, status: st.status, current_step: st.current_step, last_chunk_at: st.last_chunk_at };
    sources.sku_metrics = { synced_at: st.sku_metrics_at || null };
    sources.buyer_rfm = { synced_at: st.buyer_rfm_at || null };
  } catch (e) { /* no-op */ }
  return res.status(200).json({ now: new Date().toISOString(), sources });
}

// ── GA4: верх воронки і трафік ПО КАНАЛАХ ───────────────────────────────
// Дані GA4 — клієнтська аналітика, НЕПОВНІ (блокувальники, згода на cookie,
// cross-device) → абсолютні числа занижені й НЕ зводяться з KeyCRM. Беремо
// лише для ВІДНОСНОГО порівняння каналів і пошуку провалів воронки.
//
// GET /api/analytics?type=ga4&from=YYYY-MM-DD&to=YYYY-MM-DD
//
// Доступ: Google OAuth (той самий client, що й Google Ads) зі скоупом
//   analytics.readonly. ENV: GA4_PROPERTY_ID + GA4_REFRESH_TOKEN
//   (фолбек GOOGLE_ADS_REFRESH_TOKEN — якщо той токен переавторизували з
//   додатковим скоупом analytics.readonly).

async function fetchGa4(url, opts, ms) {
  ms = ms || 15000;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    return await fetch(url, Object.assign({}, opts, { signal: ctrl.signal }));
  } finally { clearTimeout(t); }
}

async function ga4AccessToken() {
  const id = process.env.GOOGLE_ADS_CLIENT_ID;
  const secret = process.env.GOOGLE_ADS_CLIENT_SECRET;
  const refresh = process.env.GA4_REFRESH_TOKEN || process.env.GOOGLE_ADS_REFRESH_TOKEN;
  if (!id || !secret || !refresh) return null;
  const body = new URLSearchParams({ client_id: id, client_secret: secret, refresh_token: refresh, grant_type: "refresh_token" });
  const r = await fetchGa4("https://oauth2.googleapis.com/token", {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body,
  });
  const j = await r.json();
  if (j && j.error) throw new Error("Google OAuth: " + (j.error_description || j.error));
  return j.access_token || null;
}

async function ga4RunReport(propertyId, token, reqBody) {
  const r = await fetchGa4(
    "https://analyticsdata.googleapis.com/v1beta/properties/" + propertyId + ":runReport",
    { method: "POST", headers: { Authorization: "Bearer " + token, "Content-Type": "application/json" }, body: JSON.stringify(reqBody) }
  );
  const j = await r.json();
  if (j && j.error) {
    const m = j.error.message || JSON.stringify(j.error);
    throw new Error("GA4 Data API: " + String(m).slice(0, 300));
  }
  return j;
}

// Людські назви каналів GA4 (default channel grouping).
const GA4_CH_LABEL = {
  "Direct": "Прямі",
  "Organic Search": "Пошук (органіка)",
  "Paid Search": "Google Ads (пошук)",
  "Paid Social": "Соцмережі (реклама)",
  "Organic Social": "Соцмережі (органіка)",
  "Paid Shopping": "Google Shopping",
  "Organic Shopping": "Shopping (органіка)",
  "Display": "Display (банери)",
  "Email": "Email",
  "Referral": "Реферали",
  "Affiliates": "Партнери",
  "Audio": "Аудіо",
  "Video": "Відео",
  "Paid Video": "Відео (реклама)",
  "Organic Video": "Відео (органіка)",
  "SMS": "SMS",
  "Mobile Push Notifications": "Push",
  "Unassigned": "Не визначено",
};

function ga4YMD(d) { return d.toISOString().slice(0, 10); }

async function handleGa4(req, res) {
  const propertyId = String(process.env.GA4_PROPERTY_ID || "").replace(/\D/g, "");
  if (!propertyId) {
    return res.status(200).json({ configured: false, reason: "GA4_PROPERTY_ID не налаштовано" });
  }
  let token;
  try {
    token = await ga4AccessToken();
  } catch (e) {
    return res.status(200).json({ configured: false, reason: (e && e.message) || String(e) });
  }
  if (!token) {
    return res.status(200).json({ configured: false, reason: "Google OAuth не налаштовано (CLIENT_ID/SECRET/GA4_REFRESH_TOKEN)" });
  }

  // Діапазон дат: from/to або останні 28 днів до вчора.
  const reDate = /^\d{4}-\d{2}-\d{2}$/;
  let from = (req.query && req.query.from) || null;
  let to = (req.query && req.query.to) || null;
  if (!reDate.test(String(from))) from = null;
  if (!reDate.test(String(to))) to = null;
  if (!from || !to) {
    const y = new Date(Date.now() - 24 * 3600 * 1000);      // вчора
    const s = new Date(Date.now() - 28 * 24 * 3600 * 1000); // -28 днів
    from = ga4YMD(s); to = ga4YMD(y);
  }

  const metrics = ["sessions", "totalUsers", "newUsers", "addToCarts", "checkouts", "ecommercePurchases", "purchaseRevenue"];
  const body = {
    dateRanges: [{ startDate: from, endDate: to }],
    dimensions: [{ name: "sessionDefaultChannelGroup" }],
    metrics: metrics.map((m) => ({ name: m })),
    metricAggregations: ["TOTAL"],
    orderBys: [{ desc: true, metric: { metricName: "sessions" } }],
    limit: 250,
  };

  let j;
  try {
    j = await ga4RunReport(propertyId, token, body);
  } catch (e) {
    return res.status(200).json({ configured: true, ok: false, error: (e && e.message) || String(e), from, to });
  }

  const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
  const mapRow = (metricValues, channelRaw) => {
    const mv = {};
    metrics.forEach((m, i) => { mv[m] = num(metricValues && metricValues[i] && metricValues[i].value); });
    const sessions = mv.sessions, carts = mv.addToCarts, checkouts = mv.checkouts, purchases = mv.ecommercePurchases;
    const pct = (a, b) => (b > 0 ? +(100 * a / b).toFixed(2) : 0);
    return {
      channel: channelRaw,
      label: GA4_CH_LABEL[channelRaw] || channelRaw || "(не визначено)",
      sessions, users: mv.totalUsers, newUsers: mv.newUsers,
      carts, checkouts, purchases, revenue: mv.purchaseRevenue,
      cr_cart: pct(carts, sessions),         // сеанс → в кошик (увага: addToCarts — ПОДІЇ, може бути >100%)
      cr_checkout: pct(checkouts, sessions), // сеанс → почав оформлення
      cr_purchase: pct(purchases, sessions), // сеанс → покупка (головна конверсія)
      cr_checkout_purchase: pct(purchases, checkouts), // оформлення → оплата (доводять до кінця)
    };
  };

  const channels = (j.rows || []).map((row) =>
    mapRow(row.metricValues, row.dimensionValues && row.dimensionValues[0] && row.dimensionValues[0].value)
  );
  const totalsRow = j.totals && j.totals[0] ? mapRow(j.totals[0].metricValues, "__total__") : null;

  // ── Другий звіт: середня сума кошика на кроках «оформлення» vs «покупка».
  // Перевірка гіпотези про мінімалку 500 грн: якщо середній кошик на
  // ПОЧАТКУ оформлення помітно менший за середній чек ПОКУПКИ (і близько до
  // 500) — поріг відсікає дрібні кошики на останньому кроці. eventValue =
  // сума параметра value події (begin_checkout/purchase несуть суму кошика).
  let values = null;
  try {
    const vb = {
      dateRanges: [{ startDate: from, endDate: to }],
      dimensions: [{ name: "sessionDefaultChannelGroup" }, { name: "eventName" }],
      metrics: [{ name: "eventCount" }, { name: "eventValue" }],
      dimensionFilter: { filter: { fieldName: "eventName", inListFilter: { values: ["begin_checkout", "purchase"] } } },
      limit: 500,
    };
    const jv = await ga4RunReport(propertyId, token, vb);
    const byCh = {};
    let tBC = { c: 0, v: 0 }, tP = { c: 0, v: 0 };
    for (const row of (jv.rows || [])) {
      const chRaw = row.dimensionValues[0] && row.dimensionValues[0].value;
      const ev = row.dimensionValues[1] && row.dimensionValues[1].value;
      const cnt = num(row.metricValues[0] && row.metricValues[0].value);
      const val = num(row.metricValues[1] && row.metricValues[1].value);
      const label = GA4_CH_LABEL[chRaw] || chRaw || "(не визначено)";
      if (!byCh[label]) byCh[label] = { label, bcCount: 0, bcVal: 0, pCount: 0, pVal: 0 };
      if (ev === "begin_checkout") { byCh[label].bcCount += cnt; byCh[label].bcVal += val; tBC.c += cnt; tBC.v += val; }
      else if (ev === "purchase") { byCh[label].pCount += cnt; byCh[label].pVal += val; tP.c += cnt; tP.v += val; }
    }
    const avg = (v, c) => (c > 0 ? Math.round(v / c) : null);
    const list = Object.values(byCh)
      .map((x) => ({ label: x.label, checkoutAvg: avg(x.bcVal, x.bcCount), purchaseAvg: avg(x.pVal, x.pCount), checkouts: x.bcCount, purchases: x.pCount }))
      .filter((x) => x.checkouts > 0 || x.purchases > 0)
      .sort((a, b) => (b.checkouts || 0) - (a.checkouts || 0));
    values = {
      total: { checkoutAvg: avg(tBC.v, tBC.c), purchaseAvg: avg(tP.v, tP.c), checkouts: tBC.c, purchases: tP.c },
      byChannel: list,
    };
  } catch (e) {
    values = { error: (e && e.message) || String(e) };
  }

  return res.status(200).json({ configured: true, ok: true, from, to, totals: totalsRow, channels, values });
}

// ── Вкладка «Сегменти» (опт/розниця): здоров'я, удержання, LTV ──────────
async function handleSegments(req, res, supabase) {
  const months = Math.max(6, Math.min(36, parseInt((req.query && req.query.months) || "24")));
  const [m, r] = await Promise.all([
    supabase.rpc("segment_monthly", { _months: months }),
    supabase.rpc("segment_retention", { _min_age_days: 90 }),
  ]);
  if (m.error) throw new Error("segment_monthly: " + m.error.message);
  if (r.error) throw new Error("segment_retention: " + r.error.message);
  const byMonth = new Map();
  for (const row of (m.data || [])) {
    let o = byMonth.get(row.month);
    if (!o) { o = { month: row.month, opt: null, retail: null }; byMonth.set(row.month, o); }
    const seg = {
      revenue: Math.round(Number(row.revenue) || 0),
      gross_profit: row.gross_profit != null ? Math.round(Number(row.gross_profit)) : null,
      active: Number(row.active_buyers) || 0,
      orders: Number(row.orders) || 0,
      new_buyers: Number(row.new_buyers) || 0,
      repeat_buyers: Number(row.repeat_buyers) || 0,
    };
    if (row.segment === "опт") o.opt = seg; else o.retail = seg;
  }
  const monthsRows = Array.from(byMonth.values()).sort((a, b) => b.month.localeCompare(a.month));
  const retention = {};
  for (const row of (r.data || [])) {
    retention[row.segment === "опт" ? "opt" : "retail"] = {
      buyers: Number(row.buyers) || 0,
      returned: Number(row.returned) || 0,
      repeat_pct: row.repeat_pct != null ? Number(row.repeat_pct) : null,
      avg_orders: row.avg_orders != null ? Number(row.avg_orders) : null,
      avg_profit: row.avg_profit != null ? Math.round(Number(row.avg_profit)) : null,
    };
  }
  return res.status(200).json({ months: monthsRows, retention });
}

// type=acquisition: first-touch залучення по каналах × сегмент (опт/розниця).
// RPC повертає по рядку на покупця (сегмент + utm першого заказу), канал
// класифікуємо normChannel (як у «Маркетинг»), агрегуємо тут.
async function handleAcquisition(req, res, supabase) {
  // ?since=YYYY-MM-DD — вікно по даті залучення (перший заказ); без нього — вся історія.
  const since = (req.query && req.query.since) || null;
  const { data, error } = await supabase.rpc("acquisition_first_touch", { _since: since });
  if (error) return res.status(500).json({ error: error.message });

  const byChannel = new Map(); // channel → { opt, retail }
  let totOpt = 0, totRetail = 0;
  for (const r of (data || [])) {
    const ch = normChannel(r.utm_source, r.utm_medium, r.source_name, r.utm_campaign);
    let c = byChannel.get(ch);
    if (!c) { c = { opt: 0, retail: 0 }; byChannel.set(ch, c); }
    if (r.segment === "опт") { c.opt++; totOpt++; } else { c.retail++; totRetail++; }
  }
  const channels = Array.from(byChannel.entries())
    .map(([channel, c]) => {
      const total = c.opt + c.retail;
      return {
        channel,
        opt: c.opt,
        retail: c.retail,
        total,
        opt_pct: total > 0 ? Math.round((c.opt / total) * 1000) / 10 : 0,
      };
    })
    .sort((a, b) => b.total - a.total);

  return res.status(200).json({
    channels,
    total_buyers: totOpt + totRetail,
    total_opt: totOpt,
    total_retail: totRetail,
    since: since || null,
  });
}

// type=opt_quality: якість залучення опту по місяцях (нові оптовики + ранній
// повтор 1/3 міс). «Датчик» — чи не псується привлечення при масштабуванні.
async function handleOptQuality(req, res, supabase) {
  const months = Math.max(6, Math.min(36, parseInt((req.query && req.query.months) || "24")));
  const { data, error } = await supabase.rpc("opt_acquisition_quality", { _months: months });
  if (error) return res.status(500).json({ error: error.message });

  const rows = (data || []).map(r => ({
    cohort: r.cohort,
    new_buyers: Number(r.new_buyers) || 0,
    repeat_1m_pct: r.repeat_1m_pct != null ? Number(r.repeat_1m_pct) : 0,
    repeat_3m_pct: r.repeat_3m_pct != null ? Number(r.repeat_3m_pct) : 0,
  }));

  // Зведення: середнє за останні 6 когорт vs попередні 6; порог 45%.
  const r3 = rows.map(r => r.repeat_3m_pct);
  const avg = a => a.length ? Math.round(a.reduce((s, x) => s + x, 0) / a.length * 10) / 10 : 0;
  const last6 = avg(r3.slice(-6));
  const prev6 = avg(r3.slice(-12, -6));
  const avgNew = rows.length ? Math.round(rows.reduce((s, r) => s + r.new_buyers, 0) / rows.length) : 0;

  return res.status(200).json({
    rows, months,
    threshold: 45,
    last6_repeat3m: last6,
    prev6_repeat3m: prev6,
    avg_new_per_month: avgNew,
  });
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
    if (type === "cohorts") return await handleCohorts(req, res, supabase);
    if (type === "campaigns") return await handleCampaigns(req, res, supabase);
    if (type === "freshness") return await handleFreshness(req, res, supabase);
    if (type === "ga4") return await handleGa4(req, res);
    if (type === "segments") return await handleSegments(req, res, supabase);
    if (type === "acquisition") return await handleAcquisition(req, res, supabase);
    if (type === "opt_quality") return await handleOptQuality(req, res, supabase);
    return res.status(400).json({ error: "type required: ?type=cohorts | ?type=marketing | ?type=campaigns | ?type=freshness | ?type=ga4 | ?type=segments | ?type=acquisition | ?type=opt_quality" });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
