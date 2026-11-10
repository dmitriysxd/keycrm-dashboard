// Маркетингова аналітика: канали, сегменти, привлечення/удержання, ROAS, LTV.
//
// GET /api/marketing?period=YYYY-MM|all&segment=opt|retail|all
//   → канали (last-touch) з розбивкою Нові/Повторні, витрати і ROAS,
//     + first-touch LTV по каналах привлечення, + список доступних місяців.
//
// POST /api/marketing   { action:'set_cost', channel, period:'YYYY-MM', amount }
//                       { action:'delete_cost', channel, period }
//   → ручний ввід витрат по каналу за місяць (для ROAS).
//
// Сегменти: опт / роздріб (роздріб + порожнє) / усі. «Системні» завжди виключені.
// Канал last-touch = utm_source (нормалізований), fallback на «Джерело» KeyCRM.

const { getSupabase } = require("../lib/supabase");
const { checkDashboardToken } = require("../lib/auth");

async function fetchAll(buildQuery, pageSize = 1000, hardCap = 500000) {
  const out = [];
  for (let from = 0; from < hardCap; from += pageSize) {
    const { data, error } = await buildQuery().range(from, from + pageSize - 1);
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
  "Перенос (дублікат)",
]);
function isExcluded(s) {
  if (!s) return false;
  if (EXCLUDED_STATUSES.has(s)) return true;
  if (typeof s === "string" && s.indexOf("Об") === 0 && s.indexOf("єднання замовлень") !== -1) return true;
  return false;
}

// utm_source → канал. Fallback на «Джерело» (source_name) з KeyCRM.
function normChannel(utmSource, sourceName) {
  const s = (utmSource || "").toLowerCase().trim();
  if (s) {
    if (/lama|google|gads|adwords|gclid/.test(s)) return "Google Ads";
    if (/instagram|insta|facebook|\bfb\b|meta|\big\b/.test(s)) return "Meta Ads";
    if (/sendpulse|email|e-mail|mail/.test(s)) return "Email (розсилка)";
    if (/\bsms\b/.test(s)) return "SMS";
    if (/viber/.test(s)) return "Viber";
    if (/telegram|\btg\b/.test(s)) return "Telegram";
    if (/tiktok/.test(s)) return "TikTok";
    if (/direct|прям/.test(s)) return "Прямі / не визначено";
    return "utm: " + s; // невідома мітка — показуємо як є, щоб не губити
  }
  const n = (sourceName || "").toLowerCase();
  if (!n) return "Прямі / не визначено";
  if (/viber/.test(n)) return "Viber";
  if (/telegram/.test(n)) return "Telegram";
  if (/instagram|facebook|meta/.test(n)) return "Meta (Джерело)";
  if (/bijboom|horoshop|сайт|site|opt\.com/.test(n)) return "Сайт / прямі";
  return "Прямі / не визначено";
}

function ym(dateStr) {
  // 'YYYY-MM' з ISO-дати
  if (!dateStr) return null;
  return String(dateStr).slice(0, 7);
}

async function handleGet(req, res, supabase) {
  const period = (req.query && req.query.period) || "all";
  const segment = (req.query && req.query.segment) || "all";

  // 2 роки історії (як у analytics) — для коректного Нові/Повторні потрібна історія.
  const since = new Date();
  since.setUTCFullYear(since.getUTCFullYear() - 2);

  const [salesRows, buyerRows, ftRows, costRows] = await Promise.all([
    fetchAll(() => supabase
      .from("sales")
      .select("order_id, buyer_id, ordered_at, order_status, revenue, order_grand_total, source_name, utm_source, utm_medium, utm_campaign")
      .gte("ordered_at", since.toISOString())),
    fetchAll(() => supabase.from("buyers").select("buyer_id, client_type")),
    fetchAll(() => supabase.from("v_buyer_first_touch").select("buyer_id, first_source")),
    fetchAll(() => supabase.from("channel_costs").select("channel, period, amount")),
  ]);

  const typeMap = new Map();          // buyer_id → client_type
  for (const b of buyerRows) typeMap.set(b.buyer_id, b.client_type || null);
  const ftMap = new Map();            // buyer_id → first_source
  for (const f of ftRows) if (!ftMap.has(f.buyer_id)) ftMap.set(f.buyer_id, f.first_source || null);

  // Дедуплікуємо sales по order_id (revenue — на замовлення).
  const orders = new Map();
  for (const r of salesRows) {
    if (isExcluded(r.order_status)) continue;
    if (!r.ordered_at) continue;
    let o = orders.get(r.order_id);
    if (!o) {
      o = {
        order_id: r.order_id,
        buyer_id: r.buyer_id || null,
        ordered_at: r.ordered_at,
        utm_source: r.utm_source || null,
        source_name: r.source_name || null,
        grand: r.order_grand_total != null ? parseFloat(r.order_grand_total) : null,
        line_rev: 0,
      };
      orders.set(r.order_id, o);
    }
    if (o.grand == null) o.line_rev += r.revenue != null ? parseFloat(r.revenue) : 0;
    // якщо раптом utm в одному рядку є, а в першому не було — підхопимо
    if (!o.utm_source && r.utm_source) o.utm_source = r.utm_source;
    if (!o.source_name && r.source_name) o.source_name = r.source_name;
  }
  const orderList = Array.from(orders.values()).map((o) => ({
    ...o,
    revenue: o.grand != null ? o.grand : o.line_rev,
  }));

  // Перша покупка кожного покупця (по всій історії вікна) — для Нові/Повторні.
  const firstOrderAt = new Map();
  for (const o of orderList) {
    if (!o.buyer_id) continue;
    const cur = firstOrderAt.get(o.buyer_id);
    if (!cur || o.ordered_at < cur) firstOrderAt.set(o.buyer_id, o.ordered_at);
  }

  // Фільтр сегмента по покупцю.
  function inSegment(buyerId) {
    const t = buyerId ? typeMap.get(buyerId) : null;
    if (t === "системні") return false;                 // завжди геть
    if (segment === "opt") return t === "опт";
    if (segment === "retail") return t !== "опт";        // роздріб + порожнє + анонім
    return true;                                         // all (крім системних)
  }

  // ── Агрегація каналів у вибраному періоді ──
  const chan = new Map(); // channel → {orders, revenue, newO, repO, newRev, repRev, buyers:Set}
  function bucket(ch) {
    let b = chan.get(ch);
    if (!b) { b = { channel: ch, orders: 0, revenue: 0, newO: 0, repO: 0, newRev: 0, repRev: 0, buyers: new Set() }; chan.set(ch, b); }
    return b;
  }

  const monthsSet = new Set();
  let totOrders = 0, totRev = 0, totNew = 0, totRep = 0, totNewRev = 0, totRepRev = 0;

  for (const o of orderList) {
    const m = ym(o.ordered_at);
    if (m) monthsSet.add(m);
    if (!inSegment(o.buyer_id)) continue;
    if (period !== "all" && m !== period) continue;

    const ch = normChannel(o.utm_source, o.source_name);
    const b = bucket(ch);
    const isNew = !o.buyer_id ? true : (o.ordered_at === firstOrderAt.get(o.buyer_id));
    b.orders++; b.revenue += o.revenue;
    if (o.buyer_id) b.buyers.add(o.buyer_id);
    totOrders++; totRev += o.revenue;
    if (isNew) { b.newO++; b.newRev += o.revenue; totNew++; totNewRev += o.revenue; }
    else       { b.repO++; b.repRev += o.revenue; totRep++; totRepRev += o.revenue; }
  }

  // Витрати: period='YYYY-MM' → цей місяць; 'all' → сума всіх по каналу.
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
      orders: b.orders,
      revenue: Math.round(b.revenue),
      new_orders: b.newO,
      repeat_orders: b.repO,
      new_revenue: Math.round(b.newRev),
      repeat_revenue: Math.round(b.repRev),
      buyers: b.buyers.size,
      aov: b.orders ? Math.round(b.revenue / b.orders) : 0,
      cost: Math.round(cost),
      roas: cost > 0 ? +(b.revenue / cost).toFixed(2) : null,
    };
  }).sort((a, b) => b.revenue - a.revenue);

  // Канали, що мають витрати, але без заказів у періоді — теж показати (ROAS=0).
  for (const [ch, amount] of costMap.entries()) {
    if (!chan.has(ch) && amount > 0) {
      channels.push({ channel: ch, orders: 0, revenue: 0, new_orders: 0, repeat_orders: 0,
        new_revenue: 0, repeat_revenue: 0, buyers: 0, aov: 0, cost: Math.round(amount), roas: 0 });
      totCost += amount;
    }
  }

  // ── First-touch LTV по каналах привлечення (лайфтайм, у межах сегмента) ──
  const ltvByBuyer = new Map();
  for (const o of orderList) {
    if (!o.buyer_id || !inSegment(o.buyer_id)) continue;
    ltvByBuyer.set(o.buyer_id, (ltvByBuyer.get(o.buyer_id) || 0) + o.revenue);
  }
  const ftAgg = new Map();
  let ftMatched = 0;
  for (const [buyerId, ltv] of ltvByBuyer.entries()) {
    const src = ftMap.get(buyerId);
    const ch = src ? normChannel(src, null) : "— немає first-touch —";
    if (src) ftMatched++;
    let a = ftAgg.get(ch);
    if (!a) { a = { channel: ch, buyers: 0, ltv: 0 }; ftAgg.set(ch, a); }
    a.buyers++; a.ltv += ltv;
  }
  const firstTouch = Array.from(ftAgg.values()).map((a) => ({
    channel: a.channel,
    buyers: a.buyers,
    ltv_total: Math.round(a.ltv),
    ltv_avg: a.buyers ? Math.round(a.ltv / a.buyers) : 0,
  })).sort((a, b) => b.ltv_total - a.ltv_total);

  const months = Array.from(monthsSet).sort().reverse();

  return res.status(200).json({
    period, segment,
    months,
    channels,
    totals: {
      orders: totOrders, revenue: Math.round(totRev),
      new_orders: totNew, repeat_orders: totRep,
      new_revenue: Math.round(totNewRev), repeat_revenue: Math.round(totRepRev),
      cost: Math.round(totCost),
      roas: totCost > 0 ? +(totRev / totCost).toFixed(2) : null,
    },
    first_touch: firstTouch,
    first_touch_coverage: { matched_buyers: ftMatched, total_buyers: ltvByBuyer.size },
  });
}

async function handlePost(req, res, supabase) {
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

module.exports = async (req, res) => {
  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });
  try {
    const supabase = getSupabase();
    if (req.method === "POST") return await handlePost(req, res, supabase);
    return await handleGet(req, res, supabase);
  } catch (e) {
    return res.status(500).json({ error: String((e && e.message) || e) });
  }
};
