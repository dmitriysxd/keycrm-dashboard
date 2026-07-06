// Карточка одного клиента.
//
// GET    /api/client?buyer_id=...    → профиль + RFM + 20 последних заказов + лента заметок
// PATCH  /api/client                 → body: { buyer_id, status_id?, is_wholesale? }
//
// Цель: один запрос даёт модалу всё, что нужно (профиль, история, заметки).

const { getSupabase } = require("../lib/supabase");
const { checkDashboardToken } = require("../lib/auth");
const { enrichRfmRow } = require("../lib/clients");

const EXCLUDED_STATUSES = new Set([
  "cancelled", "rejected", "canceled",
  "Повернули", "Повернення", "Відмовились",
  "incorrect_data", "underbid",
  "Перенос (дублікат)", // дубль незабраної посилки, скопійований у новий місяць
]);
function isMergerStatus(s) {
  return !!s && /^Об.{1,2}єднання замовлень$/.test(s);
}

async function readBody(req) {
  if (req.body && typeof req.body === "object") return req.body;
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (c) => { raw += c; });
    req.on("end", () => {
      if (!raw) return resolve({});
      try { resolve(JSON.parse(raw)); } catch (e) { reject(new Error("invalid JSON body")); }
    });
    req.on("error", reject);
  });
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,PATCH,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();

  try {
    if (req.method === "PATCH") {
      const body = await readBody(req);
      const buyerId = parseInt(body.buyer_id);
      if (!buyerId) return res.status(400).json({ error: "buyer_id required" });
      const patch = {};
      if (body.status_id === null || body.status_id === "") patch.status_id = null;
      else if (body.status_id != null) patch.status_id = parseInt(body.status_id);
      if (typeof body.is_wholesale === "boolean") patch.is_wholesale = body.is_wholesale;
      if (!Object.keys(patch).length) return res.status(400).json({ error: "nothing to update" });

      const { error } = await supabase.from("buyers").update(patch).eq("buyer_id", buyerId);
      if (error) throw new Error("buyers update: " + error.message);
      return res.status(200).json({ ok: true });
    }

    const buyerId = req.query && req.query.buyer_id ? parseInt(req.query.buyer_id) : null;
    if (!buyerId) return res.status(400).json({ error: "buyer_id required" });

    const [profRes, rfmRes, salesRes, notesRes] = await Promise.all([
      supabase.from("buyers").select("*").eq("buyer_id", buyerId).maybeSingle(),
      supabase.from("buyer_rfm").select("*").eq("buyer_id", buyerId).maybeSingle(),
      supabase
        .from("sales")
        .select("order_id, ordered_at, order_status, quantity, unit_price, revenue, name_snapshot")
        .eq("buyer_id", buyerId)
        .order("ordered_at", { ascending: false })
        .limit(200),
      supabase
        .from("buyer_notes")
        .select("id, created_at, outcome, body")
        .eq("buyer_id", buyerId)
        .order("created_at", { ascending: false }),
    ]);
    if (profRes.error) throw new Error("buyers select: " + profRes.error.message);
    if (rfmRes.error)  throw new Error("buyer_rfm select: " + rfmRes.error.message);
    if (salesRes.error) throw new Error("sales select: " + salesRes.error.message);
    if (notesRes.error) throw new Error("notes select: " + notesRes.error.message);

    if (!profRes.data) return res.status(404).json({ error: "buyer not found" });

    // Группируем заказы по order_id, оставляем 20 последних.
    const byOrder = new Map();
    for (const r of salesRes.data || []) {
      const ok = !EXCLUDED_STATUSES.has(r.order_status || "") && !isMergerStatus(r.order_status || "");
      const entry = byOrder.get(r.order_id) || {
        order_id: r.order_id,
        ordered_at: r.ordered_at,
        order_status: r.order_status,
        items: 0,
        qty: 0,
        revenue: 0,
        valid: ok,
      };
      entry.items += 1;
      entry.qty += parseFloat(r.quantity) || 0;
      entry.revenue += parseFloat(r.revenue) || 0;
      byOrder.set(r.order_id, entry);
    }
    const orders = Array.from(byOrder.values())
      .sort((a, b) => (a.ordered_at < b.ordered_at ? 1 : -1))
      .slice(0, 20);

    const enriched = enrichRfmRow(rfmRes.data);
    const monetary = rfmRes.data && rfmRes.data.monetary != null ? parseFloat(rfmRes.data.monetary) : 0;

    return res.status(200).json({
      buyer: profRes.data,
      rfm: rfmRes.data || null,
      enriched: Object.assign({}, enriched, { ltv: monetary }),
      orders,
      notes: notesRes.data || [],
    });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
