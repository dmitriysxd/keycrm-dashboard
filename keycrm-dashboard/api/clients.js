// Список клиентов с RFM-метриками.
//
// GET /api/clients?token=...&wholesale_only=1&status_id=3&search=ivan&segment=champions
//
// Делаем три запроса в Supabase и сшиваем в JS:
//   - buyers (master) + buyer_statuses (через JOIN-фильтрацию по status_id здесь
//     не нужна — рисуем статус по id в UI)
//   - buyer_rfm (materialized view)
// Покупателей у нас порядка единиц тысяч, поэтому отдаём всё одним массивом
// и сортируем/пагинируем на фронте — как и для SKU.

const { getSupabase } = require("../lib/supabase");
const { checkDashboardToken } = require("../lib/auth");

async function fetchAll(buildQuery, pageSize = 1000, hardCap = 50000) {
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

// Маппинг сегментов из (r,f) — упрощённая модель R-F (M используется только как метрика).
function classifySegment(r, f) {
  if (r >= 4 && f >= 4) return "champions";
  if (r >= 3 && f >= 3) return "loyal";
  if (r >= 4 && f <= 2) return "new";
  if (r <= 2 && f >= 3) return "at_risk";
  if (r <= 2 && f <= 2) return "lost";
  return "potential";
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();

  try {
    const q = req.query || {};
    const wholesaleOnly = q.wholesale_only === "1" || q.wholesale_only === "true";
    const statusId = q.status_id ? parseInt(q.status_id) : null;
    const segment = q.segment || null;
    const search = q.search ? String(q.search).toLowerCase().trim() : "";

    if (q.meta === "true") {
      // Списки для UI: статусы + быстрые KPI.
      const statuses = await fetchAll(() =>
        supabase.from("buyer_statuses").select("id, name, color, sort_order").order("sort_order", { ascending: true })
      );
      const buyersHead = await supabase.from("buyers").select("buyer_id", { count: "exact", head: true });
      const wholesaleHead = await supabase
        .from("buyers")
        .select("buyer_id", { count: "exact", head: true })
        .eq("is_wholesale", true);
      return res.status(200).json({
        statuses,
        total_buyers: buyersHead.count || 0,
        wholesale_buyers: wholesaleHead.count || 0,
        now: new Date().toISOString(),
      });
    }

    // Основной список.
    let buyersBuilder = () => {
      let bq = supabase.from("buyers").select("buyer_id, full_name, phone, email, is_wholesale, status_id, first_seen_at");
      if (wholesaleOnly) bq = bq.eq("is_wholesale", true);
      if (statusId) bq = bq.eq("status_id", statusId);
      return bq;
    };
    const buyers = await fetchAll(buyersBuilder);

    // RFM-метрики — отдельно, затем мерж по buyer_id (LEFT JOIN на стороне Node).
    const rfm = await fetchAll(() =>
      supabase.from("buyer_rfm").select("buyer_id, last_order_date, recency_days, frequency, monetary, r_score, f_score, m_score")
    );
    const rfmById = new Map();
    for (const r of rfm) rfmById.set(r.buyer_id, r);

    const statusRows = await fetchAll(() =>
      supabase.from("buyer_statuses").select("id, name, color")
    );
    const statusById = new Map();
    for (const s of statusRows) statusById.set(s.id, s);

    const merged = buyers.map((b) => {
      const m = rfmById.get(b.buyer_id) || {};
      const r_score = m.r_score || null;
      const f_score = m.f_score || null;
      const m_score = m.m_score || null;
      const seg = r_score && f_score ? classifySegment(r_score, f_score) : null;
      const st = b.status_id ? statusById.get(b.status_id) : null;
      return {
        buyer_id: b.buyer_id,
        full_name: b.full_name,
        phone: b.phone,
        email: b.email,
        is_wholesale: b.is_wholesale,
        status_id: b.status_id,
        status_name: st ? st.name : null,
        status_color: st ? st.color : null,
        first_seen_at: b.first_seen_at,
        last_order_date: m.last_order_date || null,
        recency_days: m.recency_days == null ? null : m.recency_days,
        frequency: m.frequency || 0,
        monetary: m.monetary == null ? 0 : parseFloat(m.monetary),
        r_score, f_score, m_score,
        rfm_code: r_score && f_score && m_score ? `${r_score}${f_score}${m_score}` : null,
        segment: seg,
      };
    });

    let filtered = merged;
    if (segment) filtered = filtered.filter((r) => r.segment === segment);
    if (search) {
      filtered = filtered.filter((r) => {
        const hay = ((r.full_name || "") + " " + (r.phone || "") + " " + (r.email || "")).toLowerCase();
        return hay.indexOf(search) !== -1;
      });
    }

    return res.status(200).json({
      rows: filtered,
      count: filtered.length,
      total: merged.length,
    });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
