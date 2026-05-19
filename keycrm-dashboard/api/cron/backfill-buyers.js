// Одноразовый бэкфил карточек покупателей.
//
// При ежедневном инжесте мы UPSERT'им buyers только из текущих заказов, поэтому
// исторические клиенты остаются с пустыми full_name/phone/is_wholesale.
// Этот эндпоинт берёт все DISTINCT buyer_id из sales, у которых нет полной
// карточки в buyers, и тянет /buyer/{id} из KeyCRM.
//
// Запуск:
//   GET /api/cron/backfill-buyers?secret=...&limit=300
// Параметры:
//   limit — сколько ID обработать за этот вызов (по умолчанию 300).
//   force — true → перетягивать всех, даже у кого full_name уже есть.
//
// Из-за 60-секундного лимита Vercel вызываем эндпоинт несколько раз, пока в
// ответе more=true.

const { getSupabase } = require("../../lib/supabase");
const { checkCronAuth } = require("../../lib/auth");
const { get, sleep } = require("../../lib/keycrm");

const PARALLEL = 4;
const TIME_BUDGET_MS = 50 * 1000;

function pickPhone(b) {
  if (!b) return null;
  const cs = [b.phone, b.phone_number, b.mobile, b.tel];
  for (const v of cs) {
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

function parseWholesaleFlag(buyer) {
  if (!buyer) return false;
  const cf = buyer.custom_fields || buyer.customFields || buyer.fields;
  if (!cf) return false;
  const entries = Array.isArray(cf)
    ? cf.map((x) => [String(x.name || x.label || x.key || "").toLowerCase(), x.value])
    : Object.entries(cf).map(([k, v]) => [String(k).toLowerCase(), v]);
  for (const [name, value] of entries) {
    if (!name.includes("опт")) continue;
    if (value === true || value === 1) return true;
    if (value == null) return false;
    const s = String(value).trim().toLowerCase();
    if (s === "" || s === "0" || s === "false" || s === "no" || s === "ні") return false;
    return true;
  }
  return false;
}

async function fetchBuyer(apiKey, id, ctx) {
  // KeyCRM эндпоинт детального покупателя; формат ответа варьируется
  // (data: {…} или сам объект на верхнем уровне).
  try {
    const resp = await get("/buyer/" + id, { include: "custom_fields" }, apiKey, ctx);
    if (resp && resp.data && typeof resp.data === "object") return resp.data;
    if (resp && resp.id) return resp;
    return null;
  } catch (e) {
    return { __error: (e && e.message) || String(e) };
  }
}

async function collectTargetIds(supabase, force, limit) {
  // ID, которые есть в sales и которых нет в buyers (или forced).
  // Делаем диапазонами по 1000 — Supabase не любит огромный IN.
  const sales = [];
  const PAGE = 1000;
  for (let from = 0; from < 200000; from += PAGE) {
    const { data, error } = await supabase
      .from("sales")
      .select("buyer_id")
      .not("buyer_id", "is", null)
      .range(from, from + PAGE - 1);
    if (error) throw new Error("sales scan: " + error.message);
    if (!data || !data.length) break;
    for (const r of data) sales.push(r.buyer_id);
    if (data.length < PAGE) break;
  }
  const uniq = Array.from(new Set(sales));

  // Чьи карточки уже заполнены — пропускаем, если !force.
  const known = new Set();
  if (!force) {
    for (let from = 0; from < 200000; from += PAGE) {
      const { data, error } = await supabase
        .from("buyers")
        .select("buyer_id, full_name")
        .range(from, from + PAGE - 1);
      if (error) throw new Error("buyers scan: " + error.message);
      if (!data || !data.length) break;
      for (const r of data) {
        if (r.full_name && r.full_name.trim() !== "") known.add(r.buyer_id);
      }
      if (data.length < PAGE) break;
    }
  }

  const targets = [];
  for (const id of uniq) {
    if (!force && known.has(id)) continue;
    targets.push(id);
    if (targets.length >= limit) break;
  }
  return { targets, totalDistinct: uniq.length, alreadyKnown: known.size };
}

module.exports = async function handler(req, res) {
  const auth = checkCronAuth(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

  const apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

  const supabase = getSupabase();
  const ctx = { apiCalls: 0 };
  const force = !!(req.query && req.query.force === "true");
  const limit = Math.max(1, Math.min(2000, parseInt((req.query && req.query.limit) || "300")));

  let runId = null;
  try {
    const ins = await supabase
      .from("ingest_runs")
      .insert({ kind: "backfill_buyers", status: "running", meta: { limit, force } })
      .select("id")
      .single();
    if (ins.error) throw new Error("ingest_runs insert: " + ins.error.message);
    runId = ins.data.id;

    const { targets, totalDistinct, alreadyKnown } = await collectTargetIds(supabase, force, limit);

    const startMs = Date.now();
    const rows = [];
    const errors = [];
    let processed = 0;

    for (let i = 0; i < targets.length; i += PARALLEL) {
      if (Date.now() - startMs > TIME_BUDGET_MS) break;
      const batch = targets.slice(i, i + PARALLEL);
      const results = await Promise.all(batch.map((id) => fetchBuyer(apiKey, id, ctx)));
      for (let j = 0; j < batch.length; j++) {
        const id = batch[j];
        const b = results[j];
        processed++;
        if (!b) continue;
        if (b.__error) { errors.push({ id, error: b.__error }); continue; }
        rows.push({
          buyer_id: id,
          full_name: pickFullName(b),
          phone: pickPhone(b),
          email: pickEmail(b),
          is_wholesale: parseWholesaleFlag(b),
          custom_fields_raw: b.custom_fields || b.customFields || b.fields || null,
          last_synced_at: new Date().toISOString(),
        });
      }
      if (rows.length >= 100) {
        const { error } = await supabase.from("buyers").upsert(rows.splice(0, rows.length), { onConflict: "buyer_id" });
        if (error) throw new Error("buyers upsert: " + error.message);
      }
      await sleep(50);
    }
    if (rows.length) {
      const { error } = await supabase.from("buyers").upsert(rows, { onConflict: "buyer_id" });
      if (error) throw new Error("buyers upsert (tail): " + error.message);
    }

    const remaining = targets.length - processed;
    await supabase
      .from("ingest_runs")
      .update({
        finished_at: new Date().toISOString(),
        status: "ok",
        api_calls: ctx.apiCalls,
        meta: {
          step: "backfill_buyers",
          totalDistinct,
          alreadyKnown,
          processed,
          errors_count: errors.length,
          remaining,
        },
      })
      .eq("id", runId);

    return res.status(200).json({
      ok: true,
      run_id: runId,
      total_distinct: totalDistinct,
      already_known: alreadyKnown,
      processed,
      remaining,
      more: remaining > 0,
      api_calls: ctx.apiCalls,
      errors_count: errors.length,
      errors_sample: errors.slice(0, 5),
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
    return res.status(500).json({ error: msg });
  }
};
