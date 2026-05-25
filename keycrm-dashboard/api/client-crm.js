// Об'єднаний CRM-endpoint для нотаток дзвінків і довідника статусів.
// Раніше це були окремі /api/client-note і /api/client-statuses, об'єднано
// щоб укластися в 12-функційний ліміт Vercel Hobby.
//
// === НОТАТКИ ДЗВІНКІВ ===
// POST   /api/client-crm?type=note         body: { buyer_id, outcome?, body }
// PATCH  /api/client-crm?type=note&id=...  body: { outcome?, body? }
// DELETE /api/client-crm?type=note&id=...
//
// === ДОВІДНИК СТАТУСІВ ===
// GET    /api/client-crm?type=statuses
// POST   /api/client-crm?type=statuses     body: { name, color?, sort_order? }
// DELETE /api/client-crm?type=statuses&id=...

const { getSupabase } = require("../lib/supabase");
const { checkDashboardToken } = require("../lib/auth");

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

async function handleNote(req, res, supabase) {
  if (req.method === "POST") {
    const body = await readBody(req);
    const buyerId = parseInt(body.buyer_id);
    const text = (body.body || "").toString().trim();
    const outcome = body.outcome ? String(body.outcome).trim().slice(0, 80) : null;
    if (!buyerId) return res.status(400).json({ error: "buyer_id required" });
    if (!text) return res.status(400).json({ error: "body required" });

    const { data, error } = await supabase
      .from("buyer_notes")
      .insert({ buyer_id: buyerId, outcome, body: text.slice(0, 4000) })
      .select("id, created_at, outcome, body")
      .single();
    if (error) throw new Error("notes insert: " + error.message);
    return res.status(200).json({ ok: true, note: data });
  }

  if (req.method === "PATCH") {
    const id = req.query && req.query.id ? parseInt(req.query.id) : null;
    if (!id) return res.status(400).json({ error: "id required" });
    const body = await readBody(req);
    const patch = {};
    if (body.body != null) {
      const text = String(body.body).trim();
      if (!text) return res.status(400).json({ error: "body cannot be empty" });
      patch.body = text.slice(0, 4000);
    }
    if (body.outcome !== undefined) {
      patch.outcome = body.outcome ? String(body.outcome).trim().slice(0, 80) : null;
    }
    if (Object.keys(patch).length === 0) return res.status(400).json({ error: "nothing to update" });
    const { data, error } = await supabase
      .from("buyer_notes")
      .update(patch)
      .eq("id", id)
      .select("id, created_at, outcome, body")
      .single();
    if (error) throw new Error("notes update: " + error.message);
    return res.status(200).json({ ok: true, note: data });
  }

  if (req.method === "DELETE") {
    const id = req.query && req.query.id ? parseInt(req.query.id) : null;
    if (!id) return res.status(400).json({ error: "id required" });
    const { error } = await supabase.from("buyer_notes").delete().eq("id", id);
    if (error) throw new Error("notes delete: " + error.message);
    return res.status(200).json({ ok: true });
  }

  return res.status(405).json({ error: "method not allowed for type=note" });
}

async function handleStatuses(req, res, supabase) {
  if (req.method === "GET") {
    const { data, error } = await supabase
      .from("buyer_statuses")
      .select("id, name, color, sort_order")
      .order("sort_order", { ascending: true });
    if (error) throw new Error("statuses select: " + error.message);
    return res.status(200).json({ rows: data || [] });
  }

  if (req.method === "POST") {
    const body = await readBody(req);
    const name = (body.name || "").toString().trim();
    if (!name) return res.status(400).json({ error: "name required" });
    const color = body.color ? String(body.color).slice(0, 16) : null;
    const sortOrder = body.sort_order != null ? parseInt(body.sort_order) : 100;
    const { data, error } = await supabase
      .from("buyer_statuses")
      .insert({ name: name.slice(0, 64), color, sort_order: sortOrder })
      .select("id, name, color, sort_order")
      .single();
    if (error) throw new Error("statuses insert: " + error.message);
    return res.status(200).json({ ok: true, status: data });
  }

  if (req.method === "DELETE") {
    const id = req.query && req.query.id ? parseInt(req.query.id) : null;
    if (!id) return res.status(400).json({ error: "id required" });
    const { error } = await supabase.from("buyer_statuses").delete().eq("id", id);
    if (error) throw new Error("statuses delete: " + error.message);
    return res.status(200).json({ ok: true });
  }

  return res.status(405).json({ error: "method not allowed for type=statuses" });
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();
  const type = (req.query && req.query.type) || "";

  try {
    if (type === "note") return await handleNote(req, res, supabase);
    if (type === "statuses") return await handleStatuses(req, res, supabase);
    return res.status(400).json({ error: "type required: ?type=note | ?type=statuses" });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
