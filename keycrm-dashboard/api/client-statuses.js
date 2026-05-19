// CRUD справочника статусов клиента.
//
// GET    /api/client-statuses
// POST   /api/client-statuses                body: { name, color?, sort_order? }
// DELETE /api/client-statuses?id=...

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

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();

  try {
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

    return res.status(405).json({ error: "method not allowed" });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
