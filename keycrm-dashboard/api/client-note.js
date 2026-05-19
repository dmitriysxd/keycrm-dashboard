// CRUD заметок звонков.
//
// POST   /api/client-note         body: { buyer_id, outcome?, body }
// DELETE /api/client-note?id=...

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
  res.setHeader("Access-Control-Allow-Methods", "POST,DELETE,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const supabase = getSupabase();

  try {
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

    if (req.method === "DELETE") {
      const id = req.query && req.query.id ? parseInt(req.query.id) : null;
      if (!id) return res.status(400).json({ error: "id required" });
      const { error } = await supabase.from("buyer_notes").delete().eq("id", id);
      if (error) throw new Error("notes delete: " + error.message);
      return res.status(200).json({ ok: true });
    }

    return res.status(405).json({ error: "method not allowed" });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err) });
  }
};
