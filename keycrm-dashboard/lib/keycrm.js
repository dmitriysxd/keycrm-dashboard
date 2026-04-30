const BASE = "https://openapi.keycrm.app/v1";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function get(path, params, apiKey, ctx) {
  const qs = Object.keys(params || {})
    .filter((k) => params[k] !== undefined && params[k] !== null && params[k] !== "")
    .map((k) => encodeURIComponent(k) + "=" + encodeURIComponent(params[k]))
    .join("&");
  const url = BASE + path + (qs ? "?" + qs : "");

  let attempt = 0;
  while (true) {
    attempt++;
    if (ctx) ctx.apiCalls = (ctx.apiCalls || 0) + 1;
    const res = await fetch(url, {
      headers: { Authorization: "Bearer " + apiKey, Accept: "application/json" },
    });
    if (res.status === 429) {
      if (attempt >= 5) throw new Error("KEYCRM 429: вичерпано retry");
      const wait = Math.min(2000 * Math.pow(2, attempt - 1), 30000);
      await sleep(wait);
      continue;
    }
    if (res.status === 401) throw new Error("UNAUTHORIZED: невірний KEYCRM_API_KEY");
    if (!res.ok) {
      let body = "";
      try { body = await res.text(); } catch (_) {}
      throw new Error("KEYCRM HTTP " + res.status + " [" + path + "]: " + body.substring(0, 200));
    }
    return res.json();
  }
}

async function paginate(path, params, apiKey, ctx, opts) {
  const limit = (params && params.limit) || 50;
  const maxPages = (opts && opts.maxPages) || 200;
  const onPage = opts && opts.onPage;
  const all = [];
  for (let page = 1; page <= maxPages; page++) {
    const resp = await get(path, Object.assign({}, params, { page, limit }), apiKey, ctx);
    const rows = resp.data || [];
    if (onPage) await onPage(rows, page, resp);
    else all.push(...rows);
    if (rows.length < limit) break;
    const total = resp.total || (resp.meta && resp.meta.total);
    if (total && page * limit >= total) break;
    await sleep(150);
  }
  return all;
}

module.exports = { get, paginate, sleep };
