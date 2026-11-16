const BASE = "https://openapi.keycrm.app/v1";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Таймаут на ОДИН запит. Без нього завислий запит до KeyCRM блокує Vercel-
// функцію до її 60с-ліміту → крок циклу не встигає зберегти стан (writeState)
// і цикл зависає на цьому кроці. Таймаут гарантує, що get() завжди повертає
// керування у межах кількох секунд, і крок завершується / просувається далі.
const REQUEST_TIMEOUT_MS = 12000;

async function get(path, params, apiKey, ctx) {
  const qs = Object.keys(params || {})
    .filter((k) => params[k] !== undefined && params[k] !== null && params[k] !== "")
    .map((k) => encodeURIComponent(k) + "=" + encodeURIComponent(params[k]))
    .join("&");
  const url = BASE + path + (qs ? "?" + qs : "");

  let rateRetry = 0;   // 429
  let errRetry = 0;    // таймаут / мережа
  while (true) {
    if (ctx) ctx.apiCalls = (ctx.apiCalls || 0) + 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    let res;
    try {
      res = await fetch(url, {
        headers: { Authorization: "Bearer " + apiKey, Accept: "application/json" },
        signal: controller.signal,
      });
    } catch (e) {
      clearTimeout(timer);
      // Таймаут (abort) або мережева помилка — кілька швидких ретраїв, потім
      // кидаємо. Кидок дозволяє кроку завершитись і цикл не зависає.
      if (errRetry >= 2) {
        throw new Error("KEYCRM недоступний (таймаут/мережа) [" + path + "]: " + ((e && e.message) || e));
      }
      errRetry++;
      await sleep(1000 * errRetry);
      continue;
    }
    clearTimeout(timer);
    if (res.status === 429) {
      if (rateRetry >= 5) throw new Error("KEYCRM 429: вичерпано retry");
      rateRetry++;
      await sleep(Math.min(2000 * Math.pow(2, rateRetry - 1), 30000));
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
