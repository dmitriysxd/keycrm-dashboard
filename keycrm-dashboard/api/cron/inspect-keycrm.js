// Диагностический endpoint: показывает реальную структуру KeyCRM-ответов,
// чтобы понять, какой UUID соответствует custom-полю "Опт/Роздріб" и какие
// поля заказа содержат сумму со скидкой.
//
// GET /api/cron/inspect-keycrm?secret=...
//
// Возвращает:
//   1. Дефиниции кастом-полей (если KeyCRM expose их через API)
//   2. Карта UUID → набор уникальных значений по 200 покупателям
//   3. Один пример сырого заказа целиком — увидим все поля totals/discount
//   4. Один пример сырого покупателя

const { checkCronAuth } = require("../../lib/auth");
const { get } = require("../../lib/keycrm");
const { getSupabase } = require("../../lib/supabase");

async function tryFetchDefinitions(apiKey, ctx) {
  // KeyCRM не документує цей endpoint явно, спробуємо кілька варіантів.
  const candidates = [
    "/custom-fields",
    "/custom-fields/buyer",
    "/buyer/custom-fields",
    "/custom-fields?entity_type=buyer",
  ];
  const tried = [];
  for (const path of candidates) {
    try {
      const resp = await get(path, {}, apiKey, ctx);
      tried.push({ path, ok: true, sample: resp });
      // Як тільки знайшли робочий — повертаємо. Решту не пробуємо.
      return tried;
    } catch (e) {
      tried.push({ path, ok: false, error: (e && e.message) || String(e) });
    }
  }
  return tried;
}

module.exports = async function handler(req, res) {
  const auth = checkCronAuth(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

  const apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

  const ctx = { apiCalls: 0 };
  const supabase = getSupabase();

  try {
    // 1. Спробувати знайти definitions custom-полів через API.
    const definitionsAttempts = await tryFetchDefinitions(apiKey, ctx);

    // 2. Один сирий buyer для перегляду.
    const sampleBuyerResp = await get("/buyer", { limit: 1, include: "custom_fields" }, apiKey, ctx);
    const sampleBuyer = sampleBuyerResp && sampleBuyerResp.data && sampleBuyerResp.data[0];

    // 3. Один сирий order — щоб побачити всі поля сум.
    const sampleOrderResp = await get("/order", { limit: 1, include: "products.offer,status,buyer" }, apiKey, ctx);
    const sampleOrder = sampleOrderResp && sampleOrderResp.data && sampleOrderResp.data[0];

    // 4. Зібрати карту UUID → set of values по 200 покупцям з БД.
    // (Беремо custom_fields_raw які ми записали з backfill — там вже є структура.)
    const { data: customRows, error } = await supabase
      .from("buyers")
      .select("buyer_id, full_name, custom_fields_raw")
      .not("custom_fields_raw", "is", null)
      .limit(200);
    if (error) throw new Error("buyers select: " + error.message);

    const uuidValueMap = {}; // uuid → { values: Set, samples: [{buyer_id, full_name, value}] }
    for (const row of customRows || []) {
      const cf = row.custom_fields_raw;
      if (!cf) continue;
      const list = Array.isArray(cf) ? cf : Object.entries(cf).map(([k, v]) => ({ uuid: k, value: v }));
      for (const item of list) {
        const uuid = item.uuid || item.id || item.key || item.name;
        if (!uuid) continue;
        const value = item.value;
        if (!uuidValueMap[uuid]) uuidValueMap[uuid] = { values: new Set(), samples: [] };
        if (value != null) uuidValueMap[uuid].values.add(String(value));
        if (uuidValueMap[uuid].samples.length < 3) {
          uuidValueMap[uuid].samples.push({ buyer_id: row.buyer_id, full_name: row.full_name, value });
        }
      }
    }
    const customFieldsBreakdown = Object.entries(uuidValueMap).map(([uuid, info]) => ({
      uuid,
      unique_values: Array.from(info.values),
      sample_buyers: info.samples,
    }));

    return res.status(200).json({
      ok: true,
      hint: [
        "1. Знайди в custom_fields_breakdown UUID з unique_values типу ['Опт', 'Роздріб'].",
        "2. Знайди в sample_order ключ із сумою з урахуванням знижки (grand_total / total / actual_total / discounted_total).",
        "3. Знайди в sample_order поле із самою знижкою (discount_amount / discount / manual_discount).",
        "4. Скинь весь цей JSON в чат — допишу код під твою CRM.",
      ],
      custom_field_definitions_attempts: definitionsAttempts,
      custom_fields_breakdown: customFieldsBreakdown,
      sample_buyer: sampleBuyer,
      sample_order: sampleOrder,
      sample_order_top_level_keys: sampleOrder ? Object.keys(sampleOrder) : null,
      api_calls: ctx.apiCalls,
    });
  } catch (err) {
    return res.status(500).json({ error: (err && err.message) || String(err), api_calls: ctx.apiCalls });
  }
};
