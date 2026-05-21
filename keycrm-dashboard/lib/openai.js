// Мінімальний OpenAI клієнт для аналізу зображень товарів і створення
// text embeddings. Без npm-залежностей — тільки fetch.

const BASE = "https://api.openai.com/v1";

function getApiKey() {
  const key = process.env.OPENAI_API_KEY;
  if (!key) throw new Error("OPENAI_API_KEY не налаштовано в env");
  return key;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Загальний wrapper з retry на 429 і 5xx.
async function callOpenAI(path, body, opts) {
  const apiKey = getApiKey();
  const maxAttempts = (opts && opts.maxAttempts) || 4;
  let attempt = 0;
  while (true) {
    attempt++;
    const res = await fetch(BASE + path, {
      method: "POST",
      headers: {
        "Authorization": "Bearer " + apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
    if (res.ok) return res.json();

    const text = await res.text().catch(() => "");
    const isRetryable = res.status === 429 || res.status === 500 || res.status === 502 || res.status === 503;
    if (!isRetryable || attempt >= maxAttempts) {
      throw new Error(`OpenAI ${res.status} [${path}]: ${text.substring(0, 300)}`);
    }
    const wait = Math.min(2000 * Math.pow(2, attempt - 1), 30000);
    await sleep(wait);
  }
}

// Аналіз зображення товару → структурований JSON з тегами і описом.
// Використовуємо gpt-4o-mini з JSON Schema для гарантованого валідного виходу.
async function analyzeProductImage(imageUrl, productName) {
  const schema = {
    type: "object",
    additionalProperties: false,
    properties: {
      category:           { type: "string", description: "Тип товару: сережки, кулон, кільце, каф, браслет, комплект, ланцюжок, шпильки, інше" },
      subcategory:        { type: ["string", "null"], description: "Підтип: цвяшки, кільця, висячі, з підвіскою, перстень тощо" },
      size:               { type: "string", enum: ["small", "medium", "large", "unknown"] },
      coating:            { type: ["string", "null"], description: "Покриття: родій, позолота 18К, золото rose, срібло, інше" },
      has_stone:          { type: "boolean" },
      stone_color:        { type: ["string", "null"], description: "Колір каменя: білий, чорний, червоний, синій, зелений, фіолетовий, рожевий, кольоровий-микс або null" },
      stone_shape:        { type: ["string", "null"], description: "Форма каменя: круглий, овал, груша, серце, квадрат, маркіз або null" },
      stone_count:        { type: ["string", "null"], description: "Один / декілька / багато / pavé або null" },
      style:              { type: "string", enum: ["minimal", "classic", "glamour", "themed", "vintage", "modern", "boho", "unknown"] },
      theme:              { type: ["string", "null"], description: "Тематика: серце, тварини, природа, релігія, метелики, фея, квіти, абстракт або null" },
      design_complexity:  { type: "integer", minimum: 1, maximum: 5, description: "1=мінімальний, 5=дуже складний з багатьма елементами" },
      target_age:         { type: ["string", "null"], enum: ["children", "teen", "adult", "universal", null] },
      occasions:          { type: "array", items: { type: "string" }, description: "Випадки носіння: повсякденно, святкове, весілля, офіс тощо" },
      color_palette:      { type: "array", items: { type: "string" }, description: "До 5 основних кольорів дизайну" },
      tags:               { type: "array", items: { type: "string" }, description: "До 10 ключових атрибутів дизайну українською (без емодзі)" },
      description:        { type: "string", description: "Описовий текст дизайну українською, 2-4 речення для embedding пошуку" },
    },
    required: [
      "category", "subcategory", "size", "coating", "has_stone", "stone_color",
      "stone_shape", "stone_count", "style", "theme", "design_complexity",
      "target_age", "occasions", "color_palette", "tags", "description"
    ],
  };

  const body = {
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "Ти аналітик ювелірного каталогу опт-магазину. Тобі дають фото товару і його назву. " +
          "Витягни структуровані атрибути дизайну для системи рекомендацій. " +
          "Описуй українською. Будь точним: якщо камінь не видно — has_stone=false. " +
          "Якщо не можеш визначити — використовуй null або 'unknown'.",
      },
      {
        role: "user",
        content: [
          {
            type: "text",
            text: "Назва: " + (productName || "—") + "\n\nПроаналізуй фото товару і поверни структурований JSON за схемою.",
          },
          { type: "image_url", image_url: { url: imageUrl, detail: "low" } },
        ],
      },
    ],
    response_format: {
      type: "json_schema",
      json_schema: { name: "product_design", strict: true, schema },
    },
    temperature: 0,
    max_tokens: 800,
  };

  const resp = await callOpenAI("/chat/completions", body);
  const content = resp.choices && resp.choices[0] && resp.choices[0].message && resp.choices[0].message.content;
  if (!content) throw new Error("OpenAI vision: пуста відповідь");
  try {
    return JSON.parse(content);
  } catch (e) {
    throw new Error("OpenAI vision: невалідний JSON: " + content.substring(0, 200));
  }
}

// Створити embedding для текстового опису.
// text-embedding-3-small повертає вектор 1536 елементів за ~$0.02/1M токенів.
async function createEmbedding(text) {
  const resp = await callOpenAI("/embeddings", {
    model: "text-embedding-3-small",
    input: text,
    encoding_format: "float",
  });
  const emb = resp.data && resp.data[0] && resp.data[0].embedding;
  if (!emb || !Array.isArray(emb) || emb.length !== 1536) {
    throw new Error("OpenAI embedding: некоректна відповідь");
  }
  return emb;
}

// Згенерувати text representation для embedding на основі attributes + description.
// Конкатенуємо найважливіші теги першими, описовий текст в кінці.
function buildEmbeddingText(attrs, productName) {
  const parts = [];
  if (productName) parts.push(productName);
  if (attrs.category) parts.push(attrs.category);
  if (attrs.subcategory) parts.push(attrs.subcategory);
  if (attrs.coating) parts.push("покриття: " + attrs.coating);
  if (attrs.has_stone) {
    const stone = [
      "з каменем",
      attrs.stone_color && ("колір " + attrs.stone_color),
      attrs.stone_shape && ("форма " + attrs.stone_shape),
      attrs.stone_count && (attrs.stone_count + " шт"),
    ].filter(Boolean).join(", ");
    parts.push(stone);
  } else {
    parts.push("без каменя");
  }
  if (attrs.style && attrs.style !== "unknown") parts.push("стиль " + attrs.style);
  if (attrs.theme) parts.push("тематика " + attrs.theme);
  if (attrs.target_age && attrs.target_age !== "universal") parts.push(attrs.target_age);
  if (Array.isArray(attrs.tags)) parts.push(attrs.tags.join(", "));
  if (Array.isArray(attrs.color_palette)) parts.push("кольори: " + attrs.color_palette.join(", "));
  if (attrs.description) parts.push(attrs.description);
  return parts.join(". ");
}

module.exports = { analyzeProductImage, createEmbedding, buildEmbeddingText };
