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
      subcategory:        { type: ["string", "null"], description: "Підтип: цвяшки (пусети), кільця, висячі, з підвіскою, перстень, шарм, тощо" },
      size:               { type: "string", enum: ["small", "medium", "large", "unknown"] },
      coating:            { type: ["string", "null"], description: "Покриття: родій, позолота 18К, золото rose, срібло, комбіноване, інше" },

      // Форма і силует
      overall_shape:      { type: ["string", "null"], description: "Загальна форма виробу: коло, овал, квадрат, прямокутник, крапля, серце, зірка, квітка, тварина (вказати яка), літера/символ, асиметрична, лінійна, геометрична-абстрактна, або null" },
      symmetry:           { type: "string", enum: ["symmetric", "asymmetric", "abstract", "unknown"] },
      volume:             { type: "string", enum: ["flat", "low_relief", "3d_voluminous", "hollow", "unknown"], description: "Об'ємність виробу: плаский, низький рельєф, об'ємний 3D, порожнистий" },

      // Поверхня
      surface_texture:    { type: ["string", "null"], description: "Текстура поверхні: глянцева, матова, щіткова, молоткова, гравірована, перфорована, з насічками, комбінована, або null" },
      finish:             { type: ["string", "null"], description: "Особлива обробка: антик, чернение, contrast (комбінація глянцю і матового), або null" },

      // Камінь
      has_stone:          { type: "boolean" },
      stone_color:        { type: ["string", "null"], description: "Колір каменя: білий, чорний, червоний, синій, зелений, фіолетовий, рожевий, бузковий, бежевий, кольоровий-микс, або null" },
      stone_shape:        { type: ["string", "null"], description: "Форма каменя: круглий, овал, груша/крапля, серце, квадрат princess, маркіз, изумруд (emerald cut), або null" },
      stone_size:         { type: ["string", "null"], enum: ["tiny", "small", "medium", "large", "extra_large", null], description: "Tiny ≤2мм, small 2-4мм, medium 4-7мм, large 7-12мм, extra_large >12мм" },
      stone_count:        { type: ["string", "null"], description: "один / декілька (2-5) / багато (6+) / pavé (велика кількість дрібних) або null" },
      stone_arrangement:  { type: ["string", "null"], description: "Розташування каменів: центральний / halo (навколо центрального) / pavé (суцільно вкритий) / cluster (групою) / ряд / розкидані / по периметру або null" },
      setting_type:       { type: ["string", "null"], description: "Тип закріплення: каст (prong, кігтиками), безіль (bezel, ободком), pavé, канал (channel), напівзакритий (half-bezel), без оправи (для перлів), або null" },

      // Стиль і настрій
      style:              { type: "string", enum: ["minimal", "classic", "glamour", "themed", "vintage", "modern", "boho", "art_deco", "victorian", "ethnic", "gothic", "romantic", "sporty", "unknown"] },
      theme:              { type: ["string", "null"], description: "Тематика: серце, природа/квіти, тварини/птахи, релігія, фея/міфологія, метелик, зірка, морська, космос, або null" },
      mood:               { type: ["string", "null"], enum: ["delicate", "bold", "statement", "subtle", "dramatic", "playful", "romantic", "edgy", "elegant", "casual", null] },
      design_complexity:  { type: "integer", minimum: 1, maximum: 5, description: "1=мінімум деталей, 5=дуже складний з багатьма елементами" },

      // Мотиви і декорації (масиви — щоб ловити кілька)
      motifs:             { type: "array", items: { type: "string" }, description: "До 5 впізнавані мотивів: 'квітка-троянда', 'метелик', 'хрест', 'літера M', 'нескінченність', 'сова', тощо. Конкретно а не загально." },
      decorations:        { type: "array", items: { type: "string" }, description: "До 5 декоративних елементів: 'філігрань', 'емаль', 'перли', 'гранули', 'орнамент', 'насічки', 'перфорація', тощо" },

      // Функціональні деталі
      functional_details: {
        type: ["object", "null"],
        additionalProperties: false,
        properties: {
          earring_back:    { type: ["string", "null"], description: "Для сережок: pусет (stud), гачок, англійський замок (lever back), кільце, конго, чалма" },
          chain_style:     { type: ["string", "null"], description: "Для ланцюжків/браслетів: якірний, панцирний, snake, ropa, бісмарк, плетіння" },
          ring_shank:      { type: ["string", "null"], description: "Для каблучок: тонка/середня/широка шинка, з різьбленням, гладка, плетена" },
          pendant_only:    { type: ["boolean", "null"], description: "Чи це лише підвіска без ланцюжка" },
        },
        required: ["earring_back", "chain_style", "ring_shank", "pendant_only"],
      },

      // Контекст носіння
      target_age:         { type: ["string", "null"], enum: ["children", "teen", "adult", "universal", null] },
      target_gender:      { type: ["string", "null"], enum: ["feminine", "masculine", "unisex", null] },
      occasions:          { type: "array", items: { type: "string" }, description: "До 4 випадків носіння: повсякденно, святкове, весілля, офіс, вечір, тренування, церемоніальне" },

      // Кольорова палітра
      color_palette:      { type: "array", items: { type: "string" }, description: "До 5 основних кольорів дизайну: золотий, сріблястий, родієво-білий, рожевий, чорний, тощо" },

      // Зведення
      tags:               { type: "array", items: { type: "string" }, description: "До 12 ключових атрибутів дизайну українською (без емодзі) — найважливіше для пошуку схожих" },
      description:        { type: "string", description: "Описовий текст українською на 4-6 речень: 1) форма і силует, 2) стиль і настрій, 3) деталі поверхні і декору, 4) камінь (якщо є) і його розташування, 5) для якого випадку носіння, 6) унікальні риси які відрізняють від схожих" },
    },
    required: [
      "category", "subcategory", "size", "coating",
      "overall_shape", "symmetry", "volume",
      "surface_texture", "finish",
      "has_stone", "stone_color", "stone_shape", "stone_size", "stone_count",
      "stone_arrangement", "setting_type",
      "style", "theme", "mood", "design_complexity",
      "motifs", "decorations", "functional_details",
      "target_age", "target_gender", "occasions",
      "color_palette", "tags", "description"
    ],
  };

  const body = {
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "Ти експерт ювелірного дизайну і аналітик каталогу для опт-магазину. Тобі дають фото товару і його назву. " +
          "Витягни ДЕТАЛЬНІ атрибути дизайну для системи рекомендацій 'схоже за дизайном'. " +
          "Принципи: " +
          "(1) Будь конкретним: не 'тематика квіти', а 'троянда' / 'лілія' / 'соняшник'. " +
          "(2) Опиши форму, текстуру, обробку, розташування каменів — це найважливіше для пошуку схожих. " +
          "(3) Якщо в назві товару є інформація (розмір, колір, мотив) — використовуй її разом з фото. " +
          "(4) Описовий текст має бути 4-6 речень, по черзі: форма → стиль → деталі поверхні → камінь → випадок носіння → унікальні риси. " +
          "(5) Якщо не можеш визначити — використовуй null або 'unknown'. Не вигадуй.",
      },
      {
        role: "user",
        content: [
          {
            type: "text",
            text: "Назва: " + (productName || "—") + "\n\nПроаналізуй фото ювелірного виробу і поверни структурований JSON за схемою.",
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
    max_tokens: 1500,
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
// Структура важлива: спочатку найбільш диференціюючі ознаки (форма, стиль, мотив),
// потім деталі, в кінці — повний описовий текст. Embedding ловить семантику тексту;
// рівномірне покриття всіх граней даних = краща схожість.
function buildEmbeddingText(attrs, productName) {
  const parts = [];
  if (productName) parts.push(productName);
  if (attrs.category) parts.push(attrs.category);
  if (attrs.subcategory) parts.push(attrs.subcategory);

  // Форма і силует — найважливіше для "схожих за дизайном"
  if (attrs.overall_shape) parts.push("форма: " + attrs.overall_shape);
  if (attrs.symmetry && attrs.symmetry !== "unknown") parts.push(attrs.symmetry);
  if (attrs.volume && attrs.volume !== "unknown") parts.push("об'єм: " + attrs.volume);

  // Покриття і обробка
  if (attrs.coating) parts.push("покриття: " + attrs.coating);
  if (attrs.surface_texture) parts.push("текстура: " + attrs.surface_texture);
  if (attrs.finish) parts.push("обробка: " + attrs.finish);

  // Камінь — детально
  if (attrs.has_stone) {
    const stone = [
      "з каменем",
      attrs.stone_color && ("колір " + attrs.stone_color),
      attrs.stone_shape && ("форма " + attrs.stone_shape),
      attrs.stone_size && ("розмір " + attrs.stone_size),
      attrs.stone_count && (attrs.stone_count),
      attrs.stone_arrangement && ("розташування " + attrs.stone_arrangement),
      attrs.setting_type && ("закріплення " + attrs.setting_type),
    ].filter(Boolean).join(", ");
    parts.push(stone);
  } else {
    parts.push("без каменя");
  }

  // Стиль і настрій
  if (attrs.style && attrs.style !== "unknown") parts.push("стиль " + attrs.style);
  if (attrs.mood) parts.push("настрій " + attrs.mood);
  if (attrs.theme) parts.push("тематика " + attrs.theme);
  if (attrs.design_complexity) parts.push("складність " + attrs.design_complexity + "/5");

  // Мотиви і декорації — конкретно
  if (Array.isArray(attrs.motifs) && attrs.motifs.length) {
    parts.push("мотиви: " + attrs.motifs.join(", "));
  }
  if (Array.isArray(attrs.decorations) && attrs.decorations.length) {
    parts.push("декорації: " + attrs.decorations.join(", "));
  }

  // Функціональні деталі (для конкретної категорії)
  if (attrs.functional_details && typeof attrs.functional_details === "object") {
    const fd = attrs.functional_details;
    if (fd.earring_back) parts.push("замок сережок: " + fd.earring_back);
    if (fd.chain_style) parts.push("плетіння ланцюжка: " + fd.chain_style);
    if (fd.ring_shank) parts.push("шинка каблучки: " + fd.ring_shank);
  }

  // Цільова аудиторія і випадки носіння
  if (attrs.target_age && attrs.target_age !== "universal") parts.push("вік: " + attrs.target_age);
  if (attrs.target_gender && attrs.target_gender !== "unisex") parts.push("стать: " + attrs.target_gender);
  if (Array.isArray(attrs.occasions) && attrs.occasions.length) {
    parts.push("випадки: " + attrs.occasions.join(", "));
  }

  // Кольорова палітра і теги
  if (Array.isArray(attrs.color_palette) && attrs.color_palette.length) {
    parts.push("кольори: " + attrs.color_palette.join(", "));
  }
  if (Array.isArray(attrs.tags) && attrs.tags.length) {
    parts.push(attrs.tags.join(", "));
  }

  // Повний описовий текст — найдовший шматок, embedding ловить його семантику
  if (attrs.description) parts.push(attrs.description);

  return parts.join(". ");
}

module.exports = { analyzeProductImage, createEmbedding, buildEmbeddingText };
