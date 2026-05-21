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
      physical_proportions: { type: ["string", "null"], description: "Пропорції форми: 'витягнуте по вертикалі', 'квадратне', 'широке низьке', 'компактне', 'довге тонке', 'круглі однакові' тощо. Допомагає знайти схожі за формою." },

      // ДВА окремих описи:
      // (а) general_description — широке покриття атрибутів (форма, камінь, стиль)
      // (б) distinctive_description — РОЗГОРНУТО про те, що робить цей виріб
      //     УНІКАЛЬНИМ серед інших такого ж типу. Це найважливіше для рекомендацій
      //     "схоже за дизайном" і для рукописання sales-скриптів.
      general_description: {
        type: "string",
        description: "Загальний опис українською, 3-4 речення. Покриває: тип виробу, форма і пропорції, матеріал/покриття, камінь (якщо є) і його характеристики, основний стиль і настрій. Це короткий 'паспорт' товару."
      },
      distinctive_description: {
        type: "string",
        description: "ДЕТАЛЬНИЙ розгорнутий опис українською, 6-10 речень — найголовніша частина для пошуку схожих і скриптів продажу. Опиши КОНКРЕТНО що відрізняє ЦЕЙ виріб від інших такого ж типу (інших метеликів/сердець/сережок-цвяшок). Включи: точну форму композиції (асиметрія, розкриті/складені крила, кількість пелюсток, тип орнаменту), розташування і кількість елементів, деталі поверхні (гравіровки, проколи, рельєф), як саме закріплений камінь і де він розташований, ажурні vs суцільні ділянки, пропорції розмірів частин. Уяви що клієнт по телефону хоче, щоб ти описав ЦЕЙ конкретний виріб — він має його уявити, не схожий. Якщо немає особливих рис — чесно скажи 'класична проста форма без декору'."
      },

      unique_features: {
        type: "array",
        items: { type: "string" },
        description: "5-7 пунктів-тезисів того, що візуально відрізняє цей виріб. Короткі фрази для UI: 'ажурні крила з прорізами', 'асиметричне розташування', 'центральний камінь у формі краплі', 'тонка філігранна обводка', 'три ряди дрібних каменів'. Якщо реально немає особливостей — '[нема унікальних рис, проста форма]'."
      },
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
      "color_palette", "tags",
      "physical_proportions",
      "general_description", "distinctive_description", "unique_features"
    ],
  };

  const body = {
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "Ти експерт ювелірного дизайну і аналітик каталогу для опт-магазину Xuping. " +
          "Тобі дають фото товару і його назву. " +
          "\n\n" +
          "КОНТЕКСТ: каталог — біжутерія Xuping з медичного золота з покриттям " +
          "родій / позолота 18К / золото rose. Тисячі моделей різних форм. " +
          "Клієнти обирають за дизайном — назви часто однакові для серії схожих товарів. " +
          "\n\n" +
          "ТВОЇ ДВІ ЗАДАЧІ:\n" +
          "1. Заповнити СТРУКТУРОВАНІ АТРИБУТИ (форма, камінь, стиль, мотиви, декорації " +
          "тощо) — для фільтрів і UI.\n" +
          "2. Написати ДВА ОПИСИ:\n" +
          "   • general_description (3-4 речення) — широкий 'паспорт' товару\n" +
          "   • distinctive_description (6-10 речень) — РОЗГОРНУТО про конкретні " +
          "візуальні особливості ЦЬОГО виробу. Це найцінніше для рекомендацій і " +
          "скриптів продажу. Не загальні фрази типу 'елегантний' — а конкретика: " +
          "'крила метелика з розкритих крайніх ребер, кожне з трьома прорізами, " +
          "тулуб у формі овальної намистини з гравіруванням'.\n" +
          "\n" +
          "ПРИНЦИПИ:\n" +
          "- Конкретність: не 'квіти' а 'троянда', не 'круглий камінь' а " +
          "'круглий камінь ~4мм у центрі'.\n" +
          "- distinctive_description має бути по фото — якщо вірогідно існує " +
          "сотня схожих метеликів, опиши що відрізняє ЦЕЙ.\n" +
          "- Якщо реально проста класична форма без особливостей — чесно так і " +
          "напиши, не вигадуй декорацій.\n" +
          "- Використовуй інформацію з назви (розмір 4мм, тип) разом з фото.\n" +
          "- Якщо не можеш визначити — null / 'unknown'. Не вигадуй.",
      },
      {
        role: "user",
        content: [
          {
            type: "text",
            text: "Назва товару: " + (productName || "—") +
                  "\n\nПроаналізуй фото ювелірного виробу і поверни структурований JSON.",
          },
          // detail=low: економний режим (~85 tokens/image). Для біжутерії
          // достатньо щоб бачити форму, мотиви, наявність каменя. Для дрібних
          // деталей орнаменту іноді можна upgrade на high, але це 3-5x дорожче.
          { type: "image_url", image_url: { url: imageUrl, detail: "low" } },
        ],
      },
    ],
    response_format: {
      type: "json_schema",
      json_schema: { name: "product_design", strict: true, schema },
    },
    temperature: 0.1,
    max_tokens: 2000,
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
// КРИТИЧНО: для каталогу Xuping (де ВСІ товари мають медичне золото /
// родій / позолоту 18К) — НЕ включаємо в embedding атрибути, які ОДНАКОВІ
// у всіх. Інакше вектор забивається "шумом" і всі товари схожі на всі.
//
// ВКЛЮЧАЄМО (диференціюючі): форма, мотиви, декорації, унікальні риси,
// пропорції, опис, конкретні деталі каменя і композиції.
//
// ВИКЛЮЧАЄМО (носують у всіх): coating, color_palette (золото/срібло),
// target_gender=feminine, target_age=adult, бренд.
function buildEmbeddingText(attrs, productName) {
  const parts = [];

  // 1) Категорія і підтип (різниця сережки vs кулон важлива)
  if (attrs.category) parts.push(attrs.category);
  if (attrs.subcategory) parts.push(attrs.subcategory);

  // 2) ФОРМА — топ-1 диференціатор
  if (attrs.overall_shape) parts.push("форма: " + attrs.overall_shape);
  if (attrs.physical_proportions) parts.push("пропорції: " + attrs.physical_proportions);
  if (attrs.symmetry && attrs.symmetry !== "unknown" && attrs.symmetry !== "symmetric") {
    parts.push(attrs.symmetry);  // тільки asymmetric/abstract — symmetric у більшості
  }
  if (attrs.volume && attrs.volume !== "unknown") parts.push("об'єм: " + attrs.volume);

  // 3) Поверхня і обробка
  if (attrs.surface_texture && attrs.surface_texture !== "глянцева") {
    parts.push("текстура: " + attrs.surface_texture);  // глянцева у багатьох — пропускаємо
  }
  if (attrs.finish) parts.push("обробка: " + attrs.finish);

  // 4) КАМІНЬ — деталі
  if (attrs.has_stone) {
    const stone = [
      attrs.stone_color && ("камінь " + attrs.stone_color),
      attrs.stone_shape && ("форма каменя " + attrs.stone_shape),
      attrs.stone_size && (attrs.stone_size + " розмір"),
      attrs.stone_count,
      attrs.stone_arrangement && (attrs.stone_arrangement),
      attrs.setting_type && ("закріплення " + attrs.setting_type),
    ].filter(Boolean).join(", ");
    if (stone) parts.push(stone);
  } else {
    parts.push("без каменя");  // важливо для розрізнення з/без
  }

  // 5) Стиль/настрій (тільки якщо вказані конкретно)
  if (attrs.style && attrs.style !== "unknown" && attrs.style !== "classic") {
    parts.push("стиль " + attrs.style);  // classic у багатьох — пропускаємо
  }
  if (attrs.mood) parts.push(attrs.mood);
  if (attrs.theme) parts.push("тема " + attrs.theme);
  if (attrs.design_complexity && attrs.design_complexity >= 3) {
    parts.push("складність " + attrs.design_complexity + "/5");  // 1-2 у більшості
  }

  // 6) МОТИВИ і ДЕКОРАЦІЇ — критичні диференціатори
  if (Array.isArray(attrs.motifs) && attrs.motifs.length) {
    parts.push("мотиви: " + attrs.motifs.join(", "));
  }
  if (Array.isArray(attrs.decorations) && attrs.decorations.length) {
    parts.push("декорації: " + attrs.decorations.join(", "));
  }

  // 7) Функціональні деталі (для конкретної категорії)
  if (attrs.functional_details && typeof attrs.functional_details === "object") {
    const fd = attrs.functional_details;
    if (fd.earring_back && fd.earring_back !== "pусет") parts.push("замок: " + fd.earring_back);
    if (fd.chain_style) parts.push("плетіння: " + fd.chain_style);
    if (fd.ring_shank) parts.push("шинка: " + fd.ring_shank);
  }

  // 8) UNIQUE FEATURES — топ-3 диференціатори, тут найбільш цінна інформація
  if (Array.isArray(attrs.unique_features) && attrs.unique_features.length) {
    parts.push("УНІКАЛЬНО: " + attrs.unique_features.join("; "));
  }

  // 9) Ключові теги (відфільтровані від генериків)
  if (Array.isArray(attrs.tags) && attrs.tags.length) {
    const filtered = attrs.tags.filter(t => {
      const lc = String(t).toLowerCase();
      // Викидаємо теги які повторюються в усьому каталозі
      return !["xuping", "медичне золото", "позолота 18к", "родій", "позолота",
               "ювелірний виріб", "біжутерія", "feminine", "adult"].includes(lc);
    });
    if (filtered.length) parts.push(filtered.join(", "));
  }

  // 10) Описові тексти — найважливіша частина embedding.
  //     general_description = коротко загальне.
  //     distinctive_description = розгорнуто про унікальне (ВДВІЧІ важливіше).
  //     Дублюємо distinctive_description щоб посилити її вагу в embedding.
  if (attrs.general_description) parts.push(attrs.general_description);
  if (attrs.distinctive_description) {
    parts.push(attrs.distinctive_description);
    parts.push("ОСОБЛИВОСТІ: " + attrs.distinctive_description);
  } else if (attrs.description) {
    // Зворотна сумісність зі старою версією схеми
    parts.push(attrs.description);
  }

  return parts.join(". ");
}

module.exports = { analyzeProductImage, createEmbedding, buildEmbeddingText };
