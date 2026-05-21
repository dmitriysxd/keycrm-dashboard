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
      unique_features:    { type: "array", items: { type: "string" }, description: "ОБОВ'ЯЗКОВО 3-5 пунктів. Що робить ЦЕЙ виріб ВІЗУАЛЬНО УНІКАЛЬНИМ серед інших такого ж типу (інших метеликів/сердець/сережок). Наприклад: 'крила метелика з ажурним візерунком', 'центральний камінь оточений 6 дрібними', 'асиметрична компоновка', 'витягнута форма як крапля'. НЕ повторюй category/style/coating — це у всіх однакові." },
      physical_proportions: { type: ["string", "null"], description: "Пропорції форми: 'витягнуте по вертикалі', 'квадратне', 'широке низьке', 'компактне', 'довге тонке', 'круглі однакові' тощо. Дозволяє знайти схожі за формою." },
      description:        { type: "string", description: "ДЕТАЛЬНИЙ опис українською 5-7 речень. ЗАБОРОНЕНО: 'виготовлені з медичного золота', 'позолота 18К', 'стиль гламур', 'елегантні', 'стильні' — це у всіх товарів каталогу. ОБОВ'ЯЗКОВО: (1) конкретна форма і пропорції, (2) розташування і кількість елементів, (3) деталі поверхні і орнаменту, (4) камінь — де саме і як закріплений, (5) що відрізняє цей виріб від інших такого ж типу в каталозі, (6) випадок носіння. Уяви що описуєш товар клієнту по телефону — він має уявити саме ЦЕЙ виріб, а не схожий." },
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
      "unique_features", "physical_proportions",
      "description"
    ],
  };

  const body = {
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "Ти експерт ювелірного дизайну і аналітик каталогу для опт-магазину Xuping. " +
          "\n\n" +
          "КОНТЕКСТ КАТАЛОГУ: ВСІ товари тут — біжутерія Xuping з медичного золота, " +
          "з покриттям родій / позолота 18К / золото rose. Це у ВСІХ товарів. " +
          "Тому матеріал і покриття — НЕ диференціююча ознака. " +
          "\n\n" +
          "ТВОЯ ЗАДАЧА: витягти атрибути, які РОЗРІЗНЯЮТЬ цей товар від інших Xuping. " +
          "Ціль — система рекомендацій 'схоже за дизайном'. Якщо твій опис підходить " +
          "до тисячі товарів — він поганий. Опис має ідентифікувати ЦЕЙ конкретний виріб. " +
          "\n\n" +
          "ПРИНЦИПИ:\n" +
          "1. КОНКРЕТНІСТЬ: не 'квіти', а 'троянда з трьома пелюстками'. Не 'метелик', " +
          "а 'метелик з розкритими крилами і вусиками'. Не 'круглий', а 'круглий діаметром ~6мм'.\n" +
          "2. ВІЗУАЛЬНІ ДЕТАЛІ: розташування камена, симетрія, кількість елементів, " +
          "пропорції, орнаменти, текстура поверхні — це те що відрізняє.\n" +
          "3. ОПИС ЯК ОПИС ПО ТЕЛЕФОНУ: клієнт не бачить фото. По твоєму опису " +
          "він має уявити САМЕ ЦЕЙ виріб, а не схожий.\n" +
          "4. unique_features ОБОВ'ЯЗКОВО заповни 3-5 пунктами — це найкритичніше для пошуку.\n" +
          "5. Якщо назва товару має корисну інформацію (розмір 4мм, метелик, etc.) — " +
          "використовуй її разом з фото.\n" +
          "6. Якщо не можеш визначити — null або 'unknown'. Не вигадуй.\n" +
          "\n" +
          "ЗАБОРОНЕНІ ФРАЗИ в description (бо у всіх товарів однакові):\n" +
          "- 'виготовлені з медичного золота'\n" +
          "- 'позолота 18К' / 'покриття родій' (це є в attribute coating, в опис не дублюй)\n" +
          "- 'стильні', 'елегантні', 'красиві' — без конкретики\n" +
          "- 'класичний / гламурний стиль' — без пояснення чим саме\n" +
          "Замість них використовуй конкретні деталі форми, орнаменту, композиції.",
      },
      {
        role: "user",
        content: [
          {
            type: "text",
            text: "Назва товару з каталогу: " + (productName || "—") +
                  "\n\nПроаналізуй фото ювелірного виробу. Виокреми ВІЗУАЛЬНІ риси, " +
                  "які відрізняють його від інших товарів Xuping в каталозі. " +
                  "Поверни структурований JSON.",
          },
          // detail=high: модель бачить оригінал зображення (~340-765 tokens) замість
          // 85-tok downsample. Критично для дрібних деталей в ювелірці.
          { type: "image_url", image_url: { url: imageUrl, detail: "high" } },
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

  // 10) Описовий текст — найважливіша частина embedding (longest stretch of
  //     coherent text). Описує саме цей виріб без генеричних фраз.
  if (attrs.description) parts.push(attrs.description);

  return parts.join(". ");
}

module.exports = { analyzeProductImage, createEmbedding, buildEmbeddingText };
