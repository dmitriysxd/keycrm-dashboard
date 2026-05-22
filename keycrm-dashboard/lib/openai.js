// Мінімальний OpenAI клієнт для аналізу зображень товарів і створення
// text embeddings. Без npm-залежностей — тільки fetch.
//
// Промпт і схема — продуктивна версія від chat-GPT для:
//   - structured visual analysis
//   - aesthetic decomposition
//   - similarity extraction
//   - recommendation intelligence
//   - product DNA generation

const BASE = "https://api.openai.com/v1";

function getApiKey() {
  const key = process.env.OPENAI_API_KEY;
  if (!key) throw new Error("OPENAI_API_KEY не налаштовано в env");
  return key;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

// Helper для побудови enum-фіелду з включенням null (для optional category-specific).
function enumWithNull(values) {
  return { type: ["string", "null"], enum: [...values, null] };
}
function enumStr(values) { return { type: "string", enum: values }; }
function arrEnum(values) {
  return { type: "array", items: { type: "string", enum: values } };
}

const SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    category: enumStr([
      "earrings", "chain", "bracelet", "ring", "pendant", "cross",
      "religious_pendant", "jewelry_set", "cuff", "brooch", "other",
    ]),
    subcategory: { type: ["string", "null"] },

    // ─── VISUAL IDENTITY ───────────────────────────────
    visual_identity: {
      type: "object", additionalProperties: false,
      properties: {
        primary_color: { type: "string" },
        secondary_colors: { type: "array", items: { type: "string" } },
        metal_tone: enumStr(["yellow_gold","rose_gold","white_gold","silver","platinum_style","black_metal","mixed_metal"]),
        finish: arrEnum(["mirror_polished","glossy","matte","brushed","satin","textured","hammered","oxidized"]),
        reflectivity: enumStr(["low","medium","high"]),
        luxury_level: enumStr(["budget_fashion","mass_market","premium_mass_market","luxury_style"]),
        visual_weight: enumStr(["ultra_light","light","medium","bold","heavy"]),
        dominant_material_appearance: { type: "string" },
      },
      required: ["primary_color","secondary_colors","metal_tone","finish","reflectivity","luxury_level","visual_weight","dominant_material_appearance"],
    },

    // ─── GEOMETRY ───────────────────────────────────────
    geometry: {
      type: "object", additionalProperties: false,
      properties: {
        primary_shape: arrEnum([
          // базові геометричні
          "round","oval","square","rounded_square","rectangle","teardrop","marquise","heart","cross","shield","star","infinity","elongated",
          // органічні / тематичні мотиви (важливо для нашого каталогу Xuping)
          "floral","leaf","butterfly","bird","animal","feather","bow",
          "crown","key","arrow","lock","letter","numeral","religious_symbol","celestial","zodiac_symbol",
          // абстрактні / структурні
          "geometric","abstract","braided","knot","spiral","layered","chain_based","medallion","icon_frame","symbolic",
        ]),
        secondary_shapes: { type: "array", items: { type: "string" } },
        line_style: arrEnum(["smooth","sharp","angular","flowing","interwoven","braided","twisted","layered","architectural","soft_curves","rigid","symmetrical_lines","organic"]),
        symmetry: enumStr(["perfect","near_perfect","asymmetrical"]),
        curvature: enumStr(["straight","soft_curved","highly_curved","mixed"]),
        openness: enumStr(["solid","semi_open","openwork","hollow_structure"]),
        dimensionality: enumStr(["flat","semi_3d","fully_3d"]),
        silhouette: arrEnum(["compact","elongated","airy","dense","symmetrical","flowing","rigid","statement","minimal","layered"]),
      },
      required: ["primary_shape","secondary_shapes","line_style","symmetry","curvature","openness","dimensionality","silhouette"],
    },

    // ─── STRUCTURE ───────────────────────────────────────
    structure: {
      type: "object", additionalProperties: false,
      properties: {
        construction_type: arrEnum(["hoop","stud","dangling","suspended","interlocked","woven","linked","articulated","rigid","layered","continuous","chain_based","framed","medallion_style","relief_style","cuff","tennis_style"]),
        movement_type: enumStr(["static","flexible","dangling","articulated"]),
        layering: enumStr(["single_layer","double_layer","multi_layer"]),
        center_focus: enumStr(["silhouette_based","stone_based","symbol_based","ornament_based","texture_based"]),
        attachment_style: { type: "string" },
        edge_style: arrEnum(["rounded","sharp","beveled","soft"]),
      },
      required: ["construction_type","movement_type","layering","center_focus","attachment_style","edge_style"],
    },

    // ─── SURFACE ─────────────────────────────────────────
    surface: {
      type: "object", additionalProperties: false,
      properties: {
        texture: arrEnum(["smooth_surface","braided_texture","rope_texture","hammered_texture","brushed_texture","lattice_texture","engraved_texture","reflective_surface"]),
        ornamentation: arrEnum(["greek_key","filigree","pave_line","cutout","lattice","floral_pattern","geometric_pattern","religious_symbolism","braided_texture","ornamental_frame","relief_details","vintage_ornament","modern_minimal","symbolic_detail"]),
        engraving_style: arrEnum(["none","minimal","decorative","ornate","relief_based"]),
        micro_details_level: enumStr(["minimal","moderate","rich","highly_detailed"]),
        surface_complexity: enumStr(["clean","moderate","complex","intricate"]),
      },
      required: ["texture","ornamentation","engraving_style","micro_details_level","surface_complexity"],
    },

    // ─── STONES ──────────────────────────────────────────
    stones: {
      type: "object", additionalProperties: false,
      properties: {
        has_stones: { type: "boolean" },
        stone_style: arrEnum(["pave","micro_pave","solitaire","channel_set","scattered","embedded","crystal_line","halo","centerpiece_stone","decorative_stones"]),
        stone_density: enumStr(["none","low","medium","high","full_coverage"]),
        stone_placement: arrEnum(["border","center","scattered","symmetrical","frame","accent_edges","full_surface"]),
        stone_role: enumStr(["none","dominant","decorative","accent_only","centerpiece"]),
        contrast_level: enumStr(["low","medium","high"]),
      },
      required: ["has_stones","stone_style","stone_density","stone_placement","stone_role","contrast_level"],
    },

    // ─── STYLE ───────────────────────────────────────────
    style: {
      type: "object", additionalProperties: false,
      properties: {
        core_style: arrEnum(["minimalist","classic","modern_classic","luxury","romantic","geometric","bold","elegant","vintage","fashion_forward","architectural","feminine","masculine","soft_luxury","statement","timeless","trendy","spiritual","symbolic","delicate","expressive"]),
        fashion_aesthetic: arrEnum(["clean_luxury","soft_feminine","geometric_luxury","modern_minimal","romantic_classic","bold_statement","vintage_luxury","fashion_glam","symbolic_elegance","everyday_luxury"]),
        emotional_signal: arrEnum(["soft","confident","elegant","luxurious","youthful","mature","artistic","glamorous","restrained","expressive","spiritual","powerful","calm"]),
        target_impression: arrEnum(["subtle","refined","expensive_look","fashionable","timeless","eye_catching","elegant_daily","premium_style"]),
        trend_alignment: enumStr(["timeless","trend_driven","hybrid"]),
      },
      required: ["core_style","fashion_aesthetic","emotional_signal","target_impression","trend_alignment"],
    },

    // ─── FASHION POSITIONING ────────────────────────────
    fashion_positioning: {
      type: "object", additionalProperties: false,
      properties: {
        occasion: arrEnum(["everyday","office","evening","event","party","romantic","formal","casual_luxury","spiritual","ceremonial"]),
        daily_wear_score: enumStr(["low","medium","high"]),
        statement_level: enumStr(["subtle","balanced","attention_grabbing","statement_piece"]),
        versatility: enumStr(["narrow","moderate","versatile","highly_versatile"]),
        age_style_bias: arrEnum(["youthful","universal","mature","fashion_oriented"]),
      },
      required: ["occasion","daily_wear_score","statement_level","versatility","age_style_bias"],
    },

    // ─── WEARABILITY ─────────────────────────────────────
    wearability: {
      type: "object", additionalProperties: false,
      properties: {
        visual_size_impression: enumStr(["tiny","small","medium","large","oversized"]),
        comfort_visual_estimate: enumStr(["lightweight","moderate","visually_heavy"]),
        pairing_compatibility: arrEnum(["minimal_jewelry","elegant_sets","layered_styling","statement_styling","classic_combinations","modern_fashion"]),
        styling_flexibility: enumStr(["low","medium","high"]),
      },
      required: ["visual_size_impression","comfort_visual_estimate","pairing_compatibility","styling_flexibility"],
    },

    // ─── COMPLEXITY ──────────────────────────────────────
    complexity: {
      type: "object", additionalProperties: false,
      properties: {
        design_complexity: enumStr(["minimal","moderate","advanced","highly_complex"]),
        visual_density: enumStr(["airy","balanced","dense"]),
        uniqueness_level: enumStr(["generic","recognizable","distinctive","highly_distinctive"]),
        silhouette_recognition_strength: enumStr(["weak","medium","strong","iconic"]),
      },
      required: ["design_complexity","visual_density","uniqueness_level","silhouette_recognition_strength"],
    },

    // ─── CATEGORY-SPECIFIC ATTRIBUTES ───────────────────
    // Заповнюємо тільки поля, що відповідають category. Решта = null.
    category_specific_attributes: {
      type: "object", additionalProperties: false,
      properties: {
        // EARRINGS
        earring_type: enumWithNull(["stud_earrings","hoop_earrings","huggie_earrings","drop_earrings","chandelier_earrings","geometric_earrings","cuff_earrings","threader_earrings"]),
        ear_fit: enumWithNull(["close_to_ear","semi_drop","hanging","elongated_drop"]),
        drop_length: enumWithNull(["none","short","medium","long"]),
        earring_movement_behavior: enumWithNull(["static","subtle_movement","fluid_movement","expressive_movement"]),
        ear_presence: enumWithNull(["delicate","balanced","visually_noticeable","dominant"]),
        face_framing_effect: enumWithNull(["softening","elongating","balancing","statement_framing"]),
        // CHAINS
        weave_type: enumWithNull(["snake","cuban","rope","figaro","singapore","box","anchor","cable","wheat","curb","bead_chain","layered_chain"]),
        chain_thickness: enumWithNull(["ultra_thin","thin","medium","thick","chunky"]),
        flow_behavior: enumWithNull(["fluid","semi_rigid","rigid"]),
        shine_behavior: enumWithNull(["subtle_shine","reflective","high_reflection"]),
        chain_density: enumWithNull(["airy","balanced","compact","heavy"]),
        chain_character: enumWithNull(["delicate","classic","bold","fashion_forward","luxury_style"]),
        // BRACELETS
        bracelet_type: enumWithNull(["chain_bracelet","tennis_bracelet","cuff_bracelet","charm_bracelet","rigid_bracelet","layered_bracelet"]),
        wrist_presence: enumWithNull(["delicate","balanced","statement"]),
        bracelet_movement_behavior: enumWithNull(["static","flexible","dangling","articulated"]),
        stacking_potential: enumWithNull(["low","medium","high"]),
        bracelet_structure: enumWithNull(["soft_flexible","semi_rigid","rigid"]),
        // CROSSES
        cross_style: enumWithNull(["orthodox","catholic","minimalist_cross","decorative_cross","luxury_cross","modern_cross"]),
        cross_symbolic_intensity: enumWithNull(["subtle","balanced","strong"]),
        cross_ornateness: enumWithNull(["plain","decorated","ornate","highly_ornamental"]),
        religious_aesthetic: enumWithNull(["spiritual","ceremonial","fashion_religious","luxury_spiritual"]),
        cross_proportions: enumWithNull(["compact","elongated","wide"]),
        // RELIGIOUS PENDANTS / ICONS
        icon_type: enumWithNull(["saint","virgin_mary","christ","angel","prayer_symbol","orthodox_icon"]),
        frame_style: enumWithNull(["minimal_frame","decorative_frame","ornate_frame","vintage_frame"]),
        relief_depth: enumWithNull(["flat","embossed","deeply_embossed"]),
        symbolic_focus: enumWithNull(["spiritual","protective","ceremonial","heritage"]),
        heritage_style: enumWithNull(["traditional","modernized","vintage","ceremonial"]),
        // PENDANTS
        pendant_focus: enumWithNull(["geometric","symbolic","stone_based","silhouette_based","abstract","romantic","celestial"]),
        symbolic_style: enumWithNull(["zodiac","spiritual","romantic","abstract","minimal_symbol","fashion_symbol"]),
        visual_center_strength: enumWithNull(["subtle","balanced","dominant"]),
        pendant_character: enumWithNull(["delicate","elegant","bold","statement","expressive"]),
        neck_presence: enumWithNull(["subtle","balanced","attention_grabbing"]),
        // SETS
        set_harmony: enumWithNull(["perfectly_matched","softly_coordinated","contrast_based"]),
        dominant_piece: enumWithNull(["earrings_dominant","necklace_dominant","bracelet_dominant","balanced_set"]),
        style_consistency: enumWithNull(["strict","moderate","flexible"]),
        coordination_level: enumWithNull(["minimal","balanced","highly_coordinated"]),
        set_character: enumWithNull(["elegant","luxurious","fashion_forward","timeless","statement"]),
      },
      required: [
        "earring_type","ear_fit","drop_length","earring_movement_behavior","ear_presence","face_framing_effect",
        "weave_type","chain_thickness","flow_behavior","shine_behavior","chain_density","chain_character",
        "bracelet_type","wrist_presence","bracelet_movement_behavior","stacking_potential","bracelet_structure",
        "cross_style","cross_symbolic_intensity","cross_ornateness","religious_aesthetic","cross_proportions",
        "icon_type","frame_style","relief_depth","symbolic_focus","heritage_style",
        "pendant_focus","symbolic_style","visual_center_strength","pendant_character","neck_presence",
        "set_harmony","dominant_piece","style_consistency","coordination_level","set_character",
      ],
    },

    // ─── SIMILARITY / RECOMMENDATION / SEARCH ───────────
    similarity_keys: {
      type: "array",
      items: { type: "string" },
      description: "HIGH VALUE similarity tags combining shape+style+ornament+structure+aesthetics. Examples: 'rose_gold_geometric', 'openwork_square', 'soft_luxury_style', 'micro_pave_accents', 'architectural_earrings', 'braided_curves'.",
    },
    recommendation_vectors: {
      type: "array",
      items: { type: "string" },
      description: "Abstract preference vectors. Examples: 'customers_who_like_soft_luxury', 'customers_who_prefer_geometric_design', 'customers_who_buy_delicate_jewelry'.",
    },
    search_keywords: {
      type: "array",
      items: { type: "string" },
      description: "Normalized search keywords (no marketing language).",
    },
    visual_summary: {
      type: "string",
      description: "ONE SHORT TECHNICAL summary. Example: 'Rose gold geometric hoop earrings with openwork structure, micro pave accents, polished reflective finish, and soft luxury aesthetic.' NO MARKETING.",
    },
  },
  required: [
    "category","subcategory","visual_identity","geometry","structure","surface",
    "stones","style","fashion_positioning","wearability","complexity",
    "category_specific_attributes","similarity_keys","recommendation_vectors",
    "search_keywords","visual_summary",
  ],
};

const SYSTEM_PROMPT = `YOU ARE A PROFESSIONAL JEWELRY VISUAL ANALYSIS ENGINE.

YOUR TASK IS NOT MARKETING. YOUR TASK IS:
- structured visual analysis
- aesthetic decomposition
- similarity extraction
- recommendation intelligence
- product DNA generation

The goal is HIGHLY CONSISTENT product descriptions for:
- recommendation systems / embeddings / vector similarity
- customer preference analysis / clustering
- AI search / "similar products" / visual matching

GLOBAL RULES:
1. NEVER invent attributes. If unclear from image — pick the closest enum value.
2. NEVER write marketing text.
3. NEVER use random synonyms.
4. ALWAYS use normalized vocabulary from predefined enum lists.
5. CONSISTENCY > CREATIVITY. Similar products MUST receive similar tags.
6. Small visual differences MUST be reflected in attributes.

ANALYZE BOTH:
1. OBJECTIVE FEATURES: shape, geometry, structure, stones, polish, symmetry, silhouette, openness, texture.
2. AESTHETIC SIGNALS: elegant, soft, bold, trendy, luxurious, feminine, architectural, timeless, expressive.

UNDERSTAND:
- WHY customers buy this product
- WHAT visual language it belongs to
- WHAT products FEEL similar
- WHAT emotional aesthetic it communicates

CATEGORY-SPECIFIC ATTRIBUTES:
Fill ONLY fields matching the category. ALL others MUST be null.
- earrings: earring_type, ear_fit, drop_length, earring_movement_behavior, ear_presence, face_framing_effect
- chain: weave_type, chain_thickness, flow_behavior, shine_behavior, chain_density, chain_character
- bracelet: bracelet_type, wrist_presence, bracelet_movement_behavior, stacking_potential, bracelet_structure
- cross: cross_style, cross_symbolic_intensity, cross_ornateness, religious_aesthetic, cross_proportions
- religious_pendant: icon_type, frame_style, relief_depth, symbolic_focus, heritage_style
- pendant: pendant_focus, symbolic_style, visual_center_strength, pendant_character, neck_presence
- jewelry_set: set_harmony, dominant_piece, style_consistency, coordination_level, set_character
- ring / cuff / brooch / other: keep all category-specific = null

SIMILARITY KEYS: generate 5-10 high-value tags combining shape + style + ornament + structure + aesthetics.
RECOMMENDATION VECTORS: generate 3-7 abstract preference clusters.
SEARCH KEYWORDS: 5-15 normalized terms.
VISUAL SUMMARY: ONE technical sentence (under 200 chars).

CRITICAL DISTINCTIONS the system must capture:
- soft curves vs angular geometry
- openwork vs solid
- delicate vs bold
- minimal vs intricate
- classic vs trendy
- spiritual vs fashion-oriented
- decorative stones vs centerpiece stones
- lightweight vs visually heavy
- feminine vs architectural
- timeless vs trend-driven

PRODUCT NAME IS A STRONG CONTEXT SIGNAL:
The catalog uses Ukrainian terms in product names. ALWAYS use them to inform primary_shape:
- "метелик" / "метелики" → primary_shape MUST include "butterfly"
- "корона" / "корони" / "крона" → primary_shape MUST include "crown"
- "квітка" / "квіти" / "роза" / "лілія" → primary_shape MUST include "floral" (or "leaf" for leaves)
- "серце" / "сердечко" → primary_shape MUST include "heart"
- "хрест" / "хрестик" → primary_shape MUST include "cross"
- "птах" / "пташка" / "сова" / "лебідь" → primary_shape MUST include "bird"
- "звір" / "тварина" / "кіт" / "лев" → primary_shape MUST include "animal"
- "зірка" / "зірочка" / "зорі" → primary_shape MUST include "star"
- "ангел" / "ікона" / "святий" → primary_shape MUST include "religious_symbol"
- "знак нескінченності" / "infinity" → "infinity"
- "ключ" → "key"
- "стрілка" → "arrow"
- "бантик" / "бант" → "bow"
- "перо" / "пір'я" → "feather"
- "лист" / "листочок" → "leaf"
- "цвяшки" / "пусети" → це earring_type=stud_earrings, але primary_shape визначай за зображенням (часто round/circular або симолічна форма)
- "кільця" (про сережки) → primary_shape "round" + earring_type=hoop_earrings
- "ладанка" / "медальйон" → "medallion"
- "ажур" → openness="openwork"

If product name explicitly says метелик/корона/etc — that motif IS the primary shape. Don't fall back to "floral" because it looks decorative. Use the exact motif.

The goal is NOT to describe products beautifully.
The goal is to create stable embeddings, strong similarity matching, accurate recommendations,
aesthetic clustering, customer taste profiling, visual intelligence, product DNA analysis.`;

async function analyzeProductImage(imageUrl, productName) {
  const body = {
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: SYSTEM_PROMPT },
      {
        role: "user",
        content: [
          {
            type: "text",
            text: `Product name from catalog: "${productName || "—"}"\n\nAnalyze the image of this jewelry product. Return ONLY valid structured JSON per the schema. Use the product name as additional context.`,
          },
          { type: "image_url", image_url: { url: imageUrl, detail: "low" } },
        ],
      },
    ],
    response_format: {
      type: "json_schema",
      json_schema: { name: "jewelry_dna", strict: true, schema: SCHEMA },
    },
    temperature: 0,  // максимальна детермінованість для consistency
    max_tokens: 3000,
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

// Build embedding text з нової структури.
// Стратегія: similarity_keys + recommendation_vectors — найвищий вага
// (вони спеціально створені для similarity matching), потім visual_summary,
// потім key структуровані атрибути. Без noise від матеріалів/бренду
// (медичне золото Xuping однакове у всіх → не диференціює).
function buildEmbeddingText(attrs, productName) {
  if (!attrs || typeof attrs !== "object") return productName || "";
  const parts = [];

  // 1) Category — базовий тип
  if (attrs.category) parts.push(attrs.category);
  if (attrs.subcategory) parts.push(attrs.subcategory);

  // 2) SIMILARITY KEYS — спеціально дизайнерські для similarity matching.
  //    Дублюємо для подвоєної ваги в embedding.
  if (Array.isArray(attrs.similarity_keys) && attrs.similarity_keys.length) {
    const keys = attrs.similarity_keys.join(" ");
    parts.push("similarity: " + keys);
    parts.push("design DNA: " + keys);
  }

  // 3) RECOMMENDATION VECTORS — preference clusters
  if (Array.isArray(attrs.recommendation_vectors) && attrs.recommendation_vectors.length) {
    parts.push("preferences: " + attrs.recommendation_vectors.join(", "));
  }

  // 4) VISUAL SUMMARY — concise technical sentence
  if (attrs.visual_summary) parts.push(attrs.visual_summary);

  // 5) Geometry — форма і силует, диференціатори №1
  if (attrs.geometry) {
    const g = attrs.geometry;
    const geo = [
      Array.isArray(g.primary_shape) && g.primary_shape.length && ("shape: " + g.primary_shape.join("/")),
      Array.isArray(g.line_style) && g.line_style.length && ("lines: " + g.line_style.join("/")),
      g.openness && ("openness: " + g.openness),
      g.dimensionality && ("dim: " + g.dimensionality),
      g.symmetry && (g.symmetry),
      g.curvature && (g.curvature + " curves"),
      Array.isArray(g.silhouette) && g.silhouette.length && ("silhouette: " + g.silhouette.join("/")),
    ].filter(Boolean).join(", ");
    if (geo) parts.push(geo);
  }

  // 6) Structure
  if (attrs.structure) {
    const s = attrs.structure;
    const struct = [
      Array.isArray(s.construction_type) && s.construction_type.length && ("construction: " + s.construction_type.join("/")),
      s.movement_type && (s.movement_type + " movement"),
      s.layering,
      s.center_focus,
    ].filter(Boolean).join(", ");
    if (struct) parts.push(struct);
  }

  // 7) Surface
  if (attrs.surface) {
    const sf = attrs.surface;
    const surf = [
      Array.isArray(sf.texture) && sf.texture.length && ("texture: " + sf.texture.join("/")),
      Array.isArray(sf.ornamentation) && sf.ornamentation.length && ("ornaments: " + sf.ornamentation.join("/")),
      sf.surface_complexity && (sf.surface_complexity + " complexity"),
      sf.micro_details_level && (sf.micro_details_level + " details"),
    ].filter(Boolean).join(", ");
    if (surf) parts.push(surf);
  }

  // 8) Stones
  if (attrs.stones && attrs.stones.has_stones) {
    const st = attrs.stones;
    const stoneInfo = [
      "with stones",
      Array.isArray(st.stone_style) && st.stone_style.length && ("style: " + st.stone_style.join("/")),
      st.stone_density && (st.stone_density + " density"),
      Array.isArray(st.stone_placement) && st.stone_placement.length && ("placement: " + st.stone_placement.join("/")),
      st.stone_role && (st.stone_role + " role"),
      st.contrast_level && (st.contrast_level + " contrast"),
    ].filter(Boolean).join(", ");
    parts.push(stoneInfo);
  } else if (attrs.stones && attrs.stones.has_stones === false) {
    parts.push("no stones");
  }

  // 9) Style + Aesthetic
  if (attrs.style) {
    const sv = attrs.style;
    const styleInfo = [
      Array.isArray(sv.core_style) && sv.core_style.length && ("style: " + sv.core_style.join("/")),
      Array.isArray(sv.fashion_aesthetic) && sv.fashion_aesthetic.length && ("aesthetic: " + sv.fashion_aesthetic.join("/")),
      Array.isArray(sv.emotional_signal) && sv.emotional_signal.length && ("emotion: " + sv.emotional_signal.join("/")),
      Array.isArray(sv.target_impression) && sv.target_impression.length && ("impression: " + sv.target_impression.join("/")),
      sv.trend_alignment,
    ].filter(Boolean).join(", ");
    if (styleInfo) parts.push(styleInfo);
  }

  // 10) Fashion positioning
  if (attrs.fashion_positioning) {
    const fp = attrs.fashion_positioning;
    const fashionInfo = [
      Array.isArray(fp.occasion) && fp.occasion.length && ("for: " + fp.occasion.join("/")),
      fp.statement_level,
      fp.versatility,
      Array.isArray(fp.age_style_bias) && fp.age_style_bias.length && (fp.age_style_bias.join("/")),
    ].filter(Boolean).join(", ");
    if (fashionInfo) parts.push(fashionInfo);
  }

  // 11) Visual identity (metal_tone і finish — НЕ генерик для нашого каталогу,
  //     бо у нас різні покриття: rhodium/gold18k/rose)
  if (attrs.visual_identity) {
    const vi = attrs.visual_identity;
    const visIds = [
      vi.metal_tone,
      Array.isArray(vi.finish) && vi.finish.length && (vi.finish.join("/")),
      vi.reflectivity && (vi.reflectivity + " reflectivity"),
      vi.visual_weight && (vi.visual_weight + " weight"),
    ].filter(Boolean).join(", ");
    if (visIds) parts.push(visIds);
  }

  // 12) Category-specific attributes — тільки заповнені (non-null)
  if (attrs.category_specific_attributes) {
    const cs = attrs.category_specific_attributes;
    const csValues = Object.entries(cs)
      .filter(([_, v]) => v !== null && v !== undefined && v !== "")
      .map(([k, v]) => k.replace(/_/g, " ") + ": " + v);
    if (csValues.length) parts.push(csValues.join(", "));
  }

  // 13) Complexity і uniqueness
  if (attrs.complexity) {
    const cx = attrs.complexity;
    const cxInfo = [
      cx.design_complexity && (cx.design_complexity + " design"),
      cx.visual_density,
      cx.uniqueness_level,
      cx.silhouette_recognition_strength && (cx.silhouette_recognition_strength + " silhouette"),
    ].filter(Boolean).join(", ");
    if (cxInfo) parts.push(cxInfo);
  }

  // 14) Search keywords — last, як додатковий tail
  if (Array.isArray(attrs.search_keywords) && attrs.search_keywords.length) {
    parts.push(attrs.search_keywords.join(", "));
  }

  return parts.join(". ");
}

module.exports = { analyzeProductImage, createEmbedding, buildEmbeddingText };
