// Аналізує фото товарів через OpenAI Vision + створює embeddings.
// Викликається пакетно: GET /api/cron/analyze-images?secret=...&limit=50
//
// Для кожного SKU без image_analyzed_at:
//   1. Дістає image_url з skus (заповнюється під час ingest).
//      Якщо порожній — пробує дотягнути з KeyCRM API.
//   2. Викликає OpenAI gpt-4o-mini з vision і структурованим JSON Schema.
//   3. Зберігає visual_description, visual_tags, design_attributes в skus.
//   4. Створює text embedding через text-embedding-3-small → image_embedding.
//
// Throttle: 2 паралельних запити, ~1.5s між батчами → ~50 req/min до OpenAI.
// Time budget: 45s — Vercel function ліміт 60s, лишаємо запас на upsert.

const { getSupabase } = require("../../lib/supabase");
const { checkCronAuth } = require("../../lib/auth");
const { get: keycrmGet, sleep } = require("../../lib/keycrm");
const { analyzeProductImage, createEmbedding, buildEmbeddingText } = require("../../lib/openai");

const PARALLEL = 2;
const PER_BATCH_DELAY_MS = 1500;
const TIME_BUDGET_MS = 45 * 1000;

// Спробувати знайти повне фото товару. KeyCRM повертає різні поля для
// продуктів і офферів. Пріоритет: thumbnail_url → attachments_data[0] → null.
function pickImageUrl(p) {
  if (!p || typeof p !== "object") return null;
  if (p.thumbnail_url && String(p.thumbnail_url).startsWith("http")) return p.thumbnail_url;
  if (p.picture) {
    if (typeof p.picture === "string" && p.picture.startsWith("http")) return p.picture;
    if (p.picture.original) return p.picture.original;
    if (p.picture.thumbnail) return p.picture.thumbnail;
  }
  if (Array.isArray(p.attachments_data) && p.attachments_data.length) {
    const first = p.attachments_data[0];
    if (typeof first === "string") return first;
    if (first && (first.url || first.path)) return first.url || first.path;
  }
  return null;
}

// KeyCRM обгортає всі картинки в file-storage/remote?url=ENCODED proxy.
// OpenAI Vision іноді не може скачати через цей проксі. Розпаковуємо до
// прямого URL на джерело + пере-кодуємо проблемні символи у шляху
// (особливо + який часто є в іменах файлів та інтерпретується як пробіл).
function unwrapKeycrmImageUrl(url) {
  if (!url || typeof url !== "string") return url;
  const m = url.match(/[?&]url=([^&]+)/);
  if (m) {
    try {
      const decoded = decodeURIComponent(m[1]);
      if (decoded.startsWith("http")) {
        // ВАЖЛИВО: пере-кодуємо + в %2B у шляху URL. Деякі HTTP клієнти/
        // сервери інтерпретують бare + як пробіл (старий form-encoding
        // standard), що ламає завантаження файлу типу
        // "...12345_+abc.jpg" → сервер шукає "...12345_ abc.jpg" і не
        // знаходить. Цю проблему має OpenAI Vision fetcher.
        return decoded.replace(/\+/g, "%2B");
      }
    } catch (e) {
      // ignore — повернемо оригінал
    }
  }
  return url;
}

async function fetchImageUrlFromKeycrm(productId, apiKey, ctx) {
  if (!productId || !apiKey) return null;
  try {
    const resp = await keycrmGet("/products/" + productId, {}, apiKey, ctx);
    const product = (resp && resp.data) || resp;
    return pickImageUrl(product);
  } catch (e) {
    return null;
  }
}

async function analyzeOne(sku, apiKey, ctx) {
  let imageUrl = sku.image_url;

  // Якщо в нашій БД нема URL, пробуємо дотягнути з KeyCRM на льоту.
  if (!imageUrl) {
    imageUrl = await fetchImageUrlFromKeycrm(sku.product_id, apiKey, ctx);
    if (!imageUrl) {
      return {
        offer_id: sku.offer_id,
        ok: false,
        error: "no_image_url",
      };
    }
  }

  // Готуємо два варіанти URL для retry:
  // 1) Прямий URL з джерела (швидше, надійніше)
  // 2) Оригінальний KeyCRM proxy URL (fallback якщо джерело недоступне)
  const directUrl = unwrapKeycrmImageUrl(imageUrl);
  const candidateUrls = directUrl !== imageUrl ? [directUrl, imageUrl] : [directUrl];

  let attrs = null;
  let usedUrl = directUrl;
  let lastErr = null;
  for (const tryUrl of candidateUrls) {
    try {
      attrs = await analyzeProductImage(tryUrl, sku.name);
      usedUrl = tryUrl;
      break;
    } catch (err) {
      lastErr = err;
      const msg = (err && err.message) || String(err);
      // Якщо помилка не пов'язана з URL (rate limit, server error) —
      // не пробуємо інший URL, кидаємо одразу.
      if (!/invalid_image_url|Failed to download|Error while downloading/i.test(msg)) {
        throw err;
      }
      // Інакше переходимо до наступного URL у списку.
    }
  }
  if (!attrs) {
    return {
      offer_id: sku.offer_id,
      ok: false,
      image_url: imageUrl,
      error: (lastErr && lastErr.message) || "all image URLs failed",
    };
  }

  try {
    const embedText = buildEmbeddingText(attrs, sku.name);
    const embedding = await createEmbedding(embedText);
    // visual_description в БД = visual_summary (концентрований технічний опис).
    // visual_tags = similarity_keys + recommendation_vectors (для фільтрів в UI).
    const visualDesc = attrs.visual_summary
      || attrs.distinctive_description  // зворотна сумісність з попередньою схемою
      || attrs.description
      || embedText.substring(0, 500);
    const tagsArr = [
      ...(Array.isArray(attrs.similarity_keys) ? attrs.similarity_keys : []),
      ...(Array.isArray(attrs.recommendation_vectors) ? attrs.recommendation_vectors : []),
      ...(Array.isArray(attrs.tags) ? attrs.tags : []),  // зворотна сумісність
    ].slice(0, 30);
    return {
      offer_id: sku.offer_id,
      ok: true,
      image_url: usedUrl,  // зберігаємо саме той URL, який спрацював
      visual_description: visualDesc,
      visual_tags: tagsArr,
      design_attributes: attrs,
      image_embedding: embedding,
    };
  } catch (err) {
    return {
      offer_id: sku.offer_id,
      ok: false,
      image_url: imageUrl,
      error: (err && err.message) || String(err),
    };
  }
}

module.exports = async function handler(req, res) {
  const auth = checkCronAuth(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

  const supabase = getSupabase();
  const apiKey = process.env.KEYCRM_API_KEY;
  if (!process.env.OPENAI_API_KEY) {
    return res.status(500).json({ error: "OPENAI_API_KEY не налаштовано в Vercel env" });
  }

  const force = !!(req.query && req.query.force === "true");
  const limit = Math.max(1, Math.min(500, parseInt((req.query && req.query.limit) || "30")));
  const diag = !!(req.query && (req.query.diag === "1" || req.query.diag === "true"));

  // Діагностика — швидкий статус без виклику OpenAI
  if (diag) {
    const { data: progressData, error: progressErr } = await supabase.rpc("image_analysis_progress");
    if (progressErr) {
      return res.status(500).json({ error: "image_analysis_progress: " + progressErr.message });
    }
    return res.status(200).json({
      diag: true,
      progress: progressData && progressData[0],
      env: {
        OPENAI_API_KEY: process.env.OPENAI_API_KEY ? "present" : "missing",
        KEYCRM_API_KEY: process.env.KEYCRM_API_KEY ? "present" : "missing",
      },
    });
  }

  let runId = null;
  const ctx = { apiCalls: 0 };

  try {
    // Логуємо запуск
    const ins = await supabase
      .from("ingest_runs")
      .insert({ kind: "analyze_images", status: "running", meta: { limit, force } })
      .select("id").single();
    if (ins.error) throw new Error("ingest_runs insert: " + ins.error.message);
    runId = ins.data.id;

    // Вибираємо SKU для аналізу. Force=true → перетягуємо всіх активних з фото
    // (відсортовано по offer_id для детермінізму). Без force → тільки нові.
    let query = supabase
      .from("skus")
      .select("offer_id, product_id, sku, name, image_url, image_analyzed_at, image_analysis_error")
      .eq("is_active", true);
    if (!force) {
      query = query.is("image_analyzed_at", null);
    }
    query = query.order("offer_id", { ascending: true }).limit(limit);

    const targetsRes = await query;
    if (targetsRes.error) throw new Error("skus select: " + targetsRes.error.message);
    const targets = targetsRes.data || [];

    if (targets.length === 0) {
      await supabase.from("ingest_runs").update({
        finished_at: new Date().toISOString(), status: "ok",
        meta: { step: "analyze_images", processed: 0, message: "Нічого аналізувати" },
      }).eq("id", runId);
      return res.status(200).json({ ok: true, processed: 0, more: false, message: "Все вже проаналізовано" });
    }

    const startMs = Date.now();
    let processed = 0;
    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    for (let i = 0; i < targets.length; i += PARALLEL) {
      if (Date.now() - startMs > TIME_BUDGET_MS) break;

      const batch = targets.slice(i, i + PARALLEL);
      const results = await Promise.all(batch.map((sku) => analyzeOne(sku, apiKey, ctx)));

      // Готуємо оновлення для skus — окремо для ok і для помилок
      for (const r of results) {
        processed++;
        const nowIso = new Date().toISOString();
        if (r.ok) {
          successCount++;
          // pgvector embedding передається як рядок виду '[0.1,0.2,...]'
          const embeddingStr = "[" + r.image_embedding.join(",") + "]";
          const { error: updErr } = await supabase
            .from("skus")
            .update({
              image_url: r.image_url,
              visual_description: r.visual_description,
              visual_tags: r.visual_tags,
              design_attributes: r.design_attributes,
              image_embedding: embeddingStr,
              image_analyzed_at: nowIso,
              image_analysis_error: null,
            })
            .eq("offer_id", r.offer_id);
          if (updErr) {
            // Не валимо весь батч через один збій — лог і йдемо далі
            errorCount++;
            errors.push({ offer_id: r.offer_id, error: "update: " + updErr.message });
          }
        } else {
          errorCount++;
          errors.push({ offer_id: r.offer_id, error: r.error });
          // Записуємо тільки помилку, щоб не зациклитись на цьому SKU
          await supabase
            .from("skus")
            .update({
              image_analyzed_at: nowIso,
              image_analysis_error: (r.error || "").substring(0, 500),
            })
            .eq("offer_id", r.offer_id);
        }
      }
      await sleep(PER_BATCH_DELAY_MS);
    }

    // Лічильник лишку для прогрес-бару
    const { data: progressData } = await supabase.rpc("image_analysis_progress");
    const progress = progressData && progressData[0];

    await supabase.from("ingest_runs").update({
      finished_at: new Date().toISOString(),
      status: "ok",
      meta: {
        step: "analyze_images",
        processed,
        success: successCount,
        errors: errorCount,
        progress,
      },
    }).eq("id", runId);

    return res.status(200).json({
      ok: true,
      run_id: runId,
      processed,
      success: successCount,
      errors: errorCount,
      errors_sample: errors.slice(0, 5),
      progress,
      more: progress && progress.pending > 0,
    });
  } catch (err) {
    const msg = (err && err.message) || String(err);
    if (runId) {
      await supabase.from("ingest_runs").update({
        finished_at: new Date().toISOString(),
        status: "error",
        error_message: msg.substring(0, 500),
      }).eq("id", runId);
    }
    return res.status(500).json({ error: msg });
  }
};
