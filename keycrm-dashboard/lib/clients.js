// Общие хелперы для эндпоинтов /api/clients и /api/client.
// Расчёт сегмента, velocity-trend, churn_pct.

function classifySegment(r, f) {
  if (r >= 4 && f >= 4) return "champions";
  if (r >= 3 && f >= 3) return "loyal";
  if (r >= 4 && f <= 2) return "new";
  if (r <= 2 && f >= 3) return "at_risk";
  if (r <= 2 && f <= 2) return "lost";
  return "potential";
}

function classifyVelocity(recent, prior) {
  const r = recent == null ? null : parseFloat(recent);
  const p = prior == null ? null : parseFloat(prior);
  if (!r || !p || p <= 0) return null;
  // Поправка на шум: природна варіація замовлень — кілька днів. Якщо
  // абсолютна різниця < 14 днів — це не тренд, а похибка. Чекаємо мінімум
  // 2-тижневу зміну І значущого ratio, щоб сказати "прискорюється/сповільнюється".
  const absDiff = Math.abs(r - p);
  if (absDiff < 14) return "stable";
  const ratio = r / p;
  if (ratio < 0.7) return "accelerating";
  if (ratio > 1.4) return "decelerating";
  return "stable";
}

function computeChurnPct(row, _legacyNotesArg) {
  // _legacyNotesArg раніше містив кількість негативних заметок за 30 днів.
  // Більше не використовується (фактор прибрано на запит користувача) —
  // параметр зберігаю в сигнатурі для зворотньої сумісності з enrichRfmRow.
  let score = 0;
  const reasons = [];

  const recency = row.recency_days;
  const avgInt = row.avg_interval_days == null ? null : parseFloat(row.avg_interval_days);
  if (recency != null && avgInt && avgInt > 0) {
    const ratio = recency / avgInt;
    if (ratio >= 3)        { score += 45; reasons.push("recency_3x_interval"); }
    else if (ratio >= 2)   { score += 32; reasons.push("recency_2x_interval"); }
    else if (ratio >= 1.5) { score += 20; reasons.push("recency_1_5x_interval"); }
    else if (ratio >= 1.2) { score += 8;  reasons.push("recency_1_2x_interval"); }
  } else if (recency != null) {
    if (recency >= 180)      { score += 35; reasons.push("recency_180d_no_history"); }
    else if (recency >= 90)  { score += 20; reasons.push("recency_90d_no_history"); }
  }

  const f90 = row.freq_last_90d || 0;
  const fPrior = row.freq_prior_90d || 0;
  if (fPrior >= 2) {
    if (f90 === 0)               { score += 25; reasons.push("freq_dropped_to_zero"); }
    else if (f90 / fPrior <= 0.3){ score += 18; reasons.push("freq_dropped_70pct"); }
    else if (f90 / fPrior <= 0.5){ score += 10; reasons.push("freq_dropped_50pct"); }
  }

  const aov   = row.aov          == null ? null : parseFloat(row.aov);
  const aov90 = row.aov_last_90d == null ? null : parseFloat(row.aov_last_90d);
  if (aov && aov90 && aov > 0) {
    if (aov90 / aov <= 0.5)      { score += 12; reasons.push("aov_dropped_50pct"); }
    else if (aov90 / aov <= 0.7) { score += 6;  reasons.push("aov_dropped_30pct"); }
  }

  const catLife = row.categories_lifetime || 0;
  const cat90 = row.categories_90d || 0;
  if (catLife >= 3 && cat90 < catLife * 0.4) {
    score += 8; reasons.push("category_narrowing");
  }

  // Velocity-сповільнення: ratio > 1.3 АЛЕ також абсолютна різниця > 14 днів
  // (узгоджено з classifyVelocity, не караємо за шум).
  const recentInt = row.recent_interval_days == null ? null : parseFloat(row.recent_interval_days);
  const priorInt  = row.prior_interval_days  == null ? null : parseFloat(row.prior_interval_days);
  if (recentInt && priorInt && priorInt > 0) {
    const ratio = recentInt / priorInt;
    const absDiff = recentInt - priorInt;
    if (ratio > 1.3 && absDiff > 14) {
      score += 10; reasons.push("velocity_decelerating");
    }
  }

  // Нормуємо в діапазон 0–100%. Ваги факторів підібрані так, що теоретичний
  // max у клієнта з повною історією дорівнює саме 100%:
  //   recency_3x (+45) + freq_drop_to_zero (+25) + aov_50% (+12)
  //   + categories_narrow (+8) + velocity_decel (+10) = 100.
  // Cap на 100 — захист від випадкових збігів кількох мутуально-виключних
  // сигналів (recency_3x і recency_180d не можуть бути одночасно — обидва
  // в окремих гілках if/else; залишаємо як safety).
  if (score < 0) score = 0;
  if (score > 100) score = 100;
  return { pct: Math.round(score), reasons };
}

const NEGATIVE_OUTCOMES = new Set([
  "не відповідає", "не отвечает",
  "відмова", "отказ",
  "не цікаво зараз",
]);

function isNegativeOutcome(outcome) {
  if (!outcome) return false;
  return NEGATIVE_OUTCOMES.has(String(outcome).toLowerCase().trim());
}

function enrichRfmRow(rfm, opts) {
  // rfm — строка из buyer_rfm; opts: { negNotes30d: number }
  const negCount = (opts && opts.negNotes30d) || 0;
  if (!rfm) {
    return {
      segment: null, velocity_trend: null, overdue: false,
      churn_pct: 0, churn_reasons: [], avg_interval_days: null,
    };
  }
  const avgInt = rfm.avg_interval_days == null ? null : parseFloat(rfm.avg_interval_days);
  const recency = rfm.recency_days == null ? null : rfm.recency_days;
  // Overdue: одночасно (a) recency перевищує цикл у 2× і (b) абсолютна
  // затримка > 30 днів. Це дає простір для сезонних розтягувань (січневий
  // провал у ювелірці нормальний) і ловить лише реально пропалих клієнтів.
  const overdue = !!(
    avgInt && recency != null && avgInt > 0
    && recency > avgInt * 2
    && (recency - avgInt) > 30
  );
  const churn = computeChurnPct({
    recency_days: recency,
    avg_interval_days: avgInt,
    recent_interval_days: rfm.recent_interval_days,
    prior_interval_days: rfm.prior_interval_days,
    freq_last_90d: rfm.freq_last_90d,
    freq_prior_90d: rfm.freq_prior_90d,
    aov: rfm.aov, aov_last_90d: rfm.aov_last_90d,
    categories_lifetime: rfm.categories_lifetime,
    categories_90d: rfm.categories_90d,
  }, negCount);
  return {
    segment: rfm.r_score && rfm.f_score ? classifySegment(rfm.r_score, rfm.f_score) : null,
    velocity_trend: classifyVelocity(rfm.recent_interval_days, rfm.prior_interval_days),
    overdue,
    churn_pct: churn.pct,
    churn_reasons: churn.reasons,
    avg_interval_days: avgInt,
  };
}

module.exports = {
  classifySegment,
  classifyVelocity,
  computeChurnPct,
  enrichRfmRow,
  isNegativeOutcome,
  NEGATIVE_OUTCOMES,
};
