// Документація показників аналітики клієнтів — ЄДИНЕ ДЖЕРЕЛО ПРАВДИ.
//
// Цей файл імпортується api/clients-docs.js і подається фронтенду як JSON
// для модалу довідки. Коли формула в lib/clients.js змінюється, відповідний
// запис ТУТ повинен бути оновлений у тому ж комміті. Це домовленість команди,
// не технічна перевірка — але доки і код в одному репо, рев'ю PR ловить
// розбіжності.
//
// Структура запису:
//   id        — короткий ключ (`recency`, `churn`, ...).
//   label     — назва в UI.
//   short     — однорядковий tooltip для column header / metric tile.
//   formula   — точна формула (SQL/математика), як ми її рахуємо.
//   period    — за який інтервал часу береться вхід.
//   freshness — як часто перераховується.
//   notes     — додаткові пояснення / винятки / краєві випадки.

const EXCLUDED_STATUSES = [
  "cancelled", "rejected", "canceled",
  "Повернули", "Відмовились",
  "incorrect_data", "underbid",
  "Об'єднання замовлень (LIKE)",
];

const METRIC_DOCS = {
  // ── Базові RFM ──────────────────────────────────────────
  recency: {
    id: "recency",
    label: "R · Recency (днів)",
    short: "Днів з останнього замовлення",
    formula: "CURRENT_DATE − MAX(ordered_at)",
    period: "Моментальний замір на сьогодні",
    freshness: "Перераховується щоночі в 03:15 UTC при оновленні buyer_rfm",
    notes: "Враховуються тільки не скасовані замовлення. Підсвічується червоним з ⚠, якщо перевищує цикл закупки в 1.5×.",
  },
  frequency: {
    id: "frequency",
    label: "F · Frequency",
    short: "Кількість замовлень",
    formula: "COUNT(DISTINCT order_id)",
    period: "За всю історію клієнта",
    freshness: "Щоночі",
    notes: "Враховуються тільки не скасовані замовлення.",
  },
  monetary: {
    id: "monetary",
    label: "M · Monetary (₴)",
    short: "Сума всіх замовлень",
    formula: "SUM(order_grand_total) по унікальних order_id",
    period: "За всю історію",
    freshness: "Щоночі",
    notes: "order_grand_total — це сума з урахуванням знижок на замовлення (KeyCRM поле grand_total). Для історичних замовлень, де grand_total ще не записано, використовується fallback: сума line-revenue без знижок.",
  },
  aov: {
    id: "aov",
    label: "AOV (Average Order Value)",
    short: "Середній чек",
    formula: "AVG(order_grand_total) по унікальних замовленнях клієнта = M / F",
    period: "За всю історію",
    freshness: "Щоночі",
  },
  aov_last_90d: {
    id: "aov_last_90d",
    label: "AOV 90 днів",
    short: "Середній чек за останні 90 днів",
    formula: "AVG(order_grand_total) WHERE ordered_at ≥ today − 90",
    period: "Останні 90 днів",
    freshness: "Щоночі",
  },

  // ── Інтервали і velocity ────────────────────────────────
  avg_interval: {
    id: "avg_interval",
    label: "Цикл закупки",
    short: "Середній інтервал між замовленнями (днів)",
    formula: "AVG(order_date − previous_order_date) по всіх послідовних парах замовлень клієнта",
    period: "За всю історію",
    freshness: "Щоночі",
    notes: "Якщо у клієнта лише 1 замовлення — цикл = null (немає з чого рахувати).",
  },
  recent_interval: {
    id: "recent_interval",
    label: "Інтервал · останні",
    short: "Середній інтервал серед останніх замовлень",
    formula: "AVG інтервалів, що ведуть до 3 ОСТАННІХ замовлень клієнта (тобто 2 крайніх інтервали)",
    period: "Не календарне вікно — порядкове. Береться з кінця історії клієнта.",
    freshness: "Щоночі",
    notes: "Якщо у клієнта менше 4 замовлень — null.",
  },
  prior_interval: {
    id: "prior_interval",
    label: "Інтервал · ранні",
    short: "Середній інтервал серед ранніх замовлень",
    formula: "AVG інтервалів, що НЕ входять в 'останні' (тобто всі крім 2 крайніх)",
    period: "Початок історії клієнта (від першого замовлення до 4-го з кінця)",
    freshness: "Щоночі",
    notes: "Якщо у клієнта менше 4 замовлень — null.",
  },
  velocity_trend: {
    id: "velocity_trend",
    label: "Velocity (динаміка)",
    short: "Чи прискорюється клієнт у замовленнях",
    formula:
      "1) Якщо |recent − prior| < 14 днів → stable (шум, без тренду). " +
      "2) Інакше ratio = recent / prior: <0.7 → accelerating; >1.4 → decelerating; між — stable.",
    period: "Порівняння всередині історії клієнта (без календарних вікон)",
    freshness: "Щоночі",
    notes:
      "Поправка на шум: природна варіація замовлень — кілька днів. Зміна 30д → 35д (різниця 5 днів) НЕ вважається сповільненням навіть якщо ratio формально 1.17. " +
      "Якщо recent або prior null — velocity null (недостатньо замовлень для аналізу).",
  },

  // ── Поведінкові показники ───────────────────────────────
  freq_last_90d: {
    id: "freq_last_90d",
    label: "Замовлень за 90 днів",
    short: "Кількість замовлень за останні 90 днів",
    formula: "COUNT(order_id) WHERE ordered_at ≥ today − 90",
    period: "Останні 90 днів",
    freshness: "Щоночі",
  },
  freq_prior_90d: {
    id: "freq_prior_90d",
    label: "Замовлень за попередні 90",
    short: "Замовлень за попередні 90 днів (від -180 до -90)",
    formula: "COUNT(order_id) WHERE ordered_at ≥ today − 180 AND ordered_at < today − 90",
    period: "Від 180 до 90 днів тому",
    freshness: "Щоночі",
    notes: "Вікно НЕ перетинається з freq_last_90d — рівні відрізки для чесного порівняння.",
  },
  categories_lifetime: {
    id: "categories_lifetime",
    label: "Категорії · всього",
    short: "Кількість різних категорій товару, які купував клієнт",
    formula: "COUNT(DISTINCT skus.category_id) по всіх замовленнях клієнта",
    period: "За всю історію",
    freshness: "Щоночі",
  },
  categories_90d: {
    id: "categories_90d",
    label: "Категорії · 90 днів",
    short: "Кількість категорій за останні 90 днів",
    formula: "COUNT(DISTINCT skus.category_id) WHERE ordered_at ≥ today − 90",
    period: "Останні 90 днів",
    freshness: "Щоночі",
  },

  // ── Скори і сегменти ────────────────────────────────────
  r_score: {
    id: "r_score",
    label: "R-score (квінтиль)",
    short: "Квінтиль рецентності (1–5, 5 = недавно)",
    formula: "6 − NTILE(5) OVER (ORDER BY recency_days) — менший recency дає вищий бал",
    period: "Відносне порівняння серед усіх опт-клієнтів",
    freshness: "Щоночі",
    notes: "Квінтилі ПЕРЕРАХОВУЮТЬСЯ щоночі — твій R-score може змінитись навіть якщо ти сам нічого не робив (хтось інший купив, перемішало рейтинг).",
  },
  f_score: {
    id: "f_score",
    label: "F-score (квінтиль)",
    short: "Квінтиль частоти (1–5, 5 = багато)",
    formula: "NTILE(5) OVER (ORDER BY frequency)",
    period: "Відносне порівняння серед усіх опт-клієнтів",
    freshness: "Щоночі",
  },
  m_score: {
    id: "m_score",
    label: "M-score (квінтиль)",
    short: "Квінтиль прибутковості (1–5, 5 = багато)",
    formula: "NTILE(5) OVER (ORDER BY monetary)",
    period: "Відносне порівняння серед усіх опт-клієнтів",
    freshness: "Щоночі",
  },
  rfm_code: {
    id: "rfm_code",
    label: "RFM-код",
    short: "Трьохзначний код R/F/M квінтилів",
    formula: "concat(r_score, f_score, m_score)",
    period: "Щоночі",
    notes: "555 = ідеальний клієнт; 155 = багато платив, часто, але давно пропав; 111 = найгірший.",
  },
  segment: {
    id: "segment",
    label: "Сегмент",
    short: "Auto-класифікація на основі R та F",
    formula:
      "Champions: R≥4 AND F≥4; Loyal: R≥3 AND F≥3; New: R≥4 AND F≤2; At-risk: R≤2 AND F≥3; Lost: R≤2 AND F≤2; Potential: інше",
    period: "Похідне від R/F-score, оновлюється щоночі",
    notes: "M-score не використовується для сегментації — тільки як окрема метрика.",
  },

  // ── Похідні і ризики ────────────────────────────────────
  overdue: {
    id: "overdue",
    label: "Overdue (прострочка)",
    short: "Клієнт реально прострочив свій цикл закупки",
    formula:
      "recency > avg_interval × 2  AND  (recency − avg_interval) > 30 днів. " +
      "ОБИДВА умови повинні виконатись.",
    period: "Моментальний замір",
    freshness: "Щоночі",
    notes:
      "Раніше було просто 1.5× — занадто чутливо до сезонних розтягувань (січневий провал в ювелірці нормальний). " +
      "Тепер вимагаємо одночасно 2× перевищення І мінімум +30 днів абсолютної затримки. " +
      "Приклад: цикл 30д, recency 55д (diff 25) — НЕ overdue; цикл 30д, recency 90д (ratio 3, diff 60) — OVERDUE. " +
      "Тільки для клієнтів з 2+ замовленнями (інакше цикл = null). Рядок підсвічується червоною смужкою зліва.",
  },
  ltv: {
    id: "ltv",
    label: "LTV (Lifetime Value)",
    short: "Сумарна виручка від клієнта",
    formula: "= monetary (M-метрика). Це одне й те саме число — суми всіх замовлень з урахуванням знижок.",
    period: "За всю історію",
    freshness: "Щоночі",
    notes:
      "У таблиці колонка називається 'LTV (₴)' — це і є monetary. Окремої колонки M більше немає (щоб не дублювати). " +
      "M-score (квінтиль 1-5) — це ВЖЕ окреме поняття, продовжує існувати в RFM-коді. " +
      "Зараз показуємо ІСТОРИЧНИЙ LTV. Прогнозний LTV (історичний + очікувана майбутня виручка з урахуванням churn) — TODO.",
  },
  churn_pct: {
    id: "churn_pct",
    label: "Churn (ризик відтоку)",
    short: "Ймовірність втрати клієнта в %",
    formula:
      "Зважена сума 5 факторів. Ваги підібрані так, що теоретичний максимум = 100%: " +
      "recency_3x (+45) + freq_drop_to_zero (+25) + aov_50%(+12) + categories(+8) + velocity(+10) = 100. " +
      "Cap 0–100%.",
    period: "Комбінує різні вікна — див. таблицю факторів",
    freshness: "Щоночі",
    notes:
      "Колірний код: 0–20% зелений, 20–40% сірий, 40–70% жовтий, 70–100% червоний. " +
      "Заметки звонків НЕ впливають на автоматичний churn-розрахунок — вони чистий CRM-журнал.",
  },
  churn_factors: {
    id: "churn_factors",
    label: "Фактори churn",
    short: "З чого складається churn-ризик",
    period: "Різні вікна — див. колонку 'period' в кожному рядку",
    factors: [
      { name: "Recency перевищує цикл у 1.2×",  weight: "+8%",  period: "моментально",       trigger: "recency / avg_interval ≥ 1.2 AND клієнт має 2+ замовлень" },
      { name: "Recency перевищує цикл у 1.5×",  weight: "+20%", period: "моментально",       trigger: "ratio ≥ 1.5" },
      { name: "Recency перевищує цикл у 2×",    weight: "+32%", period: "моментально",       trigger: "ratio ≥ 2" },
      { name: "Recency перевищує цикл у 3×",    weight: "+45%", period: "моментально",       trigger: "ratio ≥ 3" },
      { name: "90+ днів без замовлень (no history)", weight: "+20%", period: "моментально",  trigger: "тільки якщо у клієнта < 2 замовлень AND recency ≥ 90д" },
      { name: "180+ днів без замовлень (no history)", weight: "+35%", period: "моментально", trigger: "тільки якщо у клієнта < 2 замовлень AND recency ≥ 180д" },
      { name: "Частота впала на 50%+",         weight: "+10%", period: "90д vs попередні 90д", trigger: "freq_90 / freq_prior_90 ≤ 0.5 AND prior ≥ 2" },
      { name: "Частота впала на 70%+",         weight: "+18%", period: "90д vs попередні 90д", trigger: "ratio ≤ 0.3" },
      { name: "Частота впала до нуля",         weight: "+25%", period: "90д vs попередні 90д", trigger: "freq_90 = 0 AND prior ≥ 2" },
      { name: "AOV просів на 30%+",            weight: "+6%",  period: "90д vs всю історію", trigger: "aov_90 / aov ≤ 0.7" },
      { name: "AOV просів на 50%+",            weight: "+12%", period: "90д vs всю історію", trigger: "ratio ≤ 0.5" },
      { name: "Звузив асортимент категорій",   weight: "+8%",  period: "90д vs всю історію", trigger: "categories_90d < categories_lifetime × 0.4 AND lifetime ≥ 3 категорій" },
      { name: "Velocity сповільнюється",       weight: "+10%", period: "останні 3 інтервали vs ранні", trigger: "recent_interval / prior_interval > 1.3 AND (recent − prior) > 14 днів" },
    ],
  },

  // ── Профіль ──────────────────────────────────────────────
  is_wholesale: {
    id: "is_wholesale",
    label: "Опт-клієнт",
    short: "Чи помічений як опт у KeyCRM",
    formula: "custom_field з UUID='CT_1005' (поле 'Опт/Роздріб') містить значення 'Опт' в масиві",
    period: "Підтягується щоночі з ingest або вручну при натисканні чекбокса в карточці клієнта",
    notes: "Сторінка 'Опт-клієнти' завжди фільтрує is_wholesale=true. Для дебагу: ?show_all=1.",
  },
  status: {
    id: "status",
    label: "Статус клієнта",
    short: "Ручний CRM-статус (тёплий / VIP / спить / тощо)",
    formula: "buyers.status_id → buyer_statuses (довідник, редагується з UI)",
    period: "Не пов'язано з часом — ручна позначка",
    notes: "Налаштовується в карточці клієнта. Кнопкою '+' можна додати новий статус.",
  },
  notes: {
    id: "notes",
    label: "Заметки дзвінків",
    short: "Журнал контактів з клієнтом",
    formula: "buyer_notes: (id, buyer_id, created_at, outcome, body)",
    period: "Зберігаються назавжди в журналі клієнта",
    notes: "Журнал ручний — додаєш заметку прямо в карточці клієнта. На автоматичні метрики (churn, velocity, segment) НЕ впливає — це чистий CRM-журнал для твоїх записів про дзвінки.",
  },
};

const GENERAL_NOTES = {
  excluded_statuses: EXCLUDED_STATUSES,
  refresh_schedule: "buyer_rfm REFRESH MATERIALIZED VIEW виконується щоночі в 03:15 UTC через pg_cron. Перед цим о 03:00 щоденний ingest забирає нові замовлення з KeyCRM.",
  data_sources: {
    sales: "Рядки замовлень (line items) — основа всіх агрегатів",
    buyers: "Карточки клієнтів — ім'я/телефон/опт-флаг/статус",
    buyer_rfm: "Materialized view з усіма метриками; перебудовується щоночі",
    buyer_notes: "Журнал дзвінків — впливає на churn",
    skus: "Каталог товарів — звідки беремо category_id",
  },
};

module.exports = { METRIC_DOCS, GENERAL_NOTES };
