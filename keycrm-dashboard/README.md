# KEYCRM Analytics Dashboard

Дашборд для KEYCRM с двумя слоями данных:
1. **Live-страницы** (Огляд / Товари / Замовлення) — агрегаты в реальном времени из KEYCRM API.
2. **SKU-аналітика + Реордер** — собственная история (суточные снапшоты остатков + продажи), хранится в Supabase, считаются velocity / days-of-supply / sell-through, статусы хіт/повільний/мертвий/новий.

Разворачивается на Vercel + Supabase (оба free tier).

## Быстрый старт

### Шаг 1 — Загрузи проект на GitHub

1. Зайди на [github.com](https://github.com) → войди или зарегистрируйся (бесплатно)
2. Нажми **"New repository"**
3. Назови: `keycrm-dashboard` → **Create repository**
4. Нажми **"uploading an existing file"**
5. Перетащи ВСЕ файлы из этой папки (index.html, vercel.json, папку api/)
6. Нажми **"Commit changes"**

### Шаг 2 — Разверни на Vercel

1. Зайди на [vercel.com](https://vercel.com) → войди через GitHub
2. Нажми **"Add New Project"**
3. Выбери репозиторий `keycrm-dashboard` → **Import**
4. Нажми **Deploy** (ничего не меняй)
5. Через 30 секунд получишь ссылку вида `keycrm-dashboard-xxx.vercel.app`

### Шаг 3 — Добавь переменные окружения

В Vercel: **Settings → Environment Variables**. Добавь все:

| Name | Где взять |
|---|---|
| `KEYCRM_API_KEY` | KEYCRM → Settings → API |
| `SUPABASE_URL` | Supabase → Settings → API → Project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase → Settings → API → service_role (secret) |
| `CRON_SECRET` | случайная строка, `openssl rand -hex 32` |
| `DASHBOARD_TOKEN` | случайная строка для входа на дашборд |
| `ALERT_WEBHOOK_URL` (опц.) | Telegram/Discord webhook на падения cron |

После сохранения — **Deployments → последний → Redeploy**.

### Шаг 4 — Подними Supabase

См. подробную инструкцию в [`supabase/README.md`](supabase/README.md). Кратко:
1. Создай Supabase-проект.
2. В SQL Editor прогони `supabase/migrations/001_init.sql`, потом `002_sku_metrics.sql`.
3. Скопируй URL + service_role key в Vercel.
4. Запусти бэкфилл 90 дней и первый ингест curl-ом (команды в `supabase/README.md`).

### Шаг 5 — Открой дашборд

```
https://keycrm-dashboard-xxx.vercel.app/?key=ЗНАЧЕНИЕ_DASHBOARD_TOKEN
```

Токен сохранится в localStorage. Добавь в закладки браузера или на главный экран телефона.

---

## Структура файлов

```
keycrm-dashboard/
├── index.html              # Фронтенд (live-страницы + SKU-аналитика + Реордер)
├── vercel.json             # Vercel: maxDuration функций + Cron schedule
├── package.json            # Зависимость: @supabase/supabase-js
├── api/
│   ├── data.js             # Live-агрегаты из KEYCRM (Огляд/Товари/Замовлення)
│   ├── sku.js              # Read API из Supabase (SKU-аналитика, Реордер)
│   └── cron/
│       ├── ingest.js       # Daily Cron: tэнет KEYCRM → Supabase, refresh sku_metrics
│       └── backfill.js     # Ручной бэкфилл истории заказов (до 31 дня за вызов)
├── lib/
│   ├── supabase.js         # Клиент Supabase (service role)
│   ├── keycrm.js           # KeyCRM HTTP с rate-limit и retry
│   └── auth.js             # Token-гарды для cron и dashboard
└── supabase/
    ├── migrations/
    │   ├── 001_init.sql    # skus, stock_snapshots, sales, ingest_runs
    │   └── 002_sku_metrics.sql  # MATERIALIZED VIEW + refresh function
    └── README.md           # Инструкция по setup БД и бэкфиллу
```

## Автообновление

Дашборд автоматически обновляет данные каждые 30 минут.
Также можно нажать кнопку **"Оновити"** вручную.

## Периоды анализа

Кнопки **7 днів / 30 днів / 3 міс.** переключают период отображения заказов.

---

## Обновление кода

Если нужно изменить дашборд — просто замени файлы на GitHub.
Vercel автоматически задеплоит изменения за ~30 секунд.
