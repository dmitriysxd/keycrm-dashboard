# Supabase Setup

## 1. Создай проект

1. Зайди на [supabase.com](https://supabase.com) → **New project**.
2. Имя: `keycrm-dashboard`. Пароль БД — сохрани в надёжном месте.
3. Регион — ближайший (Frankfurt / Stockholm).
4. Дождись готовности (~2 минуты).

## 2. Применить миграции

В Supabase Studio:

1. **SQL Editor** → **New query**.
2. Скопируй полное содержимое `001_init.sql` → нажми **Run**.
3. **New query** ещё раз → скопируй `002_sku_metrics.sql` → **Run**.
4. Проверь в **Table editor**: должны быть таблицы `skus`, `stock_snapshots`, `sales`, `ingest_runs` и view `sku_metrics`.

## 3. Достань ключи

В Supabase: **Settings → API**:

- `Project URL` → переменная `SUPABASE_URL`
- `service_role` (secret, **never on frontend!**) → `SUPABASE_SERVICE_ROLE_KEY`

## 4. Добавь переменные в Vercel

**Settings → Environment Variables** → Add:

| Name | Value |
|---|---|
| `SUPABASE_URL` | `https://xxx.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY` | secret из Supabase |
| `CRON_SECRET` | случайная длинная строка (например `openssl rand -hex 32`) |
| `DASHBOARD_TOKEN` | случайная длинная строка для доступа к фронту |

После сохранения — **Redeploy** последний деплой.

## 5. Бэкфилл 90 дней истории заказов

Замени `APP` на свой Vercel-домен и `XXX` на значение `CRON_SECRET`. Запускать **последовательно**, каждый занимает до 60 секунд:

```bash
# Последние 30 дней
curl -i -H "Authorization: Bearer XXX" \
  "https://APP.vercel.app/api/cron/backfill?from=$(date -u -d '30 days ago' +%Y-%m-%d)&to=$(date -u +%Y-%m-%d)"

# 30-60 дней назад
curl -i -H "Authorization: Bearer XXX" \
  "https://APP.vercel.app/api/cron/backfill?from=$(date -u -d '60 days ago' +%Y-%m-%d)&to=$(date -u -d '30 days ago' +%Y-%m-%d)"

# 60-90 дней назад
curl -i -H "Authorization: Bearer XXX" \
  "https://APP.vercel.app/api/cron/backfill?from=$(date -u -d '90 days ago' +%Y-%m-%d)&to=$(date -u -d '60 days ago' +%Y-%m-%d)"
```

После каждого запуска проверь в Supabase: `SELECT COUNT(*) FROM sales` — число должно расти.

## 6. Запусти первый ингест

```bash
curl -i -H "Authorization: Bearer XXX" \
  "https://APP.vercel.app/api/cron/ingest"
```

Должен быть HTTP 200, в Supabase появятся строки в `skus`, `stock_snapshots` и обновится `sku_metrics`.

Дальше Vercel Cron сам будет запускать ингест каждый день в 03:00 UTC.

## 7. Открой дашборд

```
https://APP.vercel.app/?key=ЗНАЧЕНИЕ_DASHBOARD_TOKEN
```

После первого открытия токен сохранится в localStorage — параметр `?key=` можно убрать.

## Что делать если cron упал

В Supabase: `SELECT * FROM ingest_runs ORDER BY started_at DESC LIMIT 5`. Колонка `error_message` покажет причину. Запусти ингест руками тем же curl-ом из шага 6.
