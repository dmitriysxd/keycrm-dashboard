# KEYCRM Analytics Dashboard

Красивый дашборд с живыми данными из KEYCRM. Разворачивается бесплатно на Vercel за 5 минут.

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

### Шаг 3 — Добавь API-ключ

1. В Vercel зайди в свой проект → **Settings** → **Environment Variables**
2. Нажми **Add**:
   - Name: `KEYCRM_API_KEY`
   - Value: *(твой ключ из KEYCRM)*
3. Нажми **Save**
4. Зайди в **Deployments** → нажми на последний деплой → **Redeploy**

### Шаг 4 — Открой дашборд

Открой ссылку `keycrm-dashboard-xxx.vercel.app` — дашборд работает!

Добавь в закладки браузера или на главный экран телефона.

---

## Структура файлов

```
keycrm-dashboard/
├── index.html        # Фронтенд дашборда
├── vercel.json       # Настройки Vercel
└── api/
    └── data.js       # Серверная функция (прокси к KEYCRM API)
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
