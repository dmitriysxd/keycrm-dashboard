// KEYCRM API — https://docs.keycrm.app
// Base: https://openapi.keycrm.app/v1
// Rate limit: 60 req/min — додаємо затримку між запитами

const BASE = "https://openapi.keycrm.app/v1";

function sleep(ms) {
  return new Promise(function(r) { setTimeout(r, ms); });
}

async function get(path, params, apiKey) {
  var qs = Object.keys(params).map(function(k) {
    return encodeURIComponent(k) + "=" + encodeURIComponent(params[k]);
  }).join("&");
  var res = await fetch(BASE + path + (qs ? "?" + qs : ""), {
    headers: { "Authorization": "Bearer " + apiKey, "Accept": "application/json" }
  });
  if (res.status === 401) throw new Error("UNAUTHORIZED: невірний API ключ");
  if (res.status === 429) throw new Error("Занадто багато запитів — спробуйте через хвилину");
  if (!res.ok) {
    var body = ""; try { body = await res.text(); } catch(e) {}
    throw new Error("HTTP " + res.status + " [" + path + "]: " + body.substring(0, 150));
  }
  return res.json();
}

// Завантажує всі сторінки з паузою між запитами (щоб не словити 429)
async function fetchAll(path, params, apiKey, maxPages, delayMs) {
  delayMs = delayMs || 1100;
  maxPages = maxPages || 10;
  var all = [];
  var page = 1;

  while (page <= maxPages) {
    var p = Object.assign({}, params, { page: page, limit: 50 });
    var data = await get(path, p, apiKey);
    var items = data.data || [];
    all = all.concat(items);

    // Читаємо пагінацію — KEYCRM може класти її в meta або в корінь
    var src = data.meta || data;
    var total = parseInt(src.total || 0);
    var lastPage = parseInt(src.last_page || 0) || (total > 0 ? Math.ceil(total / 50) : 1);

    if (items.length === 0 || page >= lastPage) break;
    page++;

    // Пауза між запитами щоб не перевищити 60 req/min
    await sleep(delayMs);
  }
  return all;
}

function calcRevenue(order) {
  var fromItems = (order.products || []).reduce(function(s, item) {
    return s + parseFloat(item.price || 0) * parseInt(item.quantity || 1);
  }, 0);
  if (fromItems > 0) return fromItems;
  return parseFloat(order.total_price || order.sum || 0);
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  var apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

  var days = parseInt(req.query.days || "30");
  var since = new Date(); since.setDate(since.getDate() - days);
  var sinceStr = since.toISOString().split("T")[0];

  try {
    // 1. Отримуємо загальну кількість товарів (1 запит)
    var productsPage1 = await get("/products", { page: 1, limit: 50 }, apiKey);
    var productsMeta = productsPage1.meta || productsPage1;
    var totalProducts = parseInt(productsMeta.total || (productsPage1.data || []).length);
    await sleep(1100);

    // 2. Категорії (зазвичай 1-2 сторінки)
    var categories = await fetchAll("/products/categories", {}, apiKey, 5, 1100);
    var catNames = {};
    categories.forEach(function(c) { catNames[String(c.id)] = c.name; });
    await sleep(1100);

    // 3. Перша сторінка офферів для розподілу цін (тільки 1 запит — без 429)
    var offersPage = await get("/offers", { page: 1, limit: 50 }, apiKey);
    var offersData = offersPage.data || [];
    var offersMeta = offersPage.meta || offersPage;
    var totalOffers = parseInt(offersMeta.total || offersData.length);
    await sleep(1100);

    // 4. Замовлення за вибраний період (з паузами)
    var orders = await fetchAll("/order", {
      include: "products,buyer,status,manager",
      created_at_min: sinceStr,
    }, apiKey, 30, 1100);

    // --- Аналітика ---

    // Категорії з першої сторінки продуктів
    var catMap = {};
    var inStockCount = 0;
    (productsPage1.data || []).forEach(function(p) {
      var qty = parseFloat(p.quantity || p.in_stock || 0);
      if (qty > 0) inStockCount++;
      var cat = "Без категорії";
      var cid = p.category_id;
      if (cid && catNames[String(cid)]) cat = catNames[String(cid)];
      else if (p.category && p.category.name) cat = p.category.name;
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      if (qty > 0) catMap[cat].inStock++;
    });
    // Масштабуємо inStock на весь каталог
    var inStockEstimate = Math.round(inStockCount / 50 * totalProducts);

    var catList = Object.entries(catMap)
      .sort(function(a, b) { return b[1].count - a[1].count; })
      .slice(0, 10)
      .map(function(e) { return { name: e[0], count: e[1].count, inStock: e[1].inStock }; });

    // Ціновий розподіл (з першої сторінки офферів)
    var ranges = [
      { label: "< 50",    min: 0,    max: 50 },
      { label: "50–200",  min: 50,   max: 200 },
      { label: "200–500", min: 200,  max: 500 },
      { label: "500–1к",  min: 500,  max: 1000 },
      { label: "> 1000",  min: 1000, max: Infinity },
    ];
    ranges.forEach(function(r) {
      r.count = offersData.filter(function(o) {
        var p = parseFloat(o.price || 0);
        return p > 0 && p >= r.min && p < r.max;
      }).length;
    });

    // Топ продажів
    var salesMap = {};
    orders.forEach(function(o) {
      (o.products || []).forEach(function(item) {
        var name = item.name || (item.offer && item.offer.name) || "—";
        var key = String(item.offer_id || name);
        if (!salesMap[key]) salesMap[key] = { name: name, qty: 0, revenue: 0 };
        salesMap[key].qty += parseInt(item.quantity || 1);
        salesMap[key].revenue += parseFloat(item.price || 0) * parseInt(item.quantity || 1);
      });
    });
    var topProducts = Object.values(salesMap)
      .sort(function(a, b) { return b.qty - a.qty; }).slice(0, 10);

    // Продажі по днях
    var dayMap = {};
    orders.forEach(function(o) {
      var d = (o.created_at || "").substring(0, 10);
      if (!d) return;
      if (!dayMap[d]) dayMap[d] = { orders: 0, revenue: 0 };
      dayMap[d].orders++;
      dayMap[d].revenue += calcRevenue(o);
    });
    var salesByDay = Object.entries(dayMap)
      .sort(function(a, b) { return a[0].localeCompare(b[0]); })
      .map(function(e) { return { date: e[0], orders: e[1].orders, revenue: Math.round(e[1].revenue * 100) / 100 }; });

    // Статуси
    var statusMap = {};
    orders.forEach(function(o) {
      var s = (o.status && typeof o.status === "object")
        ? (o.status.name || "Невідомо") : ("id:" + (o.status_id || "?"));
      statusMap[s] = (statusMap[s] || 0) + 1;
    });
    var orderStatuses = Object.entries(statusMap)
      .sort(function(a, b) { return b[1] - a[1]; })
      .map(function(e) { return { name: e[0], count: e[1] }; });

    // KPI
    var totalRevenue = orders.reduce(function(s, o) { return s + calcRevenue(o); }, 0);

    return res.status(200).json({
      updatedAt: new Date().toISOString(),
      days: days,
      kpi: {
        totalProducts: totalProducts,   // реальна кількість з meta.total
        totalOffers: totalOffers,       // кількість варіантів
        inStock: inStockEstimate,
        outOfStock: totalProducts - inStockEstimate,
        totalOrders: orders.length,
        totalRevenue: Math.round(totalRevenue * 100) / 100,
        avgOrder: orders.length ? Math.round(totalRevenue / orders.length * 100) / 100 : 0,
      },
      categories: catList,
      topProducts: topProducts,
      salesByDay: salesByDay,
      priceRanges: ranges,
      orderStatuses: orderStatuses,
    });

  } catch(err) {
    var status = err.message.indexOf("UNAUTHORIZED") !== -1 ? 401 : 500;
    return res.status(status).json({ error: err.message });
  }
};
