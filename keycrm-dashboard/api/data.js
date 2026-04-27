// KEYCRM API — https://docs.keycrm.app
// Стратегія: мінімум запитів (~6-8 всього) щоб не словити 429 і не таймаутитись

const BASE = "https://openapi.keycrm.app/v1";

async function get(path, params, apiKey) {
  var qs = Object.keys(params).map(function(k) {
    return encodeURIComponent(k) + "=" + encodeURIComponent(params[k]);
  }).join("&");
  var res = await fetch(BASE + path + (qs ? "?" + qs : ""), {
    headers: { "Authorization": "Bearer " + apiKey, "Accept": "application/json" }
  });
  if (res.status === 401) throw new Error("UNAUTHORIZED: невірний API ключ");
  if (res.status === 429) throw new Error("Ліміт запитів KEYCRM — зачекайте хвилину та оновіть");
  if (!res.ok) {
    var t = ""; try { t = await res.text(); } catch(e) {}
    throw new Error("HTTP " + res.status + " [" + path + "]: " + t.substring(0, 150));
  }
  return res.json();
}

function calcRevenue(order) {
  var fromItems = (order.products || []).reduce(function(s, i) {
    return s + parseFloat(i.price || 0) * parseInt(i.quantity || 1);
  }, 0);
  return fromItems > 0 ? fromItems : parseFloat(order.total_price || order.sum || 0);
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
    // Всі запити паралельно — швидко і в межах 60 req/min
    // Максимум ~6 запитів одночасно — добре вписується в ліміт
    var results = await Promise.all([
      get("/products", { page: 1, limit: 50 }, apiKey),           // 1 запит: кількість товарів
      get("/products/categories", { page: 1, limit: 50 }, apiKey), // 1 запит: категорії
      get("/order", { include: "products,buyer,status,manager", created_at_min: sinceStr, page: 1, limit: 50 }, apiKey), // 1 запит: замовлення стор.1
      get("/order", { include: "products,buyer,status,manager", created_at_min: sinceStr, page: 2, limit: 50 }, apiKey), // стор.2
      get("/order", { include: "products,buyer,status,manager", created_at_min: sinceStr, page: 3, limit: 50 }, apiKey), // стор.3
      get("/offers", { page: 1, limit: 50 }, apiKey),              // 1 запит: ціни
    ]);

    var productsResp  = results[0];
    var catsResp      = results[1];
    var orders1       = (results[2].data || []);
    var orders2       = (results[3].data || []);
    var orders3       = (results[4].data || []);
    var offersResp    = results[5];

    // Реальна кількість товарів з meta
    var pMeta = productsResp.meta || productsResp;
    var totalProducts = parseInt(pMeta.total || (productsResp.data || []).length);

    // Категорії
    var catNames = {};
    (catsResp.data || []).forEach(function(c) { catNames[String(c.id)] = c.name; });

    // Всі замовлення (до 150)
    var orders = orders1.concat(orders2).concat(orders3);

    // Категорії товарів
    var catMap = {};
    var inStock = 0;
    (productsResp.data || []).forEach(function(p) {
      var qty = parseFloat(p.quantity || p.in_stock || 0);
      if (qty > 0) inStock++;
      var cat = "Без категорії";
      if (p.category_id && catNames[String(p.category_id)]) cat = catNames[String(p.category_id)];
      else if (p.category && p.category.name) cat = p.category.name;
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      if (qty > 0) catMap[cat].inStock++;
    });
    var catList = Object.entries(catMap)
      .sort(function(a, b) { return b[1].count - a[1].count; })
      .slice(0, 10)
      .map(function(e) { return { name: e[0], count: e[1].count, inStock: e[1].inStock }; });

    // Ціни з офферів
    var ranges = [
      { label: "< 50",    min: 0,    max: 50 },
      { label: "50–200",  min: 50,   max: 200 },
      { label: "200–500", min: 200,  max: 500 },
      { label: "500–1к",  min: 500,  max: 1000 },
      { label: "> 1000",  min: 1000, max: Infinity },
    ];
    (offersResp.data || []).forEach(function(o) {
      var p = parseFloat(o.price || 0);
      if (p <= 0) return;
      ranges.forEach(function(r) { if (p >= r.min && p < r.max) r.count++; });
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
      var s = (o.status && typeof o.status === "object") ? (o.status.name || "?") : ("id:" + (o.status_id || "?"));
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
        totalProducts: totalProducts,
        inStock: inStock,
        outOfStock: (productsResp.data || []).length - inStock,
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
