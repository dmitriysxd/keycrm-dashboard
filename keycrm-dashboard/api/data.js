// KEYCRM API: https://docs.keycrm.app
// Base: https://openapi.keycrm.app/v1

const BASE = "https://openapi.keycrm.app/v1";

async function fetchPages(endpoint, params, apiKey, maxPages) {
  maxPages = maxPages || 20;
  var headers = {
    "Authorization": "Bearer " + apiKey,
    "Accept": "application/json",
  };
  var all = [];
  var page = 1;
  while (page <= maxPages) {
    var url = new URL(BASE + endpoint);
    var p = Object.assign({}, params, { page: page, limit: 50 });
    Object.keys(p).forEach(function(k) { url.searchParams.set(k, p[k]); });
    var res = await fetch(url.toString(), { headers: headers });
    if (!res.ok) {
      if (res.status === 401) throw new Error("UNAUTHORIZED: невірний API ключ");
      var body = "";
      try { body = await res.text(); } catch(e) {}
      throw new Error("HTTP " + res.status + " [" + endpoint + "]: " + body.substring(0, 300));
    }
    var json = await res.json();
    var items = json.data || [];
    all = all.concat(items);
    var total = json.meta && json.meta.total ? json.meta.total : items.length;
    var lastPage = json.meta && json.meta.last_page ? json.meta.last_page : 1;
    if (page >= lastPage || items.length === 0) break;
    page++;
  }
  return all;
}

module.exports = async function(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  var apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });
  }

  var days = parseInt(req.query.days || "30");
  var since = new Date();
  since.setDate(since.getDate() - days);
  var sinceStr = since.toISOString().split("T")[0];

  // Для истории берём заказы за 2 года
  var historyDate = new Date();
  historyDate.setFullYear(historyDate.getFullYear() - 2);
  var historyStr = historyDate.toISOString().split("T")[0];

  try {
    var results = await Promise.all([
      // Активные товары
      fetchPages("/products", {}, apiKey, 30),
      // Заказы за выбранный период (для графиков)
      fetchPages("/order", {
        include: "products,buyer,status,manager",
        created_at_min: sinceStr,
      }, apiKey, 30),
      // Все заказы за 2 года (для исторического анализа товаров)
      fetchPages("/order", {
        include: "products",
        created_at_min: historyStr,
      }, apiKey, 50),
    ]);

    var products = results[0];
    var periodOrders = results[1];
    var allOrders = results[2];

    // ── Собираем ВСЕ товары из истории заказов ──────
    // Это включает товары которых уже нет в каталоге
    var historyMap = {};
    allOrders.forEach(function(o) {
      (o.products || []).forEach(function(item) {
        var name = item.name || (item.offer && item.offer.name) || "—";
        var key = String(item.offer_id || name);
        if (!historyMap[key]) {
          historyMap[key] = {
            name: name,
            offer_id: item.offer_id,
            totalQtySold: 0,
            totalRevenue: 0,
            lastSeen: "",
            price: parseFloat(item.price || 0),
          };
        }
        historyMap[key].totalQtySold += parseInt(item.quantity || 1);
        historyMap[key].totalRevenue += parseFloat(item.price || 0) * parseInt(item.quantity || 1);
        var d = (o.created_at || "").substring(0, 10);
        if (d > historyMap[key].lastSeen) historyMap[key].lastSeen = d;
      });
    });
    var allTimeProducts = Object.values(historyMap)
      .sort(function(a, b) { return b.totalQtySold - a.totalQtySold; });

    // ── Категории (по активным товарам) ─────────────
    var catMap = {};
    products.forEach(function(p) {
      var cat;
      if (p.category && typeof p.category === "object") {
        cat = p.category.name || "Без категорії";
      } else if (p.category_id) {
        cat = "Категорія #" + p.category_id;
      } else {
        cat = "Без категорії";
      }
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      if (parseFloat(p.quantity || p.in_stock || p.quantity_in_stock || 0) > 0) catMap[cat].inStock++;
    });
    var categories = Object.entries(catMap)
      .sort(function(a, b) { return b[1].count - a[1].count; })
      .slice(0, 10)
      .map(function(e) { return { name: e[0], count: e[1].count, inStock: e[1].inStock }; });

    // ── Топ продаж за ПЕРИОД ─────────────────────────
    var salesMap = {};
    periodOrders.forEach(function(o) {
      (o.products || []).forEach(function(item) {
        var name = item.name || (item.offer && item.offer.name) || "—";
        var key = String(item.offer_id || name);
        if (!salesMap[key]) salesMap[key] = { name: name, qty: 0, revenue: 0 };
        salesMap[key].qty += parseInt(item.quantity || 1);
        salesMap[key].revenue += parseFloat(item.price || 0) * parseInt(item.quantity || 1);
      });
    });
    var topProducts = Object.values(salesMap)
      .sort(function(a, b) { return b.qty - a.qty; })
      .slice(0, 10);

    // ── Продажи по дням ───────────────────────────────
    var dayMap = {};
    periodOrders.forEach(function(o) {
      var d = (o.created_at || "").substring(0, 10);
      if (!d) return;
      if (!dayMap[d]) dayMap[d] = { orders: 0, revenue: 0 };
      dayMap[d].orders++;
      dayMap[d].revenue += parseFloat(o.total_price || 0);
    });
    var salesByDay = Object.entries(dayMap)
      .sort(function(a, b) { return a[0].localeCompare(b[0]); })
      .map(function(e) { return { date: e[0], orders: e[1].orders, revenue: e[1].revenue }; });

    // ── Ценовые диапазоны ─────────────────────────────
    var ranges = [
      { label: "< 50",    min: 0,    max: 50 },
      { label: "50–200",  min: 50,   max: 200 },
      { label: "200–500", min: 200,  max: 500 },
      { label: "500–1k",  min: 500,  max: 1000 },
      { label: "> 1000",  min: 1000, max: Infinity },
    ];
    ranges.forEach(function(r) {
      r.count = products.filter(function(p) {
        var price = parseFloat(p.price || p.min_price || 0);
        return price >= r.min && price < r.max;
      }).length;
    });

    // ── Статусы заказов ───────────────────────────────
    var statusMap = {};
    periodOrders.forEach(function(o) {
      var s = (o.status && typeof o.status === "object")
        ? (o.status.name || "Невідомо")
        : ("Статус #" + (o.status_id || "?"));
      statusMap[s] = (statusMap[s] || 0) + 1;
    });
    var orderStatuses = Object.entries(statusMap)
      .sort(function(a, b) { return b[1] - a[1]; })
      .map(function(e) { return { name: e[0], count: e[1] }; });

    // ── KPI ───────────────────────────────────────────
    var inStock = products.filter(function(p) {
      return parseFloat(p.quantity || p.in_stock || p.quantity_in_stock || 0) > 0;
    }).length;
    var totalRevenue = periodOrders.reduce(function(s, o) {
      return s + parseFloat(o.total_price || 0);
    }, 0);
    var avgOrder = periodOrders.length ? totalRevenue / periodOrders.length : 0;

    return res.status(200).json({
      updatedAt: new Date().toISOString(),
      days: days,
      kpi: {
        totalProducts: products.length,         // активные в каталоге
        allTimeProducts: allTimeProducts.length, // все что когда-либо продавались
        inStock: inStock,
        outOfStock: products.length - inStock,
        totalOrders: periodOrders.length,
        allTimeOrders: allOrders.length,
        totalRevenue: totalRevenue,
        avgOrder: avgOrder,
      },
      categories: categories,
      topProducts: topProducts,
      allTimeProducts: allTimeProducts.slice(0, 50), // топ-50 за всё время
      salesByDay: salesByDay,
      priceRanges: ranges,
      orderStatuses: orderStatuses,
    });

  } catch(err) {
    var status = err.message.indexOf("UNAUTHORIZED") !== -1 ? 401 : 500;
    return res.status(status).json({ error: err.message });
  }
};
