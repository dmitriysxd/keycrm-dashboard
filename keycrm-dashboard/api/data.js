// KEYCRM API — https://docs.keycrm.app
// Base: https://openapi.keycrm.app/v1
// Endpoints:
//   GET /offers           — всі варіанти товарів (офери/SKU)
//   GET /offers/stocks    — залишки
//   GET /products/categories — категорії
//   GET /order            — замовлення

const BASE = "https://openapi.keycrm.app/v1";

async function get(path, params, apiKey) {
  var qs = Object.keys(params).map(function(k) {
    return encodeURIComponent(k) + "=" + encodeURIComponent(params[k]);
  }).join("&");
  var url = BASE + path + (qs ? "?" + qs : "");
  var res = await fetch(url, {
    headers: { "Authorization": "Bearer " + apiKey, "Accept": "application/json" }
  });
  if (res.status === 401) throw new Error("UNAUTHORIZED: невірний API ключ");
  if (!res.ok) {
    var body = ""; try { body = await res.text(); } catch(e) {}
    throw new Error("HTTP " + res.status + " [" + path + "]: " + body.substring(0, 150));
  }
  return res.json();
}

// Загружает все страницы (до maxPages * 50 записей)
async function fetchAll(path, params, apiKey, maxPages) {
  var all = [];
  for (var page = 1; page <= (maxPages || 30); page++) {
    var p = Object.assign({}, params, { page: page, limit: 50 });
    var data = await get(path, p, apiKey);
    var items = data.data || [];
    all = all.concat(items);
    var lastPage = (data.meta && data.meta.last_page) ? data.meta.last_page : 1;
    if (page >= lastPage || items.length === 0) break;
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
    // Загружаем последовательно чтобы не перегружать Vercel
    var categories = await fetchAll("/products/categories", {}, apiKey, 5);
    var offers     = await fetchAll("/offers", {}, apiKey, 30);
    var orders     = await fetchAll("/order", {
      include: "products,buyer,status,manager",
      created_at_min: sinceStr,
    }, apiKey, 30);

    // ── Карта категорий ─────────────────────────────
    var catNames = {};
    categories.forEach(function(c) { catNames[String(c.id)] = c.name; });

    // ── Аналитика по офферам ────────────────────────
    // /offers — это все варианты товаров (каждый SKU отдельно)
    // Каждый оффер имеет: id, name, sku/article, price, quantity, product_id, product (вложен)
    var catMap = {};
    var totalInStock = 0;
    offers.forEach(function(o) {
      var qty = parseFloat(o.quantity || o.in_stock || 0);
      if (qty > 0) totalInStock++;

      // Категория через связанный продукт
      var cat = "Без категорії";
      var prod = o.product || {};
      if (prod.category_id && catNames[String(prod.category_id)]) {
        cat = catNames[String(prod.category_id)];
      } else if (prod.category && typeof prod.category === "object" && prod.category.name) {
        cat = prod.category.name;
      } else if (o.category_id && catNames[String(o.category_id)]) {
        cat = catNames[String(o.category_id)];
      }
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      if (qty > 0) catMap[cat].inStock++;
    });

    var catList = Object.entries(catMap)
      .sort(function(a, b) { return b[1].count - a[1].count; })
      .slice(0, 10)
      .map(function(e) { return { name: e[0], count: e[1].count, inStock: e[1].inStock }; });

    // ── Ценовые диапазоны (из офферов) ──────────────
    var ranges = [
      { label: "< 50",    min: 0,    max: 50 },
      { label: "50–200",  min: 50,   max: 200 },
      { label: "200–500", min: 200,  max: 500 },
      { label: "500–1k",  min: 500,  max: 1000 },
      { label: "> 1000",  min: 1000, max: Infinity },
    ];
    ranges.forEach(function(r) {
      r.count = offers.filter(function(o) {
        var p = parseFloat(o.price || 0);
        return p > 0 && p >= r.min && p < r.max;
      }).length;
    });

    // ── Топ продаж ───────────────────────────────────
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

    // ── Все товары из заказов ────────────────────────
    var allTimeProducts = Object.values(salesMap)
      .sort(function(a, b) { return b.qty - a.qty; });

    // ── Продажи по дням ──────────────────────────────
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

    // ── Статусы ──────────────────────────────────────
    var statusMap = {};
    orders.forEach(function(o) {
      var s = (o.status && typeof o.status === "object")
        ? (o.status.name || "Невідомо") : ("id:" + (o.status_id || "?"));
      statusMap[s] = (statusMap[s] || 0) + 1;
    });
    var orderStatuses = Object.entries(statusMap)
      .sort(function(a, b) { return b[1] - a[1]; })
      .map(function(e) { return { name: e[0], count: e[1] }; });

    // ── KPI ──────────────────────────────────────────
    var totalRevenue = orders.reduce(function(s, o) { return s + calcRevenue(o); }, 0);

    return res.status(200).json({
      updatedAt: new Date().toISOString(),
      days: days,
      kpi: {
        totalProducts: offers.length,         // все офферы/SKU
        inStock: totalInStock,
        outOfStock: offers.length - totalInStock,
        totalOrders: orders.length,
        totalRevenue: Math.round(totalRevenue * 100) / 100,
        avgOrder: orders.length ? Math.round(totalRevenue / orders.length * 100) / 100 : 0,
      },
      categories: catList,
      topProducts: topProducts,
      allTimeProducts: allTimeProducts.slice(0, 100),
      salesByDay: salesByDay,
      priceRanges: ranges,
      orderStatuses: orderStatuses,
    });

  } catch(err) {
    var status = err.message.indexOf("UNAUTHORIZED") !== -1 ? 401 : 500;
    return res.status(status).json({ error: err.message });
  }
};
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
    // Загружаем последовательно чтобы не перегружать Vercel
    var categories = await fetchAll("/products/categories", {}, apiKey, 5);
    var offers     = await fetchAll("/offers", {}, apiKey, 30);
    var orders     = await fetchAll("/order", {
      include: "products,buyer,status,manager",
      created_at_min: sinceStr,
    }, apiKey, 30);

    // ── Карта категорий ─────────────────────────────
    var catNames = {};
    categories.forEach(function(c) { catNames[String(c.id)] = c.name; });

    // ── Аналитика по офферам ────────────────────────
    // /offers — это все варианты товаров (каждый SKU отдельно)
    // Каждый оффер имеет: id, name, sku/article, price, quantity, product_id, product (вложен)
    var catMap = {};
    var totalInStock = 0;
    offers.forEach(function(o) {
      var qty = parseFloat(o.quantity || o.in_stock || 0);
      if (qty > 0) totalInStock++;

      // Категория через связанный продукт
      var cat = "Без категорії";
      var prod = o.product || {};
      if (prod.category_id && catNames[String(prod.category_id)]) {
        cat = catNames[String(prod.category_id)];
      } else if (prod.category && typeof prod.category === "object" && prod.category.name) {
        cat = prod.category.name;
      } else if (o.category_id && catNames[String(o.category_id)]) {
        cat = catNames[String(o.category_id)];
      }
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      if (qty > 0) catMap[cat].inStock++;
    });

    var catList = Object.entries(catMap)
      .sort(function(a, b) { return b[1].count - a[1].count; })
      .slice(0, 10)
      .map(function(e) { return { name: e[0], count: e[1].count, inStock: e[1].inStock }; });

    // ── Ценовые диапазоны (из офферов) ──────────────
    var ranges = [
      { label: "< 50",    min: 0,    max: 50 },
      { label: "50–200",  min: 50,   max: 200 },
      { label: "200–500", min: 200,  max: 500 },
      { label: "500–1k",  min: 500,  max: 1000 },
      { label: "> 1000",  min: 1000, max: Infinity },
    ];
    ranges.forEach(function(r) {
      r.count = offers.filter(function(o) {
        var p = parseFloat(o.price || 0);
        return p > 0 && p >= r.min && p < r.max;
      }).length;
    });

    // ── Топ продаж ───────────────────────────────────
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

    // ── Все товары из заказов ────────────────────────
    var allTimeProducts = Object.values(salesMap)
      .sort(function(a, b) { return b.qty - a.qty; });

    // ── Продажи по дням ──────────────────────────────
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

    // ── Статусы ──────────────────────────────────────
    var statusMap = {};
    orders.forEach(function(o) {
      var s = (o.status && typeof o.status === "object")
        ? (o.status.name || "Невідомо") : ("id:" + (o.status_id || "?"));
      statusMap[s] = (statusMap[s] || 0) + 1;
    });
    var orderStatuses = Object.entries(statusMap)
      .sort(function(a, b) { return b[1] - a[1]; })
      .map(function(e) { return { name: e[0], count: e[1] }; });

    // ── KPI ──────────────────────────────────────────
    var totalRevenue = orders.reduce(function(s, o) { return s + calcRevenue(o); }, 0);

    return res.status(200).json({
      updatedAt: new Date().toISOString(),
      days: days,
      kpi: {
        totalProducts: offers.length,         // все офферы/SKU
        inStock: totalInStock,
        outOfStock: offers.length - totalInStock,
        totalOrders: orders.length,
        totalRevenue: Math.round(totalRevenue * 100) / 100,
        avgOrder: orders.length ? Math.round(totalRevenue / orders.length * 100) / 100 : 0,
      },
      categories: catList,
      topProducts: topProducts,
      allTimeProducts: allTimeProducts.slice(0, 100),
      salesByDay: salesByDay,
      priceRanges: ranges,
      orderStatuses: orderStatuses,
    });

  } catch(err) {
    var status = err.message.indexOf("UNAUTHORIZED") !== -1 ? 401 : 500;
    return res.status(status).json({ error: err.message });
  }
};
  if (fromItems > 0) return fromItems;
  return parseFloat(order.total_price || order.sum || order.ordered_sum || 0);
}

module.exports = async function(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  var apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

  var days = parseInt(req.query.days || "30");
  var since = new Date(); since.setDate(since.getDate() - days);
  var sinceStr = since.toISOString().split("T")[0];

  var historyDate = new Date(); historyDate.setFullYear(historyDate.getFullYear() - 2);
  var historyStr = historyDate.toISOString().split("T")[0];

  try {
    var results = await Promise.all([
      fetchPages("/products", {}, apiKey, 30),
      fetchPages("/products/categories", {}, apiKey, 5),
      fetchPages("/offers", {}, apiKey, 30),
      fetchPages("/order", { include: "products,buyer,status,manager", created_at_min: sinceStr }, apiKey, 30),
      fetchPages("/order", { include: "products", created_at_min: historyStr }, apiKey, 50),
    ]);

    var products      = results[0];
    var categories    = results[1];
    var offers        = results[2];
    var periodOrders  = results[3];
    var allOrders     = results[4];

    // ── Карта категорий id→name ──────────────────────
    var catNames = {};
    categories.forEach(function(c) { catNames[String(c.id)] = c.name; });

    // ── Категории товаров ────────────────────────────
    var catMap = {};
    products.forEach(function(p) {
      var cat = "Без категорії";
      if (p.category_id && catNames[String(p.category_id)]) {
        cat = catNames[String(p.category_id)];
      } else if (p.category && typeof p.category === "object" && p.category.name) {
        cat = p.category.name;
      } else if (p.category && typeof p.category === "string") {
        cat = p.category;
      }
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      var qty = parseFloat(p.quantity || p.in_stock || p.quantity_in_stock || 0);
      if (qty > 0) catMap[cat].inStock++;
    });
    var catList = Object.entries(catMap)
      .sort(function(a, b) { return b[1].count - a[1].count; })
      .slice(0, 10)
      .map(function(e) { return { name: e[0], count: e[1].count, inStock: e[1].inStock }; });

    // ── Все товары из истории заказов ────────────────
    var historyMap = {};
    allOrders.forEach(function(o) {
      (o.products || []).forEach(function(item) {
        var name = item.name || (item.offer && item.offer.name) || "—";
        var key = String(item.offer_id || name);
        if (!historyMap[key]) historyMap[key] = {
          name: name, totalQtySold: 0, totalRevenue: 0, lastSeen: "", price: parseFloat(item.price || 0),
        };
        historyMap[key].totalQtySold += parseInt(item.quantity || 1);
        historyMap[key].totalRevenue += parseFloat(item.price || 0) * parseInt(item.quantity || 1);
        var d = (o.created_at || "").substring(0, 10);
        if (d > historyMap[key].lastSeen) historyMap[key].lastSeen = d;
      });
    });
    var allTimeProducts = Object.values(historyMap)
      .sort(function(a, b) { return b.totalQtySold - a.totalQtySold; });

    // ── Топ продаж за период ─────────────────────────
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
      .sort(function(a, b) { return b.qty - a.qty; }).slice(0, 10);

    // ── Продажи по дням ──────────────────────────────
    var dayMap = {};
    periodOrders.forEach(function(o) {
      var d = (o.created_at || "").substring(0, 10);
      if (!d) return;
      if (!dayMap[d]) dayMap[d] = { orders: 0, revenue: 0 };
      dayMap[d].orders++;
      dayMap[d].revenue += calcRevenue(o);
    });
    var salesByDay = Object.entries(dayMap)
      .sort(function(a, b) { return a[0].localeCompare(b[0]); })
      .map(function(e) { return { date: e[0], orders: e[1].orders, revenue: Math.round(e[1].revenue * 100) / 100 }; });

    // ── Ценовые диапазоны (из офферов) ──────────────
    var ranges = [
      { label: "< 50",    min: 0,    max: 50 },
      { label: "50–200",  min: 50,   max: 200 },
      { label: "200–500", min: 200,  max: 500 },
      { label: "500–1k",  min: 500,  max: 1000 },
      { label: "> 1000",  min: 1000, max: Infinity },
    ];
    ranges.forEach(function(r) {
      r.count = offers.filter(function(o) {
        var price = parseFloat(o.price || 0);
        return price > 0 && price >= r.min && price < r.max;
      }).length;
    });

    // ── Статусы заказов ──────────────────────────────
    var statusMap = {};
    periodOrders.forEach(function(o) {
      var s = (o.status && typeof o.status === "object")
        ? (o.status.name || "Невідомо") : ("id:" + (o.status_id || "?"));
      statusMap[s] = (statusMap[s] || 0) + 1;
    });
    var orderStatuses = Object.entries(statusMap)
      .sort(function(a, b) { return b[1] - a[1]; })
      .map(function(e) { return { name: e[0], count: e[1] }; });

    // ── KPI ──────────────────────────────────────────
    var inStock = products.filter(function(p) {
      return parseFloat(p.quantity || p.in_stock || p.quantity_in_stock || 0) > 0;
    }).length;
    var totalRevenue = periodOrders.reduce(function(s, o) { return s + calcRevenue(o); }, 0);

    return res.status(200).json({
      updatedAt: new Date().toISOString(),
      days: days,
      kpi: {
        totalProducts: products.length,
        allTimeProducts: allTimeProducts.length,
        inStock: inStock,
        outOfStock: products.length - inStock,
        totalOrders: periodOrders.length,
        allTimeOrders: allOrders.length,
        totalRevenue: Math.round(totalRevenue * 100) / 100,
        avgOrder: periodOrders.length ? Math.round(totalRevenue / periodOrders.length * 100) / 100 : 0,
      },
      categories: catList,
      topProducts: topProducts,
      allTimeProducts: allTimeProducts.slice(0, 100),
      salesByDay: salesByDay,
      priceRanges: ranges,
      orderStatuses: orderStatuses,
    });

  } catch(err) {
    var status = err.message.indexOf("UNAUTHORIZED") !== -1 ? 401 : 500;
    return res.status(status).json({ error: err.message });
  }
};
  return parseFloat(order.total_price || order.sum || order.ordered_sum || 0);
}

module.exports = async function(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  var apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано" });

  var days = parseInt(req.query.days || "30");
  var since = new Date(); since.setDate(since.getDate() - days);
  var sinceStr = since.toISOString().split("T")[0];

  var historyDate = new Date(); historyDate.setFullYear(historyDate.getFullYear() - 2);
  var historyStr = historyDate.toISOString().split("T")[0];

  try {
    // Параллельная загрузка всего
    var results = await Promise.all([
      fetchPages("/products", {}, apiKey, 30),                            // активные товары
      fetchPages("/products/categories", {}, apiKey, 10),                 // категории
      fetchPages("/offers", {}, apiKey, 30),                              // офферы с ценами
      fetchPages("/order", { include: "products,buyer,status,manager", created_at_min: sinceStr }, apiKey, 30),   // заказы за период
      fetchPages("/order", { include: "products", created_at_min: historyStr }, apiKey, 50), // вся история
    ]);

    var products   = results[0];
    var categories = results[1];
    var offers     = results[2];
    var periodOrders = results[3];
    var allOrders    = results[4];

    // ── Карта категорий id→name ─────────────────────
    var catNames = {};
    categories.forEach(function(c) { catNames[c.id] = c.name; });

    // ── Карта офферов id→{price, quantity} ──────────
    var offerMap = {};
    offers.forEach(function(o) {
      offerMap[o.id] = {
        price: parseFloat(o.price || 0),
        quantity: parseFloat(o.quantity || o.in_stock || 0),
      };
    });

    // ── Категории ───────────────────────────────────
    var catMap = {};
    products.forEach(function(p) {
      var cat = (p.category_id && catNames[p.category_id])
        ? catNames[p.category_id]
        : (p.category && typeof p.category === "object" ? p.category.name : null)
        || "Без категорії";
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      if (parseFloat(p.quantity || p.in_stock || p.quantity_in_stock || 0) > 0) catMap[cat].inStock++;
    });
    var catList = Object.entries(catMap)
      .sort(function(a, b) { return b[1].count - a[1].count; })
      .slice(0, 10)
      .map(function(e) { return { name: e[0], count: e[1].count, inStock: e[1].inStock }; });

    // ── Все товары из истории заказов ───────────────
    var historyMap = {};
    allOrders.forEach(function(o) {
      (o.products || []).forEach(function(item) {
        var name = item.name || (item.offer && item.offer.name) || "—";
        var key = String(item.offer_id || name);
        if (!historyMap[key]) historyMap[key] = {
          name: name, offer_id: item.offer_id,
          totalQtySold: 0, totalRevenue: 0, lastSeen: "",
          price: parseFloat(item.price || 0),
        };
        historyMap[key].totalQtySold += parseInt(item.quantity || 1);
        historyMap[key].totalRevenue += parseFloat(item.price || 0) * parseInt(item.quantity || 1);
        var d = (o.created_at || "").substring(0, 10);
        if (d > historyMap[key].lastSeen) historyMap[key].lastSeen = d;
      });
    });
    var allTimeProducts = Object.values(historyMap)
      .sort(function(a, b) { return b.totalQtySold - a.totalQtySold });

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
      .sort(function(a, b) { return b.qty - a.qty; }).slice(0, 10);

    // ── Продажи по дням (выручка из позиций) ────────
    var dayMap = {};
    periodOrders.forEach(function(o) {
      var d = (o.created_at || "").substring(0, 10);
      if (!d) return;
      if (!dayMap[d]) dayMap[d] = { orders: 0, revenue: 0 };
      dayMap[d].orders++;
      dayMap[d].revenue += orderRevenue(o);
    });
    var salesByDay = Object.entries(dayMap)
      .sort(function(a, b) { return a[0].localeCompare(b[0]); })
      .map(function(e) { return { date: e[0], orders: e[1].orders, revenue: e[1].revenue }; });

    // ── Ценовые диапазоны (из офферов) ──────────────
    var offerPrices = offers.map(function(o) { return parseFloat(o.price || 0); }).filter(function(p) { return p > 0; });
    var ranges = [
      { label: "< 50",    min: 0,    max: 50 },
      { label: "50–200",  min: 50,   max: 200 },
      { label: "200–500", min: 200,  max: 500 },
      { label: "500–1k",  min: 500,  max: 1000 },
      { label: "> 1000",  min: 1000, max: Infinity },
    ];
    ranges.forEach(function(r) {
      r.count = offerPrices.filter(function(p) { return p >= r.min && p < r.max; }).length;
    });

    // ── Статусы заказов ──────────────────────────────
    var statusMap = {};
    periodOrders.forEach(function(o) {
      var s = (o.status && typeof o.status === "object")
        ? (o.status.name || "Невідомо") : ("Статус #" + (o.status_id || "?"));
      statusMap[s] = (statusMap[s] || 0) + 1;
    });
    var orderStatuses = Object.entries(statusMap)
      .sort(function(a, b) { return b[1] - a[1]; })
      .map(function(e) { return { name: e[0], count: e[1] }; });

    // ── KPI ──────────────────────────────────────────
    var inStock = products.filter(function(p) {
      return parseFloat(p.quantity || p.in_stock || p.quantity_in_stock || 0) > 0;
    }).length;
    var totalRevenue = periodOrders.reduce(function(s, o) { return s + orderRevenue(o); }, 0);

    return res.status(200).json({
      updatedAt: new Date().toISOString(),
      days: days,
      kpi: {
        totalProducts: products.length,
        allTimeProducts: allTimeProducts.length,
        inStock: inStock,
        outOfStock: products.length - inStock,
        totalOrders: periodOrders.length,
        allTimeOrders: allOrders.length,
        totalRevenue: totalRevenue,
        avgOrder: periodOrders.length ? totalRevenue / periodOrders.length : 0,
      },
      categories: catList,
      topProducts: topProducts,
      allTimeProducts: allTimeProducts.slice(0, 100),
      salesByDay: salesByDay,
      priceRanges: ranges,
      orderStatuses: orderStatuses,
    });

  } catch(err) {
    var status = err.message.indexOf("UNAUTHORIZED") !== -1 ? 401 : 500;
    return res.status(status).json({ error: err.message });
  }
};
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
