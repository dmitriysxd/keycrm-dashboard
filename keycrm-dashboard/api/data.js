// KEYCRM API — https://docs.keycrm.app
// Base: https://openapi.keycrm.app/v1

const BASE = "https://openapi.keycrm.app/v1";

async function fetchAll(path, params, apiKey) {
  var headers = { "Authorization": "Bearer " + apiKey, "Accept": "application/json" };
  var all = [];
  var page = 1;

  while (true) {
    var p = Object.assign({}, params, { page: page, limit: 50 });
    var qs = Object.keys(p).map(function(k) {
      return encodeURIComponent(k) + "=" + encodeURIComponent(p[k]);
    }).join("&");

    var res = await fetch(BASE + path + "?" + qs, { headers: headers });
    if (res.status === 401) throw new Error("UNAUTHORIZED: невірний API ключ");
    if (!res.ok) {
      var body = ""; try { body = await res.text(); } catch(e) {}
      throw new Error("HTTP " + res.status + " [" + path + "]: " + body.substring(0, 200));
    }

    var json = await res.json();
    var items = json.data || [];
    all = all.concat(items);

    // KEYCRM може повертати пагінацію в meta або в корені відповіді
    var meta = json.meta || json;
    var total = meta.total || 0;
    var lastPage = meta.last_page || (total > 0 ? Math.ceil(total / 50) : 1);
    var currentPage = meta.current_page || page;

    // Зупиняємось якщо дійшли до останньої сторінки або немає даних
    if (items.length === 0 || currentPage >= lastPage || all.length >= (total || all.length + 1)) break;
    page++;
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
    // Завантажуємо категорії (зазвичай мало — 1-2 сторінки)
    var categories = await fetchAll("/products/categories", {}, apiKey);

    // Завантажуємо офери (всі SKU з цінами та залишками)
    var offers = await fetchAll("/offers", {}, apiKey);

    // Завантажуємо замовлення за вибраний період
    var orders = await fetchAll("/order", {
      include: "products,buyer,status,manager",
      created_at_min: sinceStr,
    }, apiKey);

    // Карта категорій
    var catNames = {};
    categories.forEach(function(c) { catNames[String(c.id)] = c.name; });

    // Аналітика по офферам (всі SKU)
    var catMap = {};
    var totalInStock = 0;
    offers.forEach(function(o) {
      var qty = parseFloat(o.quantity || o.in_stock || 0);
      if (qty > 0) totalInStock++;

      var cat = "Без категорії";
      var prod = o.product || {};
      var catId = prod.category_id || o.category_id;
      if (catId && catNames[String(catId)]) {
        cat = catNames[String(catId)];
      } else if (prod.category && prod.category.name) {
        cat = prod.category.name;
      }
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      if (qty > 0) catMap[cat].inStock++;
    });

    var catList = Object.entries(catMap)
      .sort(function(a, b) { return b[1].count - a[1].count; })
      .slice(0, 10)
      .map(function(e) { return { name: e[0], count: e[1].count, inStock: e[1].inStock }; });

    // Ціновий розподіл
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

    // Топ продажів за період
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
        totalProducts: offers.length,
        inStock: totalInStock,
        outOfStock: offers.length - totalInStock,
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
