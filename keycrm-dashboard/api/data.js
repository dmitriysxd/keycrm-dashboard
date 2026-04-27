const BASE = "https://openapi.keycrm.app/v1";

async function fetchPages(endpoint, params, apiKey, maxPages) {
  maxPages = maxPages || 15;
  const headers = {
    "Authorization": "Bearer " + apiKey,
    "Content-Type": "application/json",
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
      if (res.status === 401) throw new Error("UNAUTHORIZED");
      throw new Error("HTTP " + res.status);
    }
    var json = await res.json();
    var items = json.data || [];
    all = all.concat(items);
    var lastPage = (json.meta && json.meta.last_page) ? json.meta.last_page : 1;
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
    return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано в Vercel Environment Variables" });
  }

  var days = parseInt(req.query.days || "30");
  var since = new Date();
  since.setDate(since.getDate() - days);
  var sinceStr = since.toISOString().split("T")[0];

  try {
    var results = await Promise.all([
      fetchPages("/products", {}, apiKey, 20),
      fetchPages("/orders", {
        created_at_min: sinceStr,
        include: "products,buyer,status,manager,source"
      }, apiKey, 20)
    ]);
    var products = results[0];
    var orders = results[1];

    // Категории
    var catMap = {};
    products.forEach(function(p) {
      var cat = (p.category && typeof p.category === "object") ? (p.category.name || "Без категорії") : (p.category || "Без категорії");
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      if ((p.quantity || p.in_stock || 0) > 0) catMap[cat].inStock++;
    });
    var categories = Object.entries(catMap)
      .sort(function(a, b) { return b[1].count - a[1].count; })
      .slice(0, 10)
      .map(function(e) { return { name: e[0], count: e[1].count, inStock: e[1].inStock }; });

    // Топ товаров
    var salesMap = {};
    orders.forEach(function(o) {
      var items = o.products || o.items || [];
      items.forEach(function(item) {
        var name = item.name || (item.product && item.product.name) || (item.offer && item.offer.name) || "—";
        var key = item.product_id || item.offer_id || name;
        if (!salesMap[key]) salesMap[key] = { name: name, qty: 0, revenue: 0 };
        salesMap[key].qty += parseInt(item.quantity || 1);
        salesMap[key].revenue += parseFloat(item.price || 0) * parseInt(item.quantity || 1);
      });
    });
    var topProducts = Object.values(salesMap)
      .sort(function(a, b) { return b.qty - a.qty; })
      .slice(0, 10);

    // Продажи по дням
    var dayMap = {};
    orders.forEach(function(o) {
      var d = (o.created_at || "").substring(0, 10);
      if (!d) return;
      if (!dayMap[d]) dayMap[d] = { orders: 0, revenue: 0 };
      dayMap[d].orders++;
      dayMap[d].revenue += parseFloat(o.total_price || o.sum || 0);
    });
    var salesByDay = Object.entries(dayMap)
      .sort(function(a, b) { return a[0].localeCompare(b[0]); })
      .map(function(e) { return Object.assign({ date: e[0] }, e[1]); });

    // Ценовые диапазоны
    var ranges = [
      { label: "< 50",    min: 0,    max: 50 },
      { label: "50–200",  min: 50,   max: 200 },
      { label: "200–500", min: 200,  max: 500 },
      { label: "500–1k",  min: 500,  max: 1000 },
      { label: "> 1000",  min: 1000, max: Infinity },
    ];
    ranges.forEach(function(r) {
      r.count = products.filter(function(p) {
        var price = parseFloat(p.price || 0);
        return price >= r.min && price < r.max;
      }).length;
    });

    // Статусы заказов
    var statusMap = {};
    orders.forEach(function(o) {
      var s = (o.status && typeof o.status === "object") ? (o.status.name || "Невідомо") : (o.status || "Невідомо");
      statusMap[s] = (statusMap[s] || 0) + 1;
    });
    var orderStatuses = Object.entries(statusMap)
      .sort(function(a, b) { return b[1] - a[1]; })
      .map(function(e) { return { name: e[0], count: e[1] }; });

    // KPI
    var inStock = products.filter(function(p) { return (p.quantity || p.in_stock || 0) > 0; }).length;
    var totalRevenue = orders.reduce(function(s, o) { return s + parseFloat(o.total_price || o.sum || 0); }, 0);
    var avgOrder = orders.length ? totalRevenue / orders.length : 0;

    return res.status(200).json({
      updatedAt: new Date().toISOString(),
      days: days,
      kpi: {
        totalProducts: products.length,
        inStock: inStock,
        outOfStock: products.length - inStock,
        totalOrders: orders.length,
        totalRevenue: totalRevenue,
        avgOrder: avgOrder,
      },
      categories: categories,
      topProducts: topProducts,
      salesByDay: salesByDay,
      priceRanges: ranges,
      orderStatuses: orderStatuses,
    });

  } catch(err) {
    var status = err.message === "UNAUTHORIZED" ? 401 : 500;
    return res.status(status).json({ error: err.message });
  }
};
