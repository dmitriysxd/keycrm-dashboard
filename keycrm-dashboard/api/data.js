const BASE = "https://openapi.keycrm.app/v1";

async function fetchPages(endpoint, params, apiKey, maxPages = 15) {
  const headers = {
    Authorization: `Bearer ${apiKey}`,
    "Content-Type": "application/json",
    Accept: "application/json",
  };
  let all = [];
  let page = 1;
  while (page <= maxPages) {
    const url = new URL(`${BASE}${endpoint}`);
    Object.entries({ ...params, page, limit: 50 }).forEach(([k, v]) =>
      url.searchParams.set(k, v)
    );
    const res = await fetch(url.toString(), { headers });
    if (!res.ok) {
      if (res.status === 401) throw new Error("UNAUTHORIZED");
      throw new Error(`HTTP ${res.status}`);
    }
    const json = await res.json();
    const items = json.data || [];
    all = all.concat(items);
    const lastPage = json.meta?.last_page || 1;
    if (page >= lastPage || items.length === 0) break;
    page++;
  }
  return all;
}

export default async function handler(req, res) {
  // CORS headers
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const apiKey = process.env.KEYCRM_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: "KEYCRM_API_KEY не налаштовано в змінних середовища Vercel" });
  }

  const days = parseInt(req.query.days || "30");
  const since = new Date();
  since.setDate(since.getDate() - days);
  const sinceStr = since.toISOString().split("T")[0];

  try {
    const [products, orders] = await Promise.all([
      fetchPages("/products", {}, apiKey, 20),
      fetchPages("/orders", {
        created_at_min: sinceStr,
        include: "products,buyer,status,manager,source",
      }, apiKey, 20),
    ]);

    // --- Аналитика ---

    // Категории
    const catMap = {};
    products.forEach((p) => {
      const cat =
        (typeof p.category === "object" ? p.category?.name : p.category) ||
        "Без категорії";
      if (!catMap[cat]) catMap[cat] = { count: 0, inStock: 0 };
      catMap[cat].count++;
      if ((p.quantity || p.in_stock || 0) > 0) catMap[cat].inStock++;
    });
    const categories = Object.entries(catMap)
      .sort((a, b) => b[1].count - a[1].count)
      .slice(0, 10)
      .map(([name, d]) => ({ name, count: d.count, inStock: d.inStock }));

    // Продажи по товарам
    const salesMap = {};
    orders.forEach((o) => {
      (o.products || o.items || []).forEach((item) => {
        const name =
          item.name || item.product?.name || item.offer?.name || "—";
        const key = item.product_id || item.offer_id || name;
        if (!salesMap[key]) salesMap[key] = { name, qty: 0, revenue: 0 };
        salesMap[key].qty += parseInt(item.quantity || 1);
        salesMap[key].revenue +=
          parseFloat(item.price || 0) * parseInt(item.quantity || 1);
      });
    });
    const topProducts = Object.values(salesMap)
      .sort((a, b) => b.qty - a.qty)
      .slice(0, 10);

    // Продажи по дням
    const dayMap = {};
    orders.forEach((o) => {
      const d = (o.created_at || "").substring(0, 10);
      if (!d) return;
      if (!dayMap[d]) dayMap[d] = { orders: 0, revenue: 0 };
      dayMap[d].orders++;
      dayMap[d].revenue += parseFloat(o.total_price || o.sum || 0);
    });
    const salesByDay = Object.entries(dayMap)
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([date, d]) => ({ date, ...d }));

    // Ценовые диапазоны
    const ranges = [
      { label: "< 50",    min: 0,    max: 50 },
      { label: "50–200",  min: 50,   max: 200 },
      { label: "200–500", min: 200,  max: 500 },
      { label: "500–1k",  min: 500,  max: 1000 },
      { label: "> 1000",  min: 1000, max: Infinity },
    ];
    ranges.forEach((r) => {
      r.count = products.filter((p) => {
        const price = parseFloat(p.price || 0);
        return price >= r.min && price < r.max;
      }).length;
    });

    // Статусы заказов
    const statusMap = {};
    orders.forEach((o) => {
      const s =
        (typeof o.status === "object" ? o.status?.name : o.status) ||
        "Невідомо";
      statusMap[s] = (statusMap[s] || 0) + 1;
    });
    const orderStatuses = Object.entries(statusMap)
      .sort((a, b) => b[1] - a[1])
      .map(([name, count]) => ({ name, count }));

    // KPI
    const inStock = products.filter(
      (p) => (p.quantity || p.in_stock || 0) > 0
    ).length;
    const totalRevenue = orders.reduce(
      (s, o) => s + parseFloat(o.total_price || o.sum || 0),
      0
    );
    const avgOrder = orders.length ? totalRevenue / orders.length : 0;

    return res.status(200).json({
      updatedAt: new Date().toISOString(),
      days,
      kpi: {
        totalProducts: products.length,
        inStock,
        outOfStock: products.length - inStock,
        totalOrders: orders.length,
        totalRevenue,
        avgOrder,
      },
      categories,
      topProducts,
      salesByDay,
      priceRanges: ranges,
      orderStatuses,
    });
  } catch (err) {
    const status = err.message === "UNAUTHORIZED" ? 401 : 500;
    return res.status(status).json({ error: err.message });
  }
}
