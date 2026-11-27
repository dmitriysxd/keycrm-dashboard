// /api/report/excel — красивий Excel-звіт по результативності реклами.
//
// Бере дані з уже готових ендпоінтів дашборда (type=campaigns / marketing /
// assist) — внутрішнім self-fetch по публічному URL (як робить ingest), щоб не
// дублювати важку агрегацію. Збирає книгу через exceljs зі стилями, числовими
// форматами, умовним кольором ROAS і data-барами.
//
// GET /api/report/excel?from=YYYY-MM-DD&to=YYYY-MM-DD   (DASHBOARD_TOKEN)
//   Без from/to — весь час. Повертає .xlsx як attachment.
//
// ПОВНИЙ варіант: включає собівартість/маржу/прибуток і розбивку опт/розниця.

const ExcelJS = require("exceljs");
const { checkDashboardToken } = require("../../lib/auth");

// ── Палітра / стилі ──────────────────────────────────────────────
const C = {
  dark: "FF2B2117",     // шапка таблиць (темно-коричневий)
  gold: "FFC9A227",     // акцент
  goldLt: "FFF2E8C8",   // світла заливка секцій
  goldText: "FFE8C766",
  white: "FFFFFFFF",
  green: "FF2E7D32",
  red: "FFC62828",
  gray: "FF8A8A8A",
  zebra: "FFFBF8F0",    // світлий рядок
  border: "FFE2D9C4",
};
const NF = {
  money: '#,##0" ₴"',
  money0: '#,##0" ₴"',
  pct: '0.0"%"',
  roas: '0.00"×"',
  int: "#,##0",
  float2: "0.00",
};
const thin = { style: "thin", color: { argb: C.border } };
const ALL_BORDER = { top: thin, left: thin, bottom: thin, right: thin };

function baseUrl(req) {
  if (process.env.PUBLIC_URL) return process.env.PUBLIC_URL.replace(/\/+$/, "");
  if (process.env.VERCEL_PROJECT_PRODUCTION_URL) return "https://" + process.env.VERCEL_PROJECT_PRODUCTION_URL;
  if (process.env.VERCEL_URL) return "https://" + process.env.VERCEL_URL;
  const proto = (req.headers["x-forwarded-proto"] || "https").split(",")[0].trim();
  return proto + "://" + req.headers["host"];
}

async function fetchJson(url) {
  const r = await fetch(url, { headers: { Accept: "application/json" } });
  const t = await r.text();
  let j = null;
  try { j = JSON.parse(t); } catch (e) { throw new Error("Невалідна відповідь: " + t.slice(0, 160)); }
  if (!r.ok) throw new Error((j && j.error) || ("HTTP " + r.status));
  return j;
}

// Дата YYYY-MM-DD → DD.MM.YYYY
function dmy(s) { if (!s) return ""; const p = String(s).split("-"); return p.length === 3 ? p[2] + "." + p[1] + "." + p[0] : s; }

// ── Хелпери побудови ─────────────────────────────────────────────
function styleHeaderRow(row) {
  row.eachCell((cell) => {
    cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: C.dark } };
    cell.font = { bold: true, color: { argb: C.goldText }, size: 10 };
    cell.alignment = { horizontal: "center", vertical: "middle", wrapText: true };
    cell.border = ALL_BORDER;
  });
  row.height = 30;
}

// columns: [{ h, key, w, nf, type }]  type: text|money|int|pct|roas|roasCond
// roasCond порівнює значення з row['__be_'+key] (беззбитковий ROAS) → колір.
function writeTable(ws, startRow, columns, rows, opts) {
  opts = opts || {};
  const hdr = ws.getRow(startRow);
  columns.forEach((c, i) => { hdr.getCell(i + 1).value = c.h; });
  styleHeaderRow(hdr);
  columns.forEach((c, i) => { ws.getColumn(i + 1).width = c.w || 12; });

  let r = startRow;
  rows.forEach((row, ri) => {
    r++;
    const xr = ws.getRow(r);
    columns.forEach((c, i) => {
      const cell = xr.getCell(i + 1);
      let v = row[c.key];
      if (v === undefined) v = null;
      cell.value = (v === null || v === "") ? (c.type === "text" ? (v || "") : null) : v;
      if (c.nf) cell.numFmt = c.nf;
      cell.border = ALL_BORDER;
      cell.alignment = { horizontal: c.type === "text" ? "left" : "right", vertical: "middle", wrapText: c.type === "text" };
      cell.font = { size: 10 };
      if (c.type === "roasCond" && typeof v === "number") {
        const be = row["__be_" + c.key];
        if (typeof be === "number" && be > 0) cell.font = { size: 10, bold: true, color: { argb: v >= be ? C.green : C.red } };
        else cell.font = { size: 10, bold: true };
      }
      if (row.__bold) cell.font = Object.assign({}, cell.font, { bold: true });
    });
    // зебра
    if (!row.__bold && ri % 2 === 1) xr.eachCell((cell) => { if (!cell.fill || cell.fill.pattern !== "solid") cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: C.zebra } }; });
    if (row.__band) {
      xr.eachCell((cell) => {
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: C.goldLt } };
        cell.font = { bold: true, size: 10, color: { argb: C.dark } };
      });
    }
    xr.height = 18;
  });
  // data-бари на колонці виручки, якщо задано
  if (opts.dataBarKey) {
    const ci = columns.findIndex((c) => c.key === opts.dataBarKey);
    if (ci >= 0 && r > startRow) {
      const col = String.fromCharCode(65 + ci);
      ws.addConditionalFormatting({
        ref: `${col}${startRow + 1}:${col}${r}`,
        rules: [{ type: "dataBar", gradient: false, cfvo: [{ type: "min" }, { type: "max" }], color: { argb: C.gold }, minLength: 0, maxLength: 100 }],
      });
    }
  }
  ws.views = [{ state: "frozen", ySplit: startRow }];
  return r;
}

// Рядок з кампанії/каналу → об'єкт для таблиці (повний набір).
function campRow(n, extra) {
  const cpc = (n.spend > 0 && n.clicks > 0) ? Math.round(n.spend / n.clicks) : null;
  return Object.assign({
    name: n.name,
    spend: n.spend != null ? n.spend : (n.cost || 0),
    impressions: n.impressions || 0,
    clicks: n.clicks || 0,
    ctr: n.ctr,
    cpc,
    orders: (n.orders != null) ? n.orders : ((n.done || 0) + (n.progress || 0)),
    revenue: n.revenue || 0,
    revenue_opt: n.revenue_opt || 0,
    revenue_retail: n.revenue_retail || 0,
    roas: n.roas,
    roas_opt: n.roas_opt,
    roas_retail: n.roas_retail,
    roas_new_opt: n.roas_new_opt != null ? n.roas_new_opt : null,
    new_orders: n.new_orders || 0,
    new_opt: n.new_opt || 0,
    repeat_orders: n.repeat_orders || 0,
    cac: n.cac,
    margin: n.margin,
    net_profit: n.net_profit,
    __be_roas: n.breakeven_roas,
    __be_roas_opt: n.breakeven_roas_opt,
  }, extra || {});
}

const CAMP_COLS = [
  { h: "Кампанія / канал", key: "name", w: 34, type: "text" },
  { h: "Витрата", key: "spend", w: 13, nf: NF.money, type: "money" },
  { h: "Показы", key: "impressions", w: 12, nf: NF.int, type: "int" },
  { h: "Кліки", key: "clicks", w: 10, nf: NF.int, type: "int" },
  { h: "CTR", key: "ctr", w: 8, nf: NF.pct, type: "pct" },
  { h: "CPC", key: "cpc", w: 10, nf: NF.money, type: "money" },
  { h: "Заказы", key: "orders", w: 9, nf: NF.int, type: "int" },
  { h: "Виручка", key: "revenue", w: 14, nf: NF.money, type: "money" },
  { h: "Виручка опт", key: "revenue_opt", w: 14, nf: NF.money, type: "money" },
  { h: "Виручка розн", key: "revenue_retail", w: 14, nf: NF.money, type: "money" },
  { h: "ROAS", key: "roas", w: 9, nf: NF.roas, type: "roasCond" },
  { h: "ROAS опт", key: "roas_opt", w: 9, nf: NF.roas, type: "roasCond" },
  { h: "ROAS розн", key: "roas_retail", w: 9, nf: NF.roas, type: "roas" },
  { h: "ROAS нові опт", key: "roas_new_opt", w: 10, nf: NF.roas, type: "roas" },
  { h: "Нові", key: "new_orders", w: 8, nf: NF.int, type: "int" },
  { h: "Нові опт", key: "new_opt", w: 9, nf: NF.int, type: "int" },
  { h: "Повторні", key: "repeat_orders", w: 9, nf: NF.int, type: "int" },
  { h: "CAC", key: "cac", w: 11, nf: NF.money, type: "money" },
  { h: "Маржа", key: "margin", w: 9, nf: NF.pct, type: "pct" },
  { h: "Прибуток", key: "net_profit", w: 14, nf: NF.money, type: "money" },
];

module.exports = async (req, res) => {
  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

  try {
    const reDate = /^\d{4}-\d{2}-\d{2}$/;
    let from = (req.query && req.query.from) || null;
    let to = (req.query && req.query.to) || null;
    if (!reDate.test(String(from))) from = null;
    if (!reDate.test(String(to))) to = null;
    const hasRange = !!(from && to);
    const periodLabel = hasRange ? (dmy(from) + " – " + dmy(to)) : "Весь час";

    const base = baseUrl(req);
    const qs = hasRange ? `&from=${from}&to=${to}` : "";
    const tok = process.env.DASHBOARD_TOKEN ? `&token=${encodeURIComponent(process.env.DASHBOARD_TOKEN)}` : "";
    const A = (type) => `${base}/api/analytics?type=${type}${qs}${tok}`;
    const [camp, mkt, assist] = await Promise.all([
      fetchJson(A("campaigns")),
      fetchJson(A("marketing")),
      fetchJson(A("assist")),
    ]);

    const wb = new ExcelJS.Workbook();
    wb.creator = "Bijboom-opt dashboard";
    wb.created = new Date();

    // ═══ Лист 1: Підсумок ═══
    const s1 = wb.addWorksheet("Підсумок", { properties: { defaultColWidth: 16 } });
    s1.mergeCells("A1:D1");
    s1.getCell("A1").value = "Звіт по рекламі · Bijboom-opt";
    s1.getCell("A1").font = { bold: true, size: 18, color: { argb: C.dark } };
    s1.getCell("A1").alignment = { vertical: "middle" };
    s1.getRow(1).height = 30;
    s1.mergeCells("A2:D2");
    s1.getCell("A2").value = "Період: " + periodLabel;
    s1.getCell("A2").font = { size: 12, italic: true, color: { argb: C.gray } };
    s1.getRow(2).height = 20;

    const t = mkt.totals || {};
    const kpis = [
      ["Витрата на рекламу", t.cost, NF.money],
      ["Виручка (всього)", t.revenue, NF.money],
      ["  — опт", t.revenue_opt, NF.money],
      ["  — розниця", t.revenue_retail, NF.money],
      ["Частка опту", t.opt_share, NF.pct],
      ["ROAS (всього)", t.roas, NF.roas],
      ["  — опт", t.roas_opt, NF.roas],
      ["  — розниця", t.roas_retail, NF.roas],
      ["Беззбитковий ROAS", t.breakeven_roas, NF.roas],
      ["Заказы активні", (t.done || 0) + (t.progress || 0), NF.int],
      ["Нові клієнти", t.new_orders, NF.int],
      ["  — опт", t.new_opt, NF.int],
      ["Повторні заказы", t.repeat_orders, NF.int],
      ["CAC (ціна нового клієнта)", t.cac, NF.money],
      ["Маржа", t.margin, NF.pct],
      ["Чистий прибуток (після реклами)", t.net_profit, NF.money],
    ];
    let rr = 4;
    const kh = s1.getRow(rr);
    kh.getCell(1).value = "Показник"; kh.getCell(2).value = "Значення";
    styleHeaderRow(kh);
    s1.getColumn(1).width = 34; s1.getColumn(2).width = 18;
    kpis.forEach((k, i) => {
      rr++;
      const row = s1.getRow(rr);
      const c1 = row.getCell(1), c2 = row.getCell(2);
      c1.value = k[0];
      c2.value = (k[1] == null ? null : k[1]);
      c2.numFmt = k[2];
      const sub = k[0].startsWith("  ");
      c1.font = { size: 10, bold: !sub, color: { argb: sub ? C.gray : C.dark } };
      c2.font = { size: 10, bold: !sub };
      c2.alignment = { horizontal: "right" };
      c1.border = ALL_BORDER; c2.border = ALL_BORDER;
      if (i % 2 === 1) { c1.fill = c2.fill = { type: "pattern", pattern: "solid", fgColor: { argb: C.zebra } }; }
      row.height = 18;
    });

    // Топ-5 кампаній по виручці
    const allCamps = [];
    for (const ch of (camp.channels || [])) for (const c of (ch.campaigns || [])) if (!c.is_other) allCamps.push(Object.assign({ _channel: ch.channel }, c));
    allCamps.sort((a, b) => (b.revenue || 0) - (a.revenue || 0));
    const top = allCamps.slice(0, 5);
    if (top.length) {
      rr += 2;
      s1.mergeCells(`A${rr}:D${rr}`);
      s1.getCell(`A${rr}`).value = "Топ-5 кампаній по виручці";
      s1.getCell(`A${rr}`).font = { bold: true, size: 12, color: { argb: C.dark } };
      rr++;
      const th = s1.getRow(rr);
      ["Кампанія", "Канал", "Витрата", "Виручка", "ROAS"].forEach((h, i) => th.getCell(i + 1).value = h);
      styleHeaderRow(th);
      [34, 14, 14, 16, 10].forEach((w, i) => { if (s1.getColumn(i + 1).width < w) s1.getColumn(i + 1).width = w; });
      top.forEach((c) => {
        rr++;
        const row = s1.getRow(rr);
        row.getCell(1).value = c.name;
        row.getCell(2).value = c._channel;
        row.getCell(3).value = c.spend || 0; row.getCell(3).numFmt = NF.money;
        row.getCell(4).value = c.revenue || 0; row.getCell(4).numFmt = NF.money;
        row.getCell(5).value = c.roas; row.getCell(5).numFmt = NF.roas;
        for (let i = 1; i <= 5; i++) { row.getCell(i).border = ALL_BORDER; row.getCell(i).font = { size: 10 }; }
        row.getCell(1).alignment = { horizontal: "left" };
        row.getCell(2).alignment = { horizontal: "left" };
      });
    }

    // ═══ Лист 2: Кампанії (Meta + Google, з бендами каналів) ═══
    const s2 = wb.addWorksheet("Кампанії");
    const campRows = [];
    for (const ch of (camp.channels || [])) {
      const tt = ch.totals || {};
      campRows.push(Object.assign(campRow(Object.assign({}, tt, { name: "▸ " + ch.channel + " — разом" })), { __band: true }));
      for (const c of (ch.campaigns || [])) campRows.push(campRow(c));
    }
    if (camp.totals) campRows.push(Object.assign(campRow(Object.assign({}, camp.totals, { name: "ВСЬОГО" })), { __bold: true }));
    writeTable(s2, 1, CAMP_COLS, campRows, { dataBarKey: "revenue" });

    // ═══ Лист 3: Канали (всі канали, last-touch, з ручними витратами) ═══
    const s3 = wb.addWorksheet("Канали");
    const chRows = (mkt.channels || []).map((c) => campRow(Object.assign({}, c, { name: c.channel })));
    if (mkt.totals) chRows.push(Object.assign(campRow(Object.assign({}, mkt.totals, { name: "ВСЬОГО" })), { __bold: true }));
    // Канальний лист: прибираємо колонку «ROAS нові опт» (на рівні каналу менш показова)
    const CH_COLS = CAMP_COLS.filter((c) => c.key !== "roas_new_opt");
    CH_COLS[0] = Object.assign({}, CH_COLS[0], { h: "Канал" });
    writeTable(s3, 1, CH_COLS, chRows, { dataBarKey: "revenue" });

    // ═══ Лист 4: Привів vs Закрив (first-touch assist) ═══
    const s4 = wb.addWorksheet("Привів vs Закрив");
    s4.mergeCells("A1:G1");
    s4.getCell("A1").value = "Привів vs Закрив — хто формує інтерес, а хто закриває (first-touch)";
    s4.getCell("A1").font = { bold: true, size: 13, color: { argb: C.dark } };
    s4.getRow(1).height = 24;
    s4.mergeCells("A2:G2");
    s4.getCell("A2").value = `Покриття first-touch: ${assist.coverage_pct || 0}% (${assist.matched || 0} з ${assist.total_active || 0} активних клієнтів). ` +
      "Привів = перший візит на сайт був із каналу; Закрив = канал був останнім при купівлі; Коеф = привів ÷ закрив (>1.3 відкривач, <0.77 закривач).";
    s4.getCell("A2").font = { size: 10, italic: true, color: { argb: C.gray } };
    s4.getCell("A2").alignment = { wrapText: true, vertical: "top" };
    s4.getRow(2).height = 44;
    const roleUk = { opener: "Відкривач", closer: "Закривач", balanced: "Збалансований" };
    const ASSIST_COLS = [
      { h: "Канал", key: "channel", w: 30, type: "text" },
      { h: "Привів (клієнтів)", key: "led", w: 16, nf: NF.int, type: "int" },
      { h: "Виручка привів", key: "led_rev", w: 16, nf: NF.money, type: "money" },
      { h: "Закрив (клієнтів)", key: "closed", w: 16, nf: NF.int, type: "int" },
      { h: "Виручка закрив", key: "closed_rev", w: 16, nf: NF.money, type: "money" },
      { h: "Коеф", key: "coefTxt", w: 9, type: "text" },
      { h: "Роль", key: "roleTxt", w: 16, type: "text" },
    ];
    const aRows = (assist.rows || []).map((r) => Object.assign({}, r, {
      coefTxt: r.coef == null ? "∞" : r.coef.toFixed(2),
      roleTxt: roleUk[r.role] || r.role,
    }));
    writeTable(s4, 3, ASSIST_COLS, aRows, { dataBarKey: "led_rev" });

    // ── віддаємо файл ──
    const buf = await wb.xlsx.writeBuffer();
    const stamp = hasRange ? `${from}_${to}` : "vsya-istoriya";
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="reklama_${stamp}.xlsx"`);
    res.setHeader("Cache-Control", "no-store");
    return res.status(200).send(Buffer.from(buf));
  } catch (e) {
    return res.status(500).json({ error: (e && e.message) || String(e) });
  }
};
