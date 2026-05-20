// Документація показників SKU-аналітики у вигляді JSON.
// Джерело правди — lib/sku-docs.js. UI підвантажує і рендерить у модалі
// довідки, який відкривається кнопкою "?" на сторінці SKU-аналітики.

const { checkDashboardToken } = require("../lib/auth");
const { METRIC_DOCS, STATUS_LEGEND, GENERAL_NOTES } = require("../lib/sku-docs");

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  res.setHeader("Cache-Control", "public, max-age=60, s-maxage=60");
  return res.status(200).json({
    metrics: METRIC_DOCS,
    statuses: STATUS_LEGEND,
    general: GENERAL_NOTES,
  });
};
