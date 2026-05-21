// Об'єднаний endpoint для документації показників.
// GET /api/docs?type=clients  → довідка по клієнтській аналітиці
// GET /api/docs?type=sku      → довідка по SKU-аналітиці
//
// Об'єднано в один файл, щоб укластися в 12-функційний ліміт Hobby-плану
// Vercel. Джерела правди — lib/clients-docs.js, lib/sku-docs.js.

const { checkDashboardToken } = require("../lib/auth");
const clientsDocs = require("../lib/clients-docs");
const skuDocs = require("../lib/sku-docs");

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  const type = (req.query && req.query.type) || "clients";

  res.setHeader("Cache-Control", "public, max-age=60, s-maxage=60");

  if (type === "sku") {
    return res.status(200).json({
      type: "sku",
      metrics: skuDocs.METRIC_DOCS,
      statuses: skuDocs.STATUS_LEGEND,
      bcg_roles: skuDocs.BCG_LEGEND,
      tags: skuDocs.TAG_LEGEND,
      general: skuDocs.GENERAL_NOTES,
    });
  }

  // default: clients
  return res.status(200).json({
    type: "clients",
    metrics: clientsDocs.METRIC_DOCS,
    general: clientsDocs.GENERAL_NOTES,
  });
};
