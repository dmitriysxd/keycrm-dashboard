// Видає документацію показників аналітики як JSON. UI підвантажує цей файл
// у модал довідки, який відкривається кнопкою "?" на сторінці "Опт-клієнти".
// Джерело правди — lib/clients-docs.js.

const { checkDashboardToken } = require("../lib/auth");
const { METRIC_DOCS, GENERAL_NOTES } = require("../lib/clients-docs");

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  const auth = checkDashboardToken(req);
  if (!auth.ok) return res.status(auth.status).json({ status: auth.status, error: auth.error });

  // 1 година кеша на стороні CDN — документи рідко змінюються.
  res.setHeader("Cache-Control", "public, max-age=3600, s-maxage=3600");
  return res.status(200).json({ metrics: METRIC_DOCS, general: GENERAL_NOTES });
};
