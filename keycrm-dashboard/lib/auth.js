function checkCronAuth(req) {
  const expected = process.env.CRON_SECRET;
  if (!expected) return { ok: false, status: 500, error: "CRON_SECRET не налаштовано" };
  const header = req.headers["authorization"] || req.headers["Authorization"] || "";
  const fromQuery = req.query && req.query.secret;
  const token = header.startsWith("Bearer ") ? header.slice(7) : fromQuery;
  if (!token || token !== expected) {
    return { ok: false, status: 401, error: "Unauthorized" };
  }
  return { ok: true };
}

function checkDashboardToken(req) {
  const expected = process.env.DASHBOARD_TOKEN;
  if (!expected) return { ok: true };
  const fromQuery = req.query && req.query.token;
  const header = req.headers["x-dashboard-token"] || "";
  const token = fromQuery || header;
  if (!token || token !== expected) {
    return { ok: false, status: 401, error: "Unauthorized" };
  }
  return { ok: true };
}

module.exports = { checkCronAuth, checkDashboardToken };
