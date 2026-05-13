const fs = require("fs");
const os = require("os");
const path = require("path");

function loadDotEnv() {
  const envPath = path.join(__dirname, "..", ".env");
  if (!fs.existsSync(envPath)) {
    return;
  }

  const content = fs.readFileSync(envPath, "utf8");
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }

    const separatorIndex = line.indexOf("=");
    if (separatorIndex === -1) {
      continue;
    }

    const key = line.slice(0, separatorIndex).trim();
    let value = line.slice(separatorIndex + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (!process.env[key]) {
      process.env[key] = value;
    }
  }
}

function hasLocalAirflowNetwork() {
  const interfaces = os.networkInterfaces();
  return Object.values(interfaces)
    .flat()
    .filter(Boolean)
    .some((item) => item.address && item.address.includes("1.146"));
}

function defaultBaseUrl() {
  return hasLocalAirflowNetwork()
    ? "http://127.0.0.1:8080"
    : "http://34.142.225.103:8080";
}

function defaultUsername(baseUrl) {
  if (baseUrl.includes("10.58.6.138")) {
    return "yara";
  }
  return "shaun";
}

function defaultPassword(baseUrl) {
  if (baseUrl.includes("10.58.6.138")) {
    return "lovitoyara";
  }
  return "shaun2024";
}

function normalizeBaseUrl(baseUrl) {
  return String(baseUrl || defaultBaseUrl()).trim().replace(/\/+$/, "");
}

function slugify(value, fallback) {
  const slug = String(value || "")
    .toLowerCase()
    .replace(/https?:\/\//g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug || fallback;
}

function normalizeAirflowService(rawService = {}, index = 0) {
  const baseUrl = normalizeBaseUrl(rawService.baseUrl || rawService.url || rawService.host);
  const username = String(rawService.username || defaultUsername(baseUrl)).trim();
  const name = String(rawService.name || rawService.label || baseUrl).trim();
  const alias = String(rawService.alias || rawService.label || "").trim();
  const id = String(rawService.id || slugify(`${name}-${username}`, `airflow-${index + 1}`)).trim();
  return {
    id,
    name,
    alias,
    label: alias || String(rawService.label || `${baseUrl} · ${username}`).trim(),
    baseUrl,
    username,
    password: String(rawService.password || defaultPassword(baseUrl)),
    owner: String(rawService.owner || process.env.AIRFLOW_OWNER || "Shaun").trim(),
    requestTimeoutMs: Number(
      rawService.requestTimeoutMs || process.env.AIRFLOW_REQUEST_TIMEOUT_MS || 60000,
    ),
  };
}

function ensureUniqueServiceIds(services) {
  const seen = new Set();
  return services.map((service, index) => {
    const baseId = service.id || slugify(`${service.name}-${service.username}`, `airflow-${index + 1}`);
    let nextId = baseId;
    let suffix = 2;
    while (seen.has(nextId)) {
      nextId = `${baseId}-${suffix}`;
      suffix += 1;
    }
    seen.add(nextId);
    return nextId === service.id ? service : { ...service, id: nextId };
  });
}

function legacyAirflowService() {
  const baseUrl = normalizeBaseUrl(process.env.AIRFLOW_BASE_URL || defaultBaseUrl());
  return normalizeAirflowService({
    id: process.env.AIRFLOW_SERVICE_ID || "main",
    name: process.env.AIRFLOW_SERVICE_NAME || "34.142.225.103",
    label: process.env.AIRFLOW_SERVICE_LABEL || `${baseUrl} · ${process.env.AIRFLOW_USERNAME || defaultUsername(baseUrl)}`,
    baseUrl,
    username: process.env.AIRFLOW_USERNAME || defaultUsername(baseUrl),
    password: process.env.AIRFLOW_PASSWORD || defaultPassword(baseUrl),
    owner: process.env.AIRFLOW_OWNER || "Shaun",
    requestTimeoutMs: process.env.AIRFLOW_REQUEST_TIMEOUT_MS || 60000,
  });
}

function parseAirflowServices() {
  const rawList = process.env.AIRFLOW_LIST || process.env.AIRFLOW_SERVICES || "";
  if (!rawList.trim()) {
    return [legacyAirflowService()];
  }

  try {
    const parsed = JSON.parse(rawList);
    const list = Array.isArray(parsed) ? parsed : [parsed];
    const services = ensureUniqueServiceIds(
      list.map(normalizeAirflowService).filter((service) => service.baseUrl),
    );
    return services.length ? services : [legacyAirflowService()];
  } catch (error) {
    console.warn(`AIRFLOW_LIST 解析失败，已回退到单服务配置: ${error.message}`);
    return [legacyAirflowService()];
  }
}

loadDotEnv();

const airflowServices = parseAirflowServices();
const defaultAirflowServiceId = String(
  process.env.AIRFLOW_ACTIVE_SERVICE_ID ||
    process.env.AIRFLOW_DEFAULT_SERVICE_ID ||
    airflowServices[0].id,
);
const activeAirflowService =
  airflowServices.find((service) => service.id === defaultAirflowServiceId) || airflowServices[0];

module.exports = {
  app: {
    name: "Airflow GUI Tool",
    title: "Airflow 精简工具",
  },
  airflow: activeAirflowService,
  airflowServices,
  defaultAirflowServiceId: activeAirflowService.id,
};
