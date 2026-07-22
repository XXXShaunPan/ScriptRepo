const fs = require("fs");
const os = require("os");
const path = require("path");

function parseEnvValue(value) {
  const trimmed = String(value || "").trim();
  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith("'") && trimmed.endsWith("'"))
  ) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function looksLikeJson(value) {
  const trimmed = parseEnvValue(value);
  return trimmed.startsWith("[") || trimmed.startsWith("{");
}

function canParseJson(value) {
  try {
    JSON.parse(parseEnvValue(value));
    return true;
  } catch {
    return false;
  }
}

function parseEnvContent(content) {
  const env = {};
  let pending = null;

  for (const rawLine of String(content || "").split(/\r?\n/)) {
    if (pending) {
      pending.value += `\n${rawLine}`;
      if (!looksLikeJson(pending.value) || canParseJson(pending.value)) {
        env[pending.key] = parseEnvValue(pending.value);
        pending = null;
      }
      continue;
    }

    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }

    const separatorIndex = line.indexOf("=");
    if (separatorIndex === -1) {
      continue;
    }

    const key = line.slice(0, separatorIndex).trim();
    const value = line.slice(separatorIndex + 1).trim();
    if (looksLikeJson(value) && !canParseJson(value)) {
      pending = { key, value };
      continue;
    }
    env[key] = parseEnvValue(value);
  }

  if (pending) {
    env[pending.key] = parseEnvValue(pending.value);
  }

  return env;
}

function envFileCandidates(fileName) {
  const explicitPath =
    fileName === ".env"
      ? process.env.AIRFLOW_WEBAPP_ENV_PATH
      : process.env.DAG_CONF_ENV_PATH;
  const candidates = [
    path.join(__dirname, "..", fileName),
    path.join(process.cwd(), fileName),
    explicitPath || "",
  ];
  return [
    ...new Set(candidates.filter(Boolean).map((item) => path.resolve(item))),
  ];
}

function parseEnvFile(fileName) {
  const env = {};
  for (const envPath of envFileCandidates(fileName)) {
    if (fs.existsSync(envPath)) {
      Object.assign(env, parseEnvContent(fs.readFileSync(envPath, "utf8")));
    }
  }
  return env;
}

function loadDotEnv() {
  const env = parseEnvFile(".env");
  for (const [key, value] of Object.entries(env)) {
    if (!process.env[key]) {
      process.env[key] = value;
    }
  }
}

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  return /^(1|true|yes|y|on)$/i.test(String(value).trim());
}

function parsePositiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function splitCsv(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeBaseUrl(baseUrl) {
  return String(baseUrl || "http://127.0.0.1:8080")
    .trim()
    .replace(/\/+$/, "");
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
  const baseUrl = normalizeBaseUrl(
    rawService.baseUrl || rawService.url || rawService.host,
  );
  const username = String(rawService.username || "").trim();
  const name = String(rawService.name || rawService.label || baseUrl).trim();
  const alias = String(rawService.alias || rawService.label || "").trim();
  const id = String(
    rawService.id || slugify(`${name}-${username}`, `airflow-${index + 1}`),
  ).trim();
  return {
    id,
    name,
    alias,
    label:
      alias || String(rawService.label || `${baseUrl} · ${username}`).trim(),
    baseUrl,
    username,
    password: String(rawService.password || ""),
    owner: String(
      rawService.owner || process.env.AIRFLOW_OWNER || "Shaun",
    ).trim(),
    requestTimeoutMs: Number(
      rawService.requestTimeoutMs ||
        process.env.AIRFLOW_REQUEST_TIMEOUT_MS ||
        60000,
    ),
  };
}

function ensureUniqueServiceIds(services) {
  const seen = new Set();
  return services.map((service, index) => {
    const baseId =
      service.id ||
      slugify(`${service.name}-${service.username}`, `airflow-${index + 1}`);
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
  const baseUrl = normalizeBaseUrl(process.env.AIRFLOW_BASE_URL);
  return normalizeAirflowService({
    id: process.env.AIRFLOW_SERVICE_ID || "main",
    name: process.env.AIRFLOW_SERVICE_NAME || "Airflow",
    alias: process.env.AIRFLOW_SERVICE_ALIAS || "Airflow",
    baseUrl,
    username: process.env.AIRFLOW_USERNAME || "",
    password: process.env.AIRFLOW_PASSWORD || "",
    owner: process.env.AIRFLOW_OWNER || "Shaun",
    requestTimeoutMs: process.env.AIRFLOW_REQUEST_TIMEOUT_MS || 60000,
  });
}

function parseAirflowServices() {
  const rawList =
    process.env.AIRFLOW_LIST || process.env.AIRFLOW_SERVICES || "";
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
    console.warn(`AIRFLOW_LIST parse failed, using single service: ${error.message}`);
    return [legacyAirflowService()];
  }
}

function normalizeDagConfList(value) {
  if (Array.isArray(value)) {
    return value;
  }
  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }
  return [];
}

function normalizeDagConfFieldMappingItem(item) {
  const dagName = String(item?.dag_name || item?.dagName || "").trim();
  const keys = normalizeDagConfList(item?.keys);
  const aliases = normalizeDagConfList(item?.alias || item?.aliases);
  const tips = normalizeDagConfList(
    item?.tips ||
      item?.tip ||
      item?.help ||
      item?.helps ||
      item?.description ||
      item?.descriptions,
  );
  if (!dagName || !keys.length) {
    return null;
  }
  const normalized = {
    dag_name: dagName,
    keys,
    alias: keys.map((key, index) => aliases[index] || key),
  };
  if (tips.length) {
    normalized.tips = keys.map((key, index) => tips[index] || "");
  }
  return normalized;
}

function parseDagConfMappingFromEnv(env) {
  const rawMapping = env.DAG_CONF_FIELD_MAPPING || env.DAG_CONF_MAPPING || "";
  if (!rawMapping.trim()) {
    return [];
  }

  try {
    const parsed = JSON.parse(rawMapping);
    const list = Array.isArray(parsed) ? parsed : [parsed];
    return list.map(normalizeDagConfFieldMappingItem).filter(Boolean);
  } catch (error) {
    console.warn(`DAG_CONF_FIELD_MAPPING parse failed: ${error.message}`);
    return [];
  }
}

function mergeDagConfMappings(...mappingGroups) {
  const merged = new Map();
  for (const group of mappingGroups) {
    for (const mapping of group) {
      const previous = merged.get(mapping.dag_name) || {};
      const next = {
        ...previous,
        ...mapping,
      };
      if (!mapping.tips && previous.tips) {
        next.tips = previous.tips;
      }
      merged.set(mapping.dag_name, next);
    }
  }
  return [...merged.values()];
}

function remoteEnvRequestUrl(url) {
  const nextUrl = new URL(url);
  nextUrl.searchParams.set("_airflow_webapp_ts", String(Date.now()));
  return nextUrl.toString();
}

async function parseEnvUrl(url) {
  const remoteUrl = String(url || "").trim();
  if (!/^https?:\/\//i.test(remoteUrl)) {
    return {};
  }

  try {
    const response = await fetch(remoteEnvRequestUrl(remoteUrl), {
      headers: {
        Accept: "text/plain",
        "Cache-Control": "no-cache",
        Pragma: "no-cache",
        "User-Agent": "airflow-gui-tool-webapp",
      },
    });
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return parseEnvContent(await response.text());
  } catch (error) {
    console.warn(`Remote DAG conf env failed, using local fallback: ${error.message}`);
    return {};
  }
}

async function loadDagConfFieldMapping() {
  const dagConfEnv = parseEnvFile(".dag-conf.env");
  const remoteDagConfEnv = await parseEnvUrl(
    process.env.DAG_CONF_ENV_URL ||
      process.env.DAG_CONF_MAPPING_URL ||
      dagConfEnv.DAG_CONF_ENV_URL ||
      dagConfEnv.DAG_CONF_MAPPING_URL ||
      "",
  );
  return mergeDagConfMappings(
    parseDagConfMappingFromEnv(dagConfEnv),
    parseDagConfMappingFromEnv(remoteDagConfEnv),
    parseDagConfMappingFromEnv(process.env),
  );
}

loadDotEnv();

const airflowServices = parseAirflowServices();
const defaultAirflowServiceId = String(
  process.env.AIRFLOW_ACTIVE_SERVICE_ID ||
    process.env.AIRFLOW_DEFAULT_SERVICE_ID ||
    airflowServices[0].id,
);
const activeAirflowService =
  airflowServices.find((service) => service.id === defaultAirflowServiceId) ||
  airflowServices[0];
const dataDir = path.resolve(
  process.env.AIRFLOW_WEBAPP_DATA_DIR || path.join(__dirname, "..", "data"),
);
const adminEmployeeCodes = splitCsv(process.env.SEATALK_ADMIN_EMPLOYEE_CODES);
const nodeEnv = process.env.NODE_ENV || "development";

module.exports = {
  app: {
    name: "Lovito Task Hub WebApp",
    title: "Lovito 任务中心",
  },
  server: {
    host: process.env.HOST || "127.0.0.1",
    port: parsePositiveNumber(process.env.PORT, 5178),
  },
  auth: {
    sessionSecret:
      process.env.SESSION_SECRET ||
      (nodeEnv === "production" ? "" : "dev-airflow-webapp-session-secret"),
    allowDevAuth: parseBoolean(
      process.env.ALLOW_DEV_AUTH,
      nodeEnv !== "production" && !process.env.SEATALK_APP_SECRET,
    ),
    devUser: {
      employeeCode: process.env.DEV_USER_EMPLOYEE_CODE || os.userInfo().username,
      email: process.env.DEV_USER_EMAIL || "",
      name: process.env.DEV_USER_NAME || os.userInfo().username,
    },
  },
  seatalk: {
    appId: process.env.SEATALK_APP_ID || "",
    appSecret: process.env.SEATALK_APP_SECRET || "",
    apiBaseUrl: normalizeBaseUrl(
      process.env.SEATALK_API_BASE_URL || "https://openapi.seatalk.io",
    ),
    adminEmployeeCodes,
  },
  airflow: activeAirflowService,
  airflowServices,
  defaultAirflowServiceId: activeAirflowService.id,
  dataDir,
  cookieDir: path.join(dataDir, "airflow-cookies"),
  loadDagConfFieldMapping,
  parseEnvContent,
  parseAirflowServices,
  normalizeAirflowService,
  parseDagConfMappingFromEnv,
};
