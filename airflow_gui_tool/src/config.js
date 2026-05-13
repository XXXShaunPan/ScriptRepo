const fs = require("fs");
const os = require("os");
const path = require("path");
const { execFileSync } = require("child_process");

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

  for (const rawLine of content.split(/\r?\n/)) {
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

function parseEnvFile(fileName) {
  const envPath = path.join(__dirname, "..", fileName);
  if (!fs.existsSync(envPath)) {
    return {};
  }
  return parseEnvContent(fs.readFileSync(envPath, "utf8"));
}

function isHttpUrl(value) {
  return /^https?:\/\//i.test(String(value || "").trim());
}

function remoteEnvRequestUrl(url) {
  const nextUrl = new URL(url);
  nextUrl.searchParams.set("_airflow_gui_ts", String(Date.now()));
  return nextUrl.toString();
}

function parseEnvUrl(url) {
  const remoteUrl = String(url || "").trim();
  if (!isHttpUrl(remoteUrl)) {
    return {};
  }

  try {
    const requestUrl = remoteEnvRequestUrl(remoteUrl);
    const content = execFileSync(
      "curl",
      [
        "-fsSL",
        "--max-time",
        "8",
        "-H",
        "Cache-Control: no-cache",
        "-H",
        "Pragma: no-cache",
        "-H",
        "Accept: text/plain",
        "-A",
        "airflow-gui-tool",
        requestUrl,
      ],
      {
        encoding: "utf8",
        maxBuffer: 1024 * 1024,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    return parseEnvContent(content);
  } catch (error) {
    console.warn(`远程 env 读取失败，已回退本地配置: ${remoteUrl} (${error.message})`);
    return {};
  }
}

function loadDotEnv() {
  const env = parseEnvFile(".env");
  for (const [key, value] of Object.entries(env)) {
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
    item?.tips || item?.tip || item?.help || item?.helps || item?.description || item?.descriptions,
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
    console.warn(`DAG_CONF_FIELD_MAPPING 解析失败，已忽略: ${error.message}`);
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

function parseDagConfFieldMapping() {
  const dagConfEnv = parseEnvFile(".dag-conf.env");
  const remoteDagConfEnv = parseEnvUrl(
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
const dagConfFieldMapping = parseDagConfFieldMapping();
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
  dagConfFieldMapping,
  defaultAirflowServiceId: activeAirflowService.id,
};
