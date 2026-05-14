const { app, BrowserWindow, ipcMain, Notification } = require("electron");
const crypto = require("crypto");
const fs = require("fs");
const os = require("os");
const path = require("path");
const config = require("./src/config");
const { AirflowClient } = require("./src/airflowClient");
const stateStore = require("./src/stateStore");

let airflow;
let activeAirflowService;
let dagConfFieldMapping = config.dagConfFieldMapping;
let mainWindow;

const DAG_TOGGLE_ALLOWED_USERNAME = "shaun.pan";
const systemUsername = os.userInfo().username || process.env.USER || "";

function safeFilePart(value) {
  return String(value || "airflow")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48) || "airflow";
}

function serviceHash(service) {
  return crypto
    .createHash("sha1")
    .update(`${service.baseUrl}|${service.username}`)
    .digest("hex")
    .slice(0, 12);
}

function cookieStorePathForService(service) {
  const fileName = `${safeFilePart(service.id)}-${serviceHash(service)}.json`;
  return path.join(app.getPath("userData"), "airflow-cookies", fileName);
}

function migrateLegacyCookieStore(service, targetPath) {
  const legacyPath = path.join(app.getPath("userData"), "airflow-cookies.json");
  if (fs.existsSync(targetPath) || !fs.existsSync(legacyPath)) {
    return;
  }

  try {
    const stored = JSON.parse(fs.readFileSync(legacyPath, "utf8"));
    if (stored.baseUrl !== service.baseUrl || stored.username !== service.username) {
      return;
    }
    fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    fs.copyFileSync(legacyPath, targetPath);
  } catch {
    // Ignore migration failures; the client can log in and create a fresh cookie.
  }
}

function findAirflowService(serviceId) {
  return (
    config.airflowServices.find((service) => service.id === serviceId) ||
    config.airflowServices.find((service) => service.id === config.defaultAirflowServiceId) ||
    config.airflowServices[0]
  );
}

function publicAirflowServices() {
  return config.airflowServices.map((service) => ({
    id: service.id,
    name: service.name,
    alias: service.alias,
    label: service.label,
    baseUrl: service.baseUrl,
    username: service.username,
    owner: service.owner,
  }));
}

function activePublicConfig() {
  return {
    ...airflow.publicConfig,
    services: publicAirflowServices(),
    dagConfFieldMapping,
    systemUsername,
    canToggleDag: systemUsername === DAG_TOGGLE_ALLOWED_USERNAME,
    defaultServiceId: config.defaultAirflowServiceId,
  };
}

function reloadDagConfFieldMapping() {
  dagConfFieldMapping = config.loadDagConfFieldMapping();
  return dagConfFieldMapping;
}

function formatRunIdTime(date = new Date()) {
  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    pad(date.getMonth() + 1),
    pad(date.getDate()),
    "_",
    pad(date.getHours()),
    pad(date.getMinutes()),
    pad(date.getSeconds()),
  ].join("");
}

function buildDefaultDagRunId() {
  const user = String(systemUsername || "unknown")
    .trim()
    .replace(/[^A-Za-z0-9_.-]+/g, "_") || "unknown";
  return `${user}_${formatRunIdTime()}`;
}

function assertCanToggleDag() {
  if (systemUsername !== DAG_TOGGLE_ALLOWED_USERNAME) {
    throw new Error(`当前系统用户 ${systemUsername || "unknown"} 无权操作 DAG 开关`);
  }
}

function activateAirflowService(serviceId) {
  const service = findAirflowService(serviceId);
  const cookieStorePath = cookieStorePathForService(service);
  migrateLegacyCookieStore(service, cookieStorePath);
  activeAirflowService = service;
  airflow = new AirflowClient({
    ...service,
    serviceId: service.id,
    serviceName: service.name,
    cookieStorePath,
  });
  return airflow;
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1600,
    height: 980,
    minWidth: 1280,
    minHeight: 760,
    title: config.app.title,
    icon: path.join(__dirname, "assets", "icon.png"),
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  mainWindow.loadFile(path.join(__dirname, "renderer", "index.html"));
}

function registerIpcHandlers() {
  ipcMain.handle("app:get-init", async () => ({
    config: activePublicConfig(),
    state: stateStore.readState(),
    statePath: stateStore.statePath(),
  }));

  ipcMain.handle("airflow:switch-service", async (_event, serviceId) => {
    activateAirflowService(serviceId);
    stateStore.savePatch({ airflowServiceId: activeAirflowService.id });
    return { config: activePublicConfig() };
  });
  ipcMain.handle("app:reload-dag-conf-mapping", async () => ({
    dagConfFieldMapping: reloadDagConfFieldMapping(),
  }));
  ipcMain.handle("airflow:relogin", async () => airflow.relogin());
  ipcMain.handle("airflow:list-dags", async (_event, owner) => airflow.listDags(owner));
  ipcMain.handle("airflow:set-dag-paused", async (_event, dagId, isPaused) => {
    assertCanToggleDag();
    return airflow.setDagPaused(dagId, isPaused);
  });
  ipcMain.handle("airflow:list-tasks", async (_event, dagId) => airflow.listTasks(dagId));
  ipcMain.handle("airflow:list-dag-runs", async (_event, dagId, limit) =>
    airflow.listDagRuns(dagId, limit),
  );
  ipcMain.handle("airflow:get-dag-run", async (_event, dagId, dagRunId) =>
    airflow.getDagRun(dagId, dagRunId),
  );
  ipcMain.handle("airflow:list-task-instances", async (_event, dagId, dagRunId) =>
    airflow.listTaskInstances(dagId, dagRunId),
  );
  ipcMain.handle("airflow:list-task-tries", async (_event, dagId, dagRunId, taskId, mapIndex) =>
    airflow.listTaskTries(dagId, dagRunId, taskId, mapIndex),
  );
  ipcMain.handle("airflow:trigger-dag", async (_event, dagId, conf, runId) =>
    airflow.triggerDag(dagId, conf, String(runId || "").trim() || buildDefaultDagRunId()),
  );
  ipcMain.handle("airflow:terminate-task", async (_event, params) => airflow.terminateTask(params));
  ipcMain.handle("airflow:get-task-log", async (_event, params) => airflow.getTaskLog(params));
  ipcMain.handle("state:save", async (_event, patch) => stateStore.savePatch(patch));
  ipcMain.handle("state:clear", async () => stateStore.clearState());
  ipcMain.handle("app:notify", async (_event, message) => {
    if (Notification.isSupported()) {
      new Notification({ title: config.app.title, body: message }).show();
      return true;
    }
    return false;
  });
}

app.whenReady().then(() => {
  if (process.platform === "darwin") {
    app.setAppUserModelId("com.shaun.airflowguitool");
  }
  const initialState = stateStore.readState();
  activateAirflowService(initialState.airflowServiceId || config.defaultAirflowServiceId);
  registerIpcHandlers();
  createWindow();

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});
