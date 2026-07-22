(function () {
  const sdk = window.st || null;
  let authPromise = null;
  let authStatus = null;

  function encodePart(value) {
    return encodeURIComponent(String(value || ""));
  }

  function queryString(params = {}) {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") {
        search.set(key, String(value));
      }
    });
    const text = search.toString();
    return text ? `?${text}` : "";
  }

  async function requestJson(path, options = {}) {
    const response = await fetch(path, {
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        ...(options.body ? { "Content-Type": "application/json" } : {}),
        ...(options.headers || {}),
      },
      ...options,
      body:
        options.body && typeof options.body !== "string"
          ? JSON.stringify(options.body)
          : options.body,
    });
    const text = await response.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = { message: text };
    }
    if (!response.ok) {
      const error = new Error(data.message || `${response.status} ${response.statusText}`);
      error.status = response.status;
      error.payload = data;
      throw error;
    }
    return data;
  }

  async function getAuthStatus({ force = false } = {}) {
    if (!authStatus || force) {
      authStatus = await requestJson("/api/auth/status");
    }
    return authStatus;
  }

  function isSeaTalkRuntime() {
    const appName = sdk?.clientInfo?.app;
    return String(appName || "").toLowerCase() === "seatalk";
  }

  async function getSeaTalkToken() {
    if (!sdk?.getSSOToken) {
      throw new Error("当前页面没有加载 SeaTalk Web SDK");
    }
    return sdk.getSSOToken();
  }

  async function loginWithSeaTalk() {
    const status = await getAuthStatus({ force: true });
    if (status.authenticated) {
      return status;
    }

    if (isSeaTalkRuntime()) {
      const ssoToken = await getSeaTalkToken();
      const result = await requestJson("/api/auth/seatalk", {
        method: "POST",
        body: { ssoToken },
      });
      authStatus = {
        authenticated: true,
        user: result.user,
        seatalkConfigured: true,
        devAuthAvailable: status.devAuthAvailable,
      };
      return authStatus;
    }

    if (status.devAuthAvailable) {
      const result = await requestJson("/api/auth/dev", {
        method: "POST",
        body: {},
      });
      authStatus = {
        authenticated: true,
        user: result.user,
        seatalkConfigured: status.seatalkConfigured,
        devAuthAvailable: true,
      };
      return authStatus;
    }

    throw new Error("请在 SeaTalk Workspace 内打开本 WebApp 完成登录。");
  }

  async function ensureAuthenticated() {
    if (!authPromise) {
      authPromise = loginWithSeaTalk().catch((error) => {
        authPromise = null;
        throw error;
      });
    }
    return authPromise;
  }

  async function authenticatedRequest(path, options) {
    await ensureAuthenticated();
    return requestJson(path, options);
  }

  async function notify(message) {
    if (sdk?.toast && isSeaTalkRuntime()) {
      try {
        await sdk.toast({ message: String(message || "") });
      } catch {
        // The in-page toast still handles feedback if SeaTalk toast fails.
      }
    }
    return authenticatedRequest("/api/notify", {
      method: "POST",
      body: { message },
    }).catch(() => ({ ok: false }));
  }

  function downloadTextFile(fileName, content) {
    const blob = new Blob([`${String(content || "").trimEnd()}\n`], {
      type: "text/x-python;charset=utf-8",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = fileName || "new_dag.py";
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }

  function normalizeDagFileName(fileName, dagId) {
    const fallbackName = dagId ? `${dagId}_dag` : "new_dag";
    const baseName =
      String(fileName || fallbackName)
        .trim()
        .replace(/\.py$/i, "")
        .replace(/[^A-Za-z0-9_.-]+/g, "_")
        .replace(/^_+|_+$/g, "") || "new_dag";
    return `${baseName}.py`;
  }

  window.airflowApp = {
    loginWithSeaTalk,
    getInit: async () => authenticatedRequest("/api/init"),
    reloadDagConfMapping: async () =>
      authenticatedRequest("/api/reload-dag-conf-mapping", { method: "POST" }),
    checkUpdate: async () => authenticatedRequest("/api/update/check"),
    downloadUpdate: async () => {
      throw new Error("WebApp 版本由部署流程更新，无需下载桌面更新包。");
    },
    installUpdate: async () => {
      throw new Error("WebApp 版本由部署流程更新，无需安装桌面更新包。");
    },
    onUpdateEvent: () => () => {},
    switchAirflowService: async (serviceId) =>
      authenticatedRequest("/api/airflow/switch-service", {
        method: "POST",
        body: { serviceId },
      }),
    relogin: async () =>
      authenticatedRequest("/api/airflow/relogin", { method: "POST" }),
    saveDagFile: async (payload = {}) => {
      const fileName = normalizeDagFileName(payload.fileName, payload.dagId);
      downloadTextFile(fileName, payload.code || "");
      return { downloaded: true, filePath: fileName };
    },
    getDagCode: async (dagId) =>
      authenticatedRequest(`/api/airflow/dags/${encodePart(dagId)}/code`),
    listDags: async (owner) =>
      authenticatedRequest(`/api/airflow/dags${queryString({ owner })}`),
    setDagPaused: async (dagId, isPaused) =>
      authenticatedRequest(`/api/airflow/dags/${encodePart(dagId)}/paused`, {
        method: "PATCH",
        body: { isPaused },
      }),
    listTasks: async (dagId) =>
      authenticatedRequest(`/api/airflow/dags/${encodePart(dagId)}/tasks`),
    listDagRuns: async (dagId, limit) =>
      authenticatedRequest(
        `/api/airflow/dags/${encodePart(dagId)}/dag-runs${queryString({ limit })}`,
      ),
    getDagRun: async (dagId, dagRunId) =>
      authenticatedRequest(
        `/api/airflow/dags/${encodePart(dagId)}/dag-runs/${encodePart(dagRunId)}`,
      ),
    listTaskInstances: async (dagId, dagRunId) =>
      authenticatedRequest(
        `/api/airflow/dags/${encodePart(dagId)}/dag-runs/${encodePart(
          dagRunId,
        )}/task-instances`,
      ),
    listTaskTries: async (dagId, dagRunId, taskId, mapIndex) =>
      authenticatedRequest(
        `/api/airflow/dags/${encodePart(dagId)}/dag-runs/${encodePart(
          dagRunId,
        )}/task-instances/${encodePart(taskId)}/tries${queryString({ mapIndex })}`,
      ),
    triggerDag: async (dagId, conf, runId) =>
      authenticatedRequest(`/api/airflow/dags/${encodePart(dagId)}/dag-runs`, {
        method: "POST",
        body: { conf, runId },
      }),
    terminateTask: async (params) =>
      authenticatedRequest("/api/airflow/tasks/terminate", {
        method: "POST",
        body: params,
      }),
    getTaskLog: async (params) =>
      authenticatedRequest(`/api/airflow/logs${queryString(params)}`),
    saveState: async (patch) =>
      authenticatedRequest("/api/state", { method: "POST", body: patch }),
    clearState: async () =>
      authenticatedRequest("/api/state", { method: "DELETE" }),
    notify,
  };
})();
