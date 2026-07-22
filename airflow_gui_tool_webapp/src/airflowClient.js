const fs = require("fs");
const path = require("path");
const config = require("./config");

const TERMINAL_STATES = new Set([
  "success",
  "failed",
  "skipped",
  "upstream_failed",
  "removed",
]);
const ACTIVE_DAG_RUN_STATES = new Set(["running", "queued"]);
const MAX_ACTIVE_DAG_RUNS_PER_DAG = 3;
const DAG_TRIGGER_LIMIT_ERROR_PREFIX = "DAG_TRIGGER_LIMIT:";
const DEFAULT_COOKIE_TTL_MS = 3 * 24 * 60 * 60 * 1000;

class AirflowClient {
  constructor(airflowConfig = config.airflow) {
    this.serviceId = airflowConfig.serviceId || airflowConfig.id || "";
    this.serviceName = airflowConfig.serviceName || airflowConfig.name || "";
    this.serviceLabel = airflowConfig.label || "";
    this.baseUrl = airflowConfig.baseUrl;
    this.username = airflowConfig.username;
    this.password = airflowConfig.password;
    this.owner = airflowConfig.owner;
    this.requestTimeoutMs = airflowConfig.requestTimeoutMs;
    this.cookieStorePath = airflowConfig.cookieStorePath || "";
    this.cookieTtlMs = Number(
      airflowConfig.cookieTtlMs || DEFAULT_COOKIE_TTL_MS,
    );
    this.cookieMap = new Map();
    this.cookieExpiresAt = 0;
    this.isLoggingIn = false;
  }

  get publicConfig() {
    return {
      serviceId: this.serviceId,
      serviceName: this.serviceName,
      serviceLabel: this.serviceLabel,
      baseUrl: this.baseUrl,
      username: this.username,
      owner: this.owner,
    };
  }

  cookieHeader() {
    return [...this.cookieMap.entries()]
      .map(([key, value]) => `${key}=${value}`)
      .join("; ");
  }

  isCookieUsable() {
    return this.cookieMap.size > 0 && Date.now() < this.cookieExpiresAt;
  }

  loadCookiesFromStore() {
    if (!this.cookieStorePath) {
      return false;
    }

    try {
      if (!fs.existsSync(this.cookieStorePath)) {
        return false;
      }
      const stored = JSON.parse(fs.readFileSync(this.cookieStorePath, "utf8"));
      const expiresAt = Number(stored.expiresAt || 0);
      if (
        stored.baseUrl !== this.baseUrl ||
        stored.username !== this.username ||
        !stored.cookies ||
        expiresAt <= Date.now()
      ) {
        this.clearCookieStore();
        return false;
      }

      this.cookieMap = new Map(Object.entries(stored.cookies));
      this.cookieExpiresAt = expiresAt;
      return this.cookieMap.size > 0;
    } catch {
      this.clearCookieStore();
      return false;
    }
  }

  saveCookiesToStore() {
    if (
      !this.cookieStorePath ||
      !this.cookieMap.size ||
      !this.cookieExpiresAt
    ) {
      return;
    }

    const payload = {
      baseUrl: this.baseUrl,
      username: this.username,
      expiresAt: this.cookieExpiresAt,
      savedAt: new Date().toISOString(),
      cookies: Object.fromEntries(this.cookieMap.entries()),
    };
    fs.mkdirSync(path.dirname(this.cookieStorePath), { recursive: true });
    fs.writeFileSync(
      this.cookieStorePath,
      JSON.stringify(payload, null, 2),
      "utf8",
    );
  }

  clearCookieStore() {
    this.cookieMap.clear();
    this.cookieExpiresAt = 0;
    if (!this.cookieStorePath) {
      return;
    }
    try {
      if (fs.existsSync(this.cookieStorePath)) {
        fs.unlinkSync(this.cookieStorePath);
      }
    } catch {
      // Ignore cookie cleanup failures. A later login can overwrite the file.
    }
  }

  updateCookies(headers) {
    const fromGetSetCookie =
      typeof headers.getSetCookie === "function" ? headers.getSetCookie() : [];
    const fallback = headers.get("set-cookie");
    const rawCookies = fromGetSetCookie.length
      ? fromGetSetCookie
      : fallback
        ? fallback.split(/,(?=\s*[^;,]+=)/g)
        : [];

    for (const rawCookie of rawCookies) {
      const firstPart = rawCookie.split(";")[0];
      const separatorIndex = firstPart.indexOf("=");
      if (separatorIndex === -1) {
        continue;
      }
      const name = firstPart.slice(0, separatorIndex).trim();
      const value = firstPart.slice(separatorIndex + 1).trim();
      if (name) {
        this.cookieMap.set(name, value);
      }
    }
  }

  async fetchWithTimeout(url, options = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
    try {
      return await fetch(url, {
        ...options,
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timeout);
    }
  }

  async login(force = false) {
    if (!force && (this.isCookieUsable() || this.loadCookiesFromStore())) {
      return;
    }
    if (this.isLoggingIn) {
      await new Promise((resolve) => setTimeout(resolve, 250));
      return this.login(force);
    }

    this.isLoggingIn = true;
    try {
      if (force) {
        this.clearCookieStore();
      } else if (this.cookieMap.size) {
        this.cookieMap.clear();
        this.cookieExpiresAt = 0;
      }

      const nextUrl = `${this.baseUrl}/home`;
      const loginGetUrl = `${this.baseUrl}/login/?next=${nextUrl}`;
      const loginPostUrl = `${this.baseUrl}/login/?next=${encodeURIComponent(nextUrl)}`;
      const loginPageResponse = await this.fetchWithTimeout(loginGetUrl, {
        headers: this.browserHeaders({ accept: "text/html" }),
      });
      this.updateCookies(loginPageResponse.headers);
      const loginPage = await loginPageResponse.text();
      const csrfToken = this.extractCsrfToken(loginPage);
      if (!csrfToken) {
        throw new Error("无法从 Airflow 登录页提取 CSRF Token");
      }

      const formData = new URLSearchParams({
        csrf_token: csrfToken,
        username: this.username,
        password: this.password,
        next: nextUrl,
      });
      const postResponse = await this.fetchWithTimeout(loginPostUrl, {
        method: "POST",
        headers: {
          ...this.browserHeaders({ accept: "text/html" }),
          "Content-Type": "application/x-www-form-urlencoded",
          Cookie: this.cookieHeader(),
          Origin: this.baseUrl,
          Referer: loginGetUrl,
        },
        body: formData,
        redirect: "manual",
      });
      this.updateCookies(postResponse.headers);

      const isRedirect =
        postResponse.status >= 300 && postResponse.status < 400;
      if (!postResponse.ok && !isRedirect) {
        const body = await postResponse.text();
        throw new Error(
          `Airflow 登录失败 (${postResponse.status}): ${body.slice(0, 240)}`,
        );
      }

      let body = "";
      if (isRedirect) {
        const location = postResponse.headers.get("location") || "/home";
        const redirectUrl = new URL(location, this.baseUrl).toString();
        const homeResponse = await this.fetchWithTimeout(redirectUrl, {
          headers: {
            ...this.browserHeaders({ accept: "text/html" }),
            Cookie: this.cookieHeader(),
            Referer: loginGetUrl,
          },
          redirect: "manual",
        });
        this.updateCookies(homeResponse.headers);
        body = await homeResponse.text();
      } else {
        body = await postResponse.text();
      }

      if (this.isLoginPage(body)) {
        throw new Error(
          `Airflow 登录后仍返回登录页，请检查账号、密码或 AIRFLOW_BASE_URL。当前: ${this.baseUrl} / ${this.username}`,
        );
      }

      this.cookieExpiresAt = Date.now() + this.cookieTtlMs;
      this.saveCookiesToStore();
    } finally {
      this.isLoggingIn = false;
    }
  }

  extractCsrfToken(html) {
    const patterns = [
      /name=["']csrf_token["'][^>]*value=["']([^"']+)["']/i,
      /id=["']csrf_token["'][^>]*value=["']([^"']+)["']/i,
      /value=["']([^"']+)["'][^>]*name=["']csrf_token["']/i,
      /value=["']([^"']+)["'][^>]*id=["']csrf_token["']/i,
    ];
    for (const pattern of patterns) {
      const match = html.match(pattern);
      if (match) {
        return match[1];
      }
    }
    return "";
  }

  isLoginPage(html) {
    return (
      /name=["']username["']/i.test(html) && /name=["']password["']/i.test(html)
    );
  }

  browserHeaders({ accept = "application/json" } = {}) {
    return {
      Accept: accept,
      "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    };
  }

  defaultHeaders({ accept = "application/json" } = {}) {
    return this.browserHeaders({ accept });
  }

  async request(path, options = {}, retry = true) {
    await this.login();
    const url = path.startsWith("http") ? path : `${this.baseUrl}${path}`;
    const headers = {
      ...this.defaultHeaders(),
      Cookie: this.cookieHeader(),
      ...(options.headers || {}),
    };

    const response = await this.fetchWithTimeout(url, {
      ...options,
      headers,
    });
    this.updateCookies(response.headers);
    if (response.ok && this.isCookieUsable()) {
      this.saveCookiesToStore();
    }

    if ((response.status === 401 || response.status === 403) && retry) {
      await this.login(true);
      return this.request(path, options, false);
    }

    return response;
  }

  async requestJson(path, options = {}) {
    const response = await this.request(path, options);
    const bodyText = await response.text();
    if (!response.ok) {
      throw new Error(
        `${response.status} ${response.statusText}: ${bodyText.slice(0, 500)}`,
      );
    }
    if (!bodyText) {
      return {};
    }
    try {
      return JSON.parse(bodyText);
    } catch {
      throw new Error(`Airflow 返回了非 JSON 内容: ${bodyText.slice(0, 500)}`);
    }
  }

  async apiGet(path, params = {}) {
    const url = new URL(`${this.baseUrl}${path}`);
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== "") {
        url.searchParams.set(key, String(value));
      }
    }
    return this.requestJson(`${url.pathname}${url.search}`);
  }

  async apiGetPaginated(path, params = {}, collectionKey, pageSize = 500) {
    const items = [];
    let offset = Number(params.offset || 0);
    let totalEntries = null;

    while (true) {
      const data = await this.apiGet(path, {
        ...params,
        limit: pageSize,
        offset,
      });
      const pageItems = Array.isArray(data[collectionKey])
        ? data[collectionKey]
        : [];
      items.push(...pageItems);

      if (Number.isFinite(Number(data.total_entries))) {
        totalEntries = Number(data.total_entries);
      }

      offset += pageItems.length;
      if (!pageItems.length) {
        break;
      }
      if (totalEntries !== null && offset >= totalEntries) {
        break;
      }
      if (totalEntries === null && pageItems.length < pageSize) {
        break;
      }
    }

    return {
      [collectionKey]: items,
      total_entries: totalEntries ?? items.length,
    };
  }

  async apiPost(path, body) {
    return this.requestJson(path, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
  }

  async apiPatch(path, body) {
    return this.requestJson(path, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
  }

  normalizeOwners(dag) {
    const owners = dag.owners || dag.owner || [];
    if (Array.isArray(owners)) {
      return owners.map(String);
    }
    return String(owners)
      .split(",")
      .map((owner) => owner.trim())
      .filter(Boolean);
  }

  async listDags(owner = this.owner) {
    const data = await this.apiGetPaginated(
      "/api/v1/dags",
      {
        only_active: "true",
      },
      "dags",
    );
    const ownerNeedle = String(owner || "").toLowerCase();
    const dags = (data.dags || [])
      .map((dag) => this.normalizeDag(dag))
      .filter(
        (dag) =>
          !ownerNeedle ||
          dag.owners.some((item) => {
            const normalized = item.toLowerCase();
            return (
              normalized === ownerNeedle || normalized.includes(ownerNeedle)
            );
          }),
      )
      .sort((a, b) => a.dag_id.localeCompare(b.dag_id));

    return {
      dags,
      total: dags.length,
      totalAirflow: Number(data.total_entries || data.dags?.length || 0),
      owner,
    };
  }

  normalizeDag(dag) {
    return {
      dag_id: dag.dag_id,
      description: dag.description || "",
      fileloc: dag.fileloc || "",
      is_paused: Boolean(dag.is_paused),
      is_active: dag.is_active !== false,
      owners: this.normalizeOwners(dag),
      tags: Array.isArray(dag.tags)
        ? dag.tags.map((tag) => tag.name || tag)
        : [],
    };
  }

  async getDagCode(dagId) {
    const normalizedDagId = String(dagId || "").trim();
    if (!normalizedDagId) {
      throw new Error("DAG ID 不能为空");
    }

    const details = await this.getDagDetailsForSource(normalizedDagId);
    const inlineCode =
      details.source_code ||
      details.dag_code ||
      details.code ||
      details.source ||
      "";
    if (typeof inlineCode === "string" && inlineCode.trim()) {
      return {
        dagId: normalizedDagId,
        fileloc: details.fileloc || "",
        code: inlineCode,
      };
    }

    const fileToken =
      details.file_token ||
      details.fileToken ||
      details.dag?.file_token ||
      details.dag?.fileToken ||
      "";
    if (!fileToken) {
      throw new Error("Airflow 未返回 DAG file_token，无法读取源码");
    }

    const response = await this.request(
      `/api/v1/dagSources/${encodeURIComponent(fileToken)}`,
      {
        headers: {
          Accept: "text/plain,application/json,*/*",
        },
      },
    );
    const bodyText = await response.text();
    if (!response.ok) {
      throw new Error(
        `${response.status} ${response.statusText}: ${bodyText.slice(0, 500)}`,
      );
    }

    return {
      dagId: normalizedDagId,
      fileloc: details.fileloc || details.dag?.fileloc || "",
      code: this.normalizeDagSourceResponse(bodyText),
    };
  }

  async getDagDetailsForSource(dagId) {
    const encodedDagId = encodeURIComponent(dagId);
    const paths = [
      `/api/v1/dags/${encodedDagId}/details`,
      `/api/v1/dags/${encodedDagId}`,
    ];
    let lastError = null;

    for (const path of paths) {
      try {
        return await this.apiGet(path);
      } catch (error) {
        lastError = error;
      }
    }

    throw lastError || new Error("DAG details 加载失败");
  }

  normalizeDagSourceResponse(bodyText) {
    const sourceText = String(bodyText || "");
    const trimmed = sourceText.trimStart();
    if (!trimmed.startsWith("{")) {
      return sourceText;
    }

    try {
      const data = JSON.parse(sourceText);
      const wrappedCode =
        data.content ||
        data.source_code ||
        data.dag_source ||
        data.code ||
        data.source ||
        data.message ||
        "";
      if (typeof wrappedCode === "string" && wrappedCode) {
        return wrappedCode;
      }
    } catch {
      // Airflow usually returns plain text; keep the original body if parsing fails.
    }

    return sourceText;
  }

  async setDagPaused(dagId, isPaused) {
    const data = await this.apiPatch(
      `/api/v1/dags/${encodeURIComponent(dagId)}?update_mask=is_paused`,
      {
        is_paused: Boolean(isPaused),
      },
    );
    return this.normalizeDag(data);
  }

  async listTasks(dagId) {
    const data = await this.apiGet(
      `/api/v1/dags/${encodeURIComponent(dagId)}/tasks`,
    );
    const tasks = (data.tasks || [])
      .map((task) => ({
        task_id: task.task_id,
        owner: task.owner || "",
        trigger_rule: task.trigger_rule || "",
        operator_name: task.operator_name || task.class_ref?.class_name || "",
        downstream_task_ids: task.downstream_task_ids || [],
        upstream_task_ids: task.upstream_task_ids || [],
      }))
      .sort((a, b) => a.task_id.localeCompare(b.task_id));
    return { tasks };
  }

  async listDagRuns(dagId, limit = 25) {
    let data;
    try {
      data = await this.apiGet(
        `/api/v1/dags/${encodeURIComponent(dagId)}/dagRuns`,
        {
          limit,
          order_by: "-execution_date",
        },
      );
    } catch {
      data = await this.apiGet(
        `/api/v1/dags/${encodeURIComponent(dagId)}/dagRuns`,
        { limit },
      );
    }

    const dagRuns = (data.dag_runs || [])
      .map((run) => this.normalizeDagRun(run))
      .sort((a, b) =>
        String(b.execution_date || "").localeCompare(
          String(a.execution_date || ""),
        ),
      );
    return { dag_runs: dagRuns };
  }

  async listActiveDagRuns(dagId, limit = MAX_ACTIVE_DAG_RUNS_PER_DAG + 1) {
    const path = `/api/v1/dags/${encodeURIComponent(dagId)}/dagRuns`;
    const activeRunsById = new Map();

    try {
      for (const state of ACTIVE_DAG_RUN_STATES) {
        let data;
        try {
          data = await this.apiGet(path, {
            limit,
            state,
            order_by: "-execution_date",
          });
        } catch {
          data = await this.apiGet(path, { limit, state });
        }

        const normalizedRuns = (data.dag_runs || []).map((run) =>
          this.normalizeDagRun(run),
        );
        if (normalizedRuns.some((run) => run.state !== state)) {
          throw new Error("Airflow dagRuns state filter was not applied");
        }

        for (const normalizedRun of normalizedRuns) {
          if (ACTIVE_DAG_RUN_STATES.has(normalizedRun.state)) {
            activeRunsById.set(normalizedRun.dag_run_id, normalizedRun);
          }
        }
      }
    } catch {
      const fallback = await this.listDagRuns(dagId, 100);
      for (const run of fallback.dag_runs || []) {
        if (ACTIVE_DAG_RUN_STATES.has(run.state)) {
          activeRunsById.set(run.dag_run_id, run);
        }
      }
    }

    const dagRuns = [...activeRunsById.values()].sort((a, b) =>
      String(b.execution_date || "").localeCompare(
        String(a.execution_date || ""),
      ),
    );
    return {
      dag_runs: dagRuns,
      count: dagRuns.length,
      max_allowed: MAX_ACTIVE_DAG_RUNS_PER_DAG,
    };
  }

  async assertCanTriggerDag(dagId) {
    const activeRuns = await this.listActiveDagRuns(dagId);
    if (activeRuns.count < MAX_ACTIVE_DAG_RUNS_PER_DAG) {
      return activeRuns;
    }

    const sampleRunIds = activeRuns.dag_runs
      .slice(0, MAX_ACTIVE_DAG_RUNS_PER_DAG)
      .map((run) => `${run.dag_run_id}(${run.state})`)
      .join("、");
    throw new Error(
      `${DAG_TRIGGER_LIMIT_ERROR_PREFIX} ${dagId} 当前已有 ${activeRuns.count} 个 running/queued 任务，最多允许 ${MAX_ACTIVE_DAG_RUNS_PER_DAG} 个同时存在。请等待已有任务结束后再触发。${sampleRunIds ? `当前任务：${sampleRunIds}` : ""}`,
    );
  }

  async getDagRun(dagId, dagRunId) {
    const data = await this.apiGet(
      `/api/v1/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(dagRunId)}`,
    );
    return this.normalizeDagRun(data);
  }

  normalizeDagRun(run) {
    return {
      dag_id: run.dag_id,
      dag_run_id: run.dag_run_id || run.run_id,
      state: run.state || "unknown",
      conf: run.conf || {},
      execution_date: run.execution_date || run.logical_date || "",
      start_date: run.start_date || "",
      end_date: run.end_date || "",
      run_type: run.run_type || "",
    };
  }

  async listTaskInstances(dagId, dagRunId) {
    const data = await this.apiGet(
      `/api/v1/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(dagRunId)}/taskInstances`,
    );
    const taskInstances = (data.task_instances || [])
      .map((task) => this.normalizeTaskInstance(task))
      .sort((a, b) => a.task_id.localeCompare(b.task_id));
    return { task_instances: taskInstances };
  }

  async listTaskTries(dagId, dagRunId, taskId, mapIndex = -1) {
    const encodedDagId = encodeURIComponent(dagId);
    const encodedDagRunId = encodeURIComponent(dagRunId);
    const encodedTaskId = encodeURIComponent(taskId);
    const normalizedMapIndex = Number.isFinite(Number(mapIndex))
      ? Number(mapIndex)
      : -1;
    const taskPath =
      normalizedMapIndex >= 0
        ? `/api/v1/dags/${encodedDagId}/dagRuns/${encodedDagRunId}/taskInstances/${encodedTaskId}/${normalizedMapIndex}/tries`
        : `/api/v1/dags/${encodedDagId}/dagRuns/${encodedDagRunId}/taskInstances/${encodedTaskId}/tries`;
    const data = await this.apiGet(taskPath);
    const taskTries = (data.task_instances || [])
      .map((task) => this.normalizeTaskInstance(task))
      .sort((a, b) => Number(b.try_number || 0) - Number(a.try_number || 0));
    return {
      task_tries: taskTries,
      total: Number(data.total_entries || taskTries.length),
    };
  }

  normalizeTaskInstance(task) {
    return {
      task_id: task.task_id,
      state: task.state || "none",
      try_number: Number(task.try_number || 1),
      map_index: Number.isFinite(Number(task.map_index))
        ? Number(task.map_index)
        : -1,
      start_date: task.start_date || "",
      end_date: task.end_date || "",
      duration: task.duration || null,
      operator: task.operator || "",
      queued_when: task.queued_when || "",
    };
  }

  async triggerDag(dagId, conf = {}, runId = "") {
    await this.assertCanTriggerDag(dagId);
    const nextRunId =
      runId || `gui_${new Date().toISOString().replace(/[:.]/g, "-")}`;
    const data = await this.apiPost(
      `/api/v1/dags/${encodeURIComponent(dagId)}/dagRuns`,
      {
        conf,
        dag_run_id: nextRunId,
      },
    );
    return this.normalizeDagRun(data);
  }

  async setTaskState({
    dagId,
    dagRunId,
    taskId,
    mapIndex = -1,
    newState = "failed",
    dryRun = false,
  }) {
    const encodedDagId = encodeURIComponent(dagId);
    const encodedDagRunId = encodeURIComponent(dagRunId);
    const encodedTaskId = encodeURIComponent(taskId);
    const normalizedMapIndex = Number.isFinite(Number(mapIndex))
      ? Number(mapIndex)
      : -1;
    const taskPath =
      normalizedMapIndex >= 0
        ? `/api/v1/dags/${encodedDagId}/dagRuns/${encodedDagRunId}/taskInstances/${encodedTaskId}/${normalizedMapIndex}`
        : `/api/v1/dags/${encodedDagId}/dagRuns/${encodedDagRunId}/taskInstances/${encodedTaskId}`;
    return this.apiPatch(taskPath, {
      new_state: newState,
      dry_run: Boolean(dryRun),
    });
  }

  async terminateTask(params) {
    return this.setTaskState({
      ...params,
      newState: "failed",
      dryRun: false,
    });
  }

  async getTaskLog({
    dagId,
    dagRunId,
    taskId,
    executionDate,
    offset = 0,
    tryNumber = 1,
    mapIndex = -1,
  }) {
    if (!dagId || !taskId) {
      throw new Error("dagId 和 taskId 不能为空");
    }
    if (!executionDate && !dagRunId) {
      throw new Error("缺少 executionDate 或 dagRunId，无法读取日志");
    }

    if (executionDate) {
      try {
        return await this.getTaskLogFromUiEndpoint({
          dagId,
          taskId,
          executionDate,
          offset,
          tryNumber,
          mapIndex,
        });
      } catch (error) {
        if (!dagRunId) {
          throw error;
        }
      }
    }

    return this.getTaskLogFromApiEndpoint({
      dagId,
      dagRunId,
      taskId,
      tryNumber,
      mapIndex,
    });
  }

  async getTaskLogFromUiEndpoint({
    dagId,
    taskId,
    executionDate,
    offset,
    tryNumber,
    mapIndex,
  }) {
    const url = new URL(`${this.baseUrl}/get_logs_with_metadata`);
    url.searchParams.set("dag_id", dagId);
    url.searchParams.set("task_id", taskId);
    url.searchParams.set("map_index", String(mapIndex ?? -1));
    url.searchParams.set("execution_date", executionDate);
    url.searchParams.set(
      "try_number",
      String(Math.max(1, Number(tryNumber || 1))),
    );
    url.searchParams.set(
      "metadata",
      JSON.stringify({ end_of_log: false, log_pos: Number(offset || 0) }),
    );

    const response = await this.request(`${url.pathname}${url.search}`, {
      headers: {
        Accept: "*/*",
        "X-Requested-With": "XMLHttpRequest",
      },
    });
    const bodyText = await response.text();
    if (!response.ok) {
      throw new Error(
        `${response.status} ${response.statusText}: ${bodyText.slice(0, 300)}`,
      );
    }

    const data = JSON.parse(bodyText || "{}");
    const metadata = data.metadata || {};
    return {
      log: this.extractLogContent(data.message),
      next_offset: Number(metadata.log_pos ?? offset ?? 0),
      end_of_log: Boolean(metadata.end_of_log),
      source: "ui",
    };
  }

  async getTaskLogFromApiEndpoint({
    dagId,
    dagRunId,
    taskId,
    tryNumber,
    mapIndex,
  }) {
    const url = new URL(
      `${this.baseUrl}/api/v1/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(
        dagRunId,
      )}/taskInstances/${encodeURIComponent(taskId)}/logs/${Math.max(1, Number(tryNumber || 1))}`,
    );
    url.searchParams.set("full_content", "true");
    url.searchParams.set("map_index", String(mapIndex ?? -1));

    const response = await this.request(`${url.pathname}${url.search}`, {
      headers: {
        Accept: "text/plain,application/json,*/*",
      },
    });
    const bodyText = await response.text();
    if (!response.ok) {
      throw new Error(
        `${response.status} ${response.statusText}: ${bodyText.slice(0, 300)}`,
      );
    }

    let log = bodyText;
    try {
      const data = JSON.parse(bodyText);
      log = data.content || data.log || bodyText;
    } catch {
      // The stable API often returns plain text. Keep it as-is.
    }

    return {
      log,
      next_offset: log.length,
      end_of_log: true,
      source: "api",
    };
  }

  extractLogContent(message) {
    if (!message) {
      return "";
    }
    if (typeof message === "string") {
      return message;
    }
    if (Array.isArray(message)) {
      return message
        .map((entry) => {
          if (Array.isArray(entry)) {
            return entry.length > 1 ? entry[1] : entry[0];
          }
          return entry;
        })
        .filter(Boolean)
        .join("");
    }
    return String(message);
  }

  isTerminalState(state) {
    return TERMINAL_STATES.has(String(state || "").toLowerCase());
  }

  async relogin() {
    await this.login(true);
    return { ok: true, baseUrl: this.baseUrl, username: this.username };
  }
}

module.exports = {
  AirflowClient,
  TERMINAL_STATES,
};
