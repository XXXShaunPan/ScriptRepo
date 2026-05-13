const api = window.airflowApp;

const terminalStates = new Set([
  "success",
  "failed",
  "skipped",
  "upstream_failed",
  "removed",
]);
const activeStatePriority = [
  "running",
  "queued",
  "scheduled",
  "up_for_retry",
  "up_for_reschedule",
  "deferred",
  "restarting",
  "none",
];

const DAG_TAG_CATEGORIES = [
  {
    key: "paid_ads",
    label: "Paid Ads",
    tags: ["Paid Ads", "paid_ads", "paid-ads"],
  },
  {
    key: "listing",
    label: "Listing",
    tags: ["Listing"],
  },
  {
    key: "buyer",
    label: "买手",
    tags: ["买手"],
  },
];
const DAG_CATEGORY_FILTERS = [
  { key: "all", label: "全部" },
  ...DAG_TAG_CATEGORIES,
  { key: "other", label: "其他" },
];

const elements = {};
const state = {
  config: null,
  dagConfFieldMapping: [],
  lastView: {},
  owner: "Shaun",
  dags: [],
  dagSearch: "",
  dagCategory: "all",
  selectedDag: null,
  staticTasks: [],
  dagRuns: [],
  selectedRun: null,
  taskInstances: [],
  selectedTask: null,
  taskTries: [],
  selectedTryNumber: null,
  logText: "",
  logMeta: {
    offset: 0,
    endOfLog: false,
  },
  pollingTimer: null,
  pollInFlight: false,
  autoTail: true,
  autoFollowTask: true,
  isTriggering: false,
  isTerminating: false,
  isLoading: false,
};

function $(id) {
  return document.getElementById(id);
}

function cacheElements() {
  [
    "connectionText",
    "airflowServiceSelect",
    "ownerInput",
    "reloginButton",
    "refreshAllButton",
    "dagCategoryBar",
    "dagSearchInput",
    "dagCountText",
    "dagList",
    "selectedDagTitle",
    "selectedDagMeta",
    "refreshDagButton",
    "triggerForm",
    "runIdInput",
    "triggerButton",
    "mappedConfFields",
    "confJsonField",
    "confInput",
    "runStatusText",
    "runConfModal",
    "runConfModalTitle",
    "runConfModalMeta",
    "runConfModalText",
    "copyRunConfButton",
    "closeRunConfModalButton",
    "runList",
    "taskModeText",
    "taskList",
    "autoFollowToggle",
    "autoTailToggle",
    "refreshLogButton",
    "terminateTaskButton",
    "clearLogButton",
    "logContextText",
    "statusStrip",
    "logViewer",
    "toastHost",
  ].forEach((id) => {
    elements[id] = $(id);
  });
}

async function init() {
  cacheElements();
  bindEvents();
  setBusy(true, "正在初始化");

  try {
    const initData = await api.getInit();
    state.config = initData.config;
    state.dagConfFieldMapping = state.config.dagConfFieldMapping || [];
    state.lastView = initData.state || {};
    state.owner = state.lastView.owner || state.config.owner || "Shaun";
    state.dagCategory = normalizeDagCategoryKey(
      state.lastView.dagCategory || "all"
    );
    state.autoTail = state.lastView.autoTail !== false;
    state.autoFollowTask = state.lastView.autoFollowTask !== false;

    elements.ownerInput.value = state.owner;
    elements.autoTailToggle.checked = state.autoTail;
    elements.autoFollowToggle.checked = state.autoFollowTask;
    renderAirflowServiceOptions();

    await refreshDags({ restore: true });
  } catch (error) {
    showToast(`初始化失败：${error.message}`, "error");
    renderError(elements.dagList, error);
  } finally {
    setBusy(false);
  }
}

function bindEvents() {
  elements.refreshAllButton.addEventListener("click", () =>
    refreshDags({ restore: false })
  );
  elements.reloginButton.addEventListener("click", relogin);
  elements.refreshDagButton.addEventListener("click", () =>
    refreshSelectedDag({ keepSelection: true })
  );
  elements.airflowServiceSelect.addEventListener("change", (event) => {
    switchAirflowService(event.target.value);
  });
  elements.dagSearchInput.addEventListener("input", (event) => {
    state.dagSearch = event.target.value.trim().toLowerCase();
    renderDagList();
  });
  elements.dagCategoryBar.addEventListener("click", async (event) => {
    const button = event.target.closest(".category-button");
    if (!button) {
      return;
    }
    state.dagCategory = normalizeDagCategoryKey(
      button.dataset.category || "all"
    );
    renderDagCategories();
    renderDagList();
    saveLastView({ dagCategory: state.dagCategory });
    const visibleDags = getVisibleDags();
    const currentIsVisible = visibleDags.some(
      (dag) => dag.dag_id === state.selectedDag?.dag_id
    );
    if (currentIsVisible) {
      return;
    }
    if (visibleDags[0]) {
      await selectDag(visibleDags[0]);
    } else {
      resetDetail();
    }
  });
  elements.ownerInput.addEventListener("change", () => {
    state.owner = elements.ownerInput.value.trim() || "Shaun";
    refreshDags({ restore: false });
  });
  elements.triggerForm.addEventListener("submit", triggerSelectedDag);
  elements.copyRunConfButton.addEventListener("click", copySelectedRunConf);
  elements.closeRunConfModalButton.addEventListener("click", closeRunConfModal);
  elements.runConfModal.addEventListener("click", (event) => {
    if (event.target === elements.runConfModal) {
      closeRunConfModal();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !elements.runConfModal.hidden) {
      closeRunConfModal();
    }
  });
  elements.refreshLogButton.addEventListener("click", () =>
    loadLog({ reset: true })
  );
  elements.terminateTaskButton.addEventListener("click", terminateSelectedTask);
  elements.clearLogButton.addEventListener("click", () => {
    state.logText = "";
    state.logMeta = { offset: 0, endOfLog: false };
    renderLog();
  });
  elements.autoTailToggle.addEventListener("change", (event) => {
    state.autoTail = event.target.checked;
    saveLastView();
  });
  elements.autoFollowToggle.addEventListener("change", (event) => {
    state.autoFollowTask = event.target.checked;
    saveLastView();
  });
}

function setBusy(isBusy, text = "") {
  state.isLoading = isBusy;
  elements.connectionText.textContent = isBusy ? text : "";
  if (elements.airflowServiceSelect) {
    elements.airflowServiceSelect.disabled = isBusy;
  }
}

function renderAirflowServiceOptions() {
  const services = state.config?.services || [];
  const activeServiceId = state.config?.serviceId || services[0]?.id || "";
  elements.airflowServiceSelect.innerHTML = services.length
    ? services
        .map((service) => {
          const label =
            service.label || `${service.baseUrl} · ${service.username}`;
          return `<option value="${escapeAttr(service.id)}">${escapeHtml(
            label
          )}</option>`;
        })
        .join("")
    : `<option value="">未配置 Airflow</option>`;
  elements.airflowServiceSelect.value = activeServiceId;
  const activeService = services.find(
    (service) => service.id === activeServiceId
  );
  elements.airflowServiceSelect.title = activeService
    ? `${activeService.baseUrl} · ${activeService.username}`
    : "选择 Airflow 服务";
}

async function switchAirflowService(serviceId) {
  if (!serviceId || serviceId === state.config?.serviceId) {
    renderAirflowServiceOptions();
    return;
  }

  stopPolling();
  setBusy(true, "正在切换 Airflow");
  try {
    const result = await api.switchAirflowService(serviceId);
    state.config = result.config;
    state.dagConfFieldMapping = state.config.dagConfFieldMapping || [];
    state.owner = state.config.owner || "Shaun";
    state.dags = [];
    state.staticTasks = [];
    state.dagRuns = [];
    state.taskInstances = [];
    state.taskTries = [];
    state.selectedDag = null;
    state.selectedRun = null;
    state.selectedTask = null;
    state.selectedTryNumber = null;
    state.logText = "";
    state.logMeta = { offset: 0, endOfLog: false };
    elements.ownerInput.value = state.owner;
    renderAirflowServiceOptions();
    renderDagCategories();
    resetDetail();
    saveLastView({
      airflowServiceId: state.config.serviceId,
      owner: state.owner,
      selectedDagId: "",
      selectedDagRunId: "",
      selectedTaskId: "",
      executionDate: "",
      tryNumber: 1,
      logOffset: 0,
    });
    await refreshDags({ restore: false });
    showToast(`已切换到 ${state.config.serviceLabel || state.config.baseUrl}`);
  } catch (error) {
    showToast(`切换 Airflow 失败：${error.message}`, "error");
    renderAirflowServiceOptions();
  } finally {
    setBusy(false);
  }
}

async function relogin() {
  try {
    setBusy(true, "正在重连 Airflow");
    await api.relogin();
    showToast("Airflow 已重新登录");
    await refreshDags({ restore: true });
  } catch (error) {
    showToast(`重连失败：${error.message}`, "error");
  } finally {
    setBusy(false);
  }
}

async function refreshDags({ restore = false } = {}) {
  stopPolling();
  setBusy(true, `正在加载 ${state.owner} 的 DAG`);
  elements.dagList.innerHTML = `<div class="empty-state">加载 DAG 中...</div>`;
  saveLastView({ owner: state.owner });

  try {
    const result = await api.listDags(state.owner);
    state.dags = result.dags || [];
    renderDagCategories();
    renderDagList();

    const visibleDags = getVisibleDags();
    const restoreDagId = restore
      ? state.lastView.selectedDagId
      : state.selectedDag?.dag_id;
    const nextDag =
      visibleDags.find((dag) => dag.dag_id === restoreDagId) ||
      visibleDags.find((dag) => dag.dag_id === state.selectedDag?.dag_id) ||
      visibleDags[0] ||
      null;

    if (nextDag) {
      const restoreState = restore ? state.lastView : {};
      await selectDag(nextDag, { restoreState });
    } else {
      resetDetail();
    }
  } catch (error) {
    renderError(elements.dagList, error);
    showToast(`DAG 加载失败：${error.message}`, "error");
  } finally {
    setBusy(false);
  }
}

function renderDagList() {
  const filtered = getVisibleDags();
  const categoryTotal = state.dags.filter((dag) =>
    matchesDagCategory(dag, state.dagCategory)
  ).length;
  const categoryLabel = getDagCategoryConfig(state.dagCategory).label;

  elements.dagCountText.textContent =
    state.dagCategory === "all"
      ? `${filtered.length} / ${state.dags.length} items`
      : `${categoryLabel} · ${filtered.length} / ${categoryTotal} items`;
  if (!filtered.length) {
    elements.dagList.innerHTML = `<div class="empty-state">没有匹配的 DAG</div>`;
    return;
  }

  elements.dagList.innerHTML = filtered
    .map((dag) => {
      const activeClass =
        dag.dag_id === state.selectedDag?.dag_id ? " active" : "";
      const category = getDagCategory(dag);
      const tags = [
        `<span class="chip">${escapeHtml(
          dag.owners.join(", ") || "no owner"
        )}</span>`,
        `<span class="chip category">${escapeHtml(category.label)}</span>`,
        dag.is_paused
          ? `<span class="chip skipped">paused</span>`
          : `<span class="chip success">active</span>`,
      ].join("");
      return `
        <button class="dag-row${activeClass}" type="button" data-dag-id="${escapeAttr(
        dag.dag_id
      )}">
          <span class="dag-id">${escapeHtml(dag.dag_id)}</span>
          <span class="dag-description">${escapeHtml(
            dag.description || dag.fileloc || "-"
          )}</span>
          <span class="tag-row">${tags}</span>
        </button>
      `;
    })
    .join("");

  elements.dagList.querySelectorAll(".dag-row").forEach((row) => {
    row.addEventListener("click", () => {
      const dag = state.dags.find((item) => item.dag_id === row.dataset.dagId);
      if (dag) {
        selectDag(dag);
      }
    });
  });
}

function renderDagCategories() {
  elements.dagCategoryBar.innerHTML = DAG_CATEGORY_FILTERS.map((category) => {
    const activeClass = category.key === state.dagCategory ? " active" : "";
    const count =
      category.key === "all"
        ? state.dags.length
        : state.dags.filter((dag) => matchesDagCategory(dag, category.key))
            .length;
    return `
      <button class="category-button${activeClass}" type="button" data-category="${escapeAttr(
      category.key
    )}">
        <span>${escapeHtml(category.label)}</span>
        <strong>${escapeHtml(count)}</strong>
      </button>
    `;
  }).join("");
}

function getVisibleDags() {
  return state.dags.filter((dag) => {
    if (!matchesDagCategory(dag, state.dagCategory)) {
      return false;
    }
    const searchText = `${dag.dag_id} ${dag.description} ${dag.owners.join(
      " "
    )} ${(dag.tags || []).join(" ")}`.toLowerCase();
    return !state.dagSearch || searchText.includes(state.dagSearch);
  });
}

function matchesDagCategory(dag, categoryKey) {
  const normalizedCategory = normalizeDagCategoryKey(categoryKey);
  if (normalizedCategory === "all") {
    return true;
  }
  if (normalizedCategory === "other") {
    return !DAG_TAG_CATEGORIES.some((category) =>
      dagHasCategoryTag(dag, category)
    );
  }
  const category = DAG_TAG_CATEGORIES.find(
    (item) => item.key === normalizedCategory
  );
  return category ? dagHasCategoryTag(dag, category) : true;
}

function getDagCategory(dag) {
  const category = DAG_TAG_CATEGORIES.find((item) =>
    dagHasCategoryTag(dag, item)
  );
  return category || getDagCategoryConfig("other");
}

function dagHasCategoryTag(dag, category) {
  const dagTags = (dag.tags || []).map(normalizeDagTag).filter(Boolean);
  return category.tags.some((tag) => {
    const keyword = normalizeDagTag(tag);
    return keyword && dagTags.some((dagTag) => dagTag.includes(keyword));
  });
}

function getDagCategoryConfig(categoryKey) {
  return (
    DAG_CATEGORY_FILTERS.find((category) => category.key === categoryKey) ||
    DAG_CATEGORY_FILTERS[0]
  );
}

function normalizeDagCategoryKey(categoryKey) {
  const key = String(categoryKey || "all");
  return DAG_CATEGORY_FILTERS.some((category) => category.key === key)
    ? key
    : "all";
}

function normalizeDagTag(tag) {
  return String(tag || "")
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, " ");
}

async function selectDag(dag, { restoreState = {} } = {}) {
  state.selectedDag = dag;
  state.staticTasks = [];
  state.dagRuns = [];
  state.selectedRun = null;
  state.taskInstances = [];
  state.selectedTask = null;
  state.taskTries = [];
  state.selectedTryNumber = null;
  state.logText = "";
  state.logMeta = { offset: 0, endOfLog: false };
  renderDagList();
  renderSelectedDag();
  renderRuns();
  renderTasks();
  renderLog();
  saveLastView({
    selectedDagId: dag.dag_id,
    selectedDagRunId: "",
    selectedTaskId: "",
    tryNumber: 1,
    executionDate: "",
  });

  await refreshSelectedDag({ restoreState });
}

async function refreshSelectedDag({
  restoreState = {},
  keepSelection = false,
} = {}) {
  if (!state.selectedDag) {
    return;
  }
  const dagId = state.selectedDag.dag_id;
  elements.runList.innerHTML = `<div class="empty-state">加载 runs 中...</div>`;
  elements.taskList.innerHTML = `<div class="empty-state">加载 Task Tries 中...</div>`;

  try {
    const [taskResult, runResult] = await Promise.all([
      api.listTasks(dagId),
      api.listDagRuns(dagId, 30),
    ]);
    state.staticTasks = taskResult.tasks || [];
    state.dagRuns = runResult.dag_runs || [];
    renderSelectedDag();
    renderRuns();

    const preferredRunId =
      restoreState.selectedDagRunId ||
      (keepSelection && state.selectedRun?.dag_run_id);
    const nextRun =
      state.dagRuns.find((run) => run.dag_run_id === preferredRunId) ||
      state.dagRuns[0] ||
      null;

    if (nextRun) {
      await selectRun(nextRun, {
        preferredTaskId:
          restoreState.selectedTaskId ||
          (keepSelection && state.selectedTask?.task_id),
        preferredTryNumber:
          restoreState.tryNumber || (keepSelection && state.selectedTryNumber),
        restored: Boolean(restoreState.selectedTaskId),
      });
      if (restoreState.selectedTaskId) {
        startPolling();
      }
    } else {
      state.taskInstances = [];
      renderTasks();
      renderLog();
    }
  } catch (error) {
    renderError(elements.taskList, error);
    showToast(`当前 DAG 刷新失败：${error.message}`, "error");
  }
}

function renderSelectedDag() {
  if (!state.selectedDag) {
    resetDetail();
    return;
  }
  elements.selectedDagTitle.textContent = state.selectedDag.dag_id;
  elements.selectedDagMeta.textContent =
    state.selectedDag.description ||
    `${state.selectedDag.owners.join(", ")} · ${
      state.selectedDag.fileloc || ""
    }`;
  renderDagConfFields();
}

function resetDetail() {
  state.selectedDag = null;
  state.staticTasks = [];
  state.dagRuns = [];
  state.selectedRun = null;
  state.taskInstances = [];
  state.selectedTask = null;
  state.taskTries = [];
  state.selectedTryNumber = null;
  state.logText = "";
  elements.selectedDagTitle.textContent = "选择一个 DAG";
  elements.selectedDagMeta.textContent = "等待加载";
  renderDagConfFields();
  elements.runList.innerHTML = `<div class="empty-state">暂无 DAG</div>`;
  elements.taskModeText.textContent = "等待 task";
  elements.taskList.innerHTML = `<div class="empty-state">暂无 Task Tries</div>`;
  renderLog();
}

function renderRuns() {
  elements.runStatusText.textContent = state.selectedRun
    ? state.selectedRun.state
    : `${state.dagRuns.length} runs`;

  if (!state.dagRuns.length) {
    elements.runList.innerHTML = `<div class="empty-state">暂无 run</div>`;
    return;
  }

  elements.runList.innerHTML = state.dagRuns
    .map((run) => {
      const activeClass =
        run.dag_run_id === state.selectedRun?.dag_run_id ? " active" : "";
      const confPreview = formatRunConfPreview(run.conf);
      const confTitle = formatRunConfTitle(run.conf);
      return `
        <div class="run-row${activeClass}" role="button" tabindex="0" data-run-id="${escapeAttr(
        run.dag_run_id
      )}">
          <div class="run-row-main">
            <span class="run-id">${escapeHtml(run.dag_run_id)}</span>
            <span class="run-meta">${formatTime(
              run.execution_date
            )} · ${formatTime(run.start_date)} · ${escapeHtml(
        run.run_type || "-"
      )}</span>
            <span class="run-conf" data-conf-run-id="${escapeAttr(
              run.dag_run_id
            )}" title="${escapeAttr(confTitle)}">${escapeHtml(
        confPreview
      )}</span>
            <span class="tag-row"><span class="chip ${stateClass(
              run.state
            )}">${escapeHtml(run.state || "unknown")}</span></span>
          </div>
          <button class="icon-button small run-conf-button" type="button" data-conf-run-id="${escapeAttr(
            run.dag_run_id
          )}" title="查看并复制 Run Conf">
            <svg><use href="#icon-copy"></use></svg>
          </button>
        </div>
      `;
    })
    .join("");

  elements.runList.querySelectorAll(".run-row").forEach((row) => {
    const chooseRun = () => {
      const run = state.dagRuns.find(
        (item) => item.dag_run_id === row.dataset.runId
      );
      if (run) {
        selectRun(run);
      }
    };
    row.addEventListener("click", chooseRun);
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        chooseRun();
      }
    });
  });

  elements.runList
    .querySelectorAll(".run-conf-button, .run-conf")
    .forEach((button) => {
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        const run = state.dagRuns.find(
          (item) => item.dag_run_id === button.dataset.confRunId
        );
        if (run) {
          openRunConfModal(run);
        }
      });
    });
}

function openRunConfModal(run) {
  elements.runConfModalMeta.textContent = run.dag_run_id;
  elements.runConfModalText.textContent = formatRunConfTitle(run.conf);
  elements.runConfModal.hidden = false;
}

function closeRunConfModal() {
  elements.runConfModal.hidden = true;
}

async function copySelectedRunConf() {
  const text = elements.runConfModalText.textContent || "{}";
  try {
    await copyText(text);
    showToast("Run Conf 已复制");
  } catch (error) {
    showToast(`复制失败：${error.message}`, "error");
  }
}

async function copyText(text) {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return;
    } catch {
      // file:// renderers can reject the Clipboard API; use the selection fallback below.
    }
  }

  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.select();
  const ok = document.execCommand("copy");
  textarea.remove();
  if (!ok) {
    throw new Error("当前环境不支持剪贴板写入");
  }
}

async function selectRun(
  run,
  { preferredTaskId = "", preferredTryNumber = "", restored = false } = {}
) {
  state.selectedRun = run;
  state.selectedTask = null;
  state.taskTries = [];
  state.selectedTryNumber = null;
  state.logText = "";
  state.logMeta = { offset: 0, endOfLog: false };
  renderRuns();
  elements.taskList.innerHTML = `<div class="empty-state">加载 Task Tries 中...</div>`;
  saveLastView({
    selectedDagRunId: run.dag_run_id,
    executionDate: run.execution_date,
  });

  try {
    await refreshTaskInstances();
    const preferredTask = state.taskInstances.find(
      (task) => task.task_id === preferredTaskId
    );
    const nextTask =
      preferredTask ||
      pickCurrentTask(state.taskInstances) ||
      state.taskInstances[0] ||
      null;
    if (nextTask) {
      await selectTask(nextTask, {
        resetLog: true,
        restored,
        preferredTryNumber,
      });
    } else {
      renderTasks();
      renderLog();
    }

    if (!terminalStates.has(String(run.state || "").toLowerCase())) {
      startPolling();
    }
  } catch (error) {
    renderError(elements.taskList, error);
    showToast(`Task instance 加载失败：${error.message}`, "error");
  }
}

async function refreshTaskInstances() {
  if (!state.selectedDag || !state.selectedRun) {
    state.taskInstances = [];
    return;
  }
  const result = await api.listTaskInstances(
    state.selectedDag.dag_id,
    state.selectedRun.dag_run_id
  );
  state.taskInstances = result.task_instances || [];
  renderTasks();
}

function renderTasks() {
  const taskName = state.selectedTask?.task_id || "等待 task";
  elements.taskModeText.textContent = taskName;

  if (!state.selectedRun) {
    elements.taskList.innerHTML = `<div class="empty-state">选择一个 run 后查看 Task Tries</div>`;
    return;
  }
  if (!state.selectedTask) {
    elements.taskList.innerHTML = `<div class="empty-state">等待自动选择当前 task</div>`;
    return;
  }
  if (!state.taskTries.length) {
    elements.taskList.innerHTML = `<div class="empty-state">暂无 Task Tries</div>`;
    return;
  }

  elements.taskList.innerHTML = state.taskTries
    .map((taskTry) => {
      const tryNumber = Number(taskTry.try_number || 1);
      const activeClass =
        tryNumber === Number(state.selectedTryNumber || 0) ? " active" : "";
      const metaParts = [
        formatTime(taskTry.start_date),
        taskTry.end_date ? `ended ${formatTime(taskTry.end_date)}` : "",
        formatDuration(taskTry.duration),
      ].filter(Boolean);
      return `
        <button class="task-row${activeClass}" type="button" data-try-number="${escapeAttr(
        tryNumber
      )}">
          <span class="task-row-grid">
            <span>
              <span class="task-id">try ${escapeHtml(tryNumber)}</span>
              <span class="task-meta">${escapeHtml(
                metaParts.join(" · ") || "尚无时间记录"
              )}</span>
            </span>
            <span class="chip ${stateClass(taskTry.state)}">${escapeHtml(
        taskTry.state || "none"
      )}</span>
          </span>
        </button>
      `;
    })
    .join("");

  elements.taskList.querySelectorAll(".task-row").forEach((row) => {
    row.addEventListener("click", () => {
      const taskTry = state.taskTries.find(
        (item) =>
          Number(item.try_number || 1) === Number(row.dataset.tryNumber || 1)
      );
      if (taskTry) {
        selectTry(taskTry, { resetLog: true });
      }
    });
  });
}

async function refreshTaskTries() {
  if (!state.selectedDag || !state.selectedRun || !state.selectedTask) {
    state.taskTries = [];
    renderTasks();
    return;
  }

  try {
    const result = await api.listTaskTries(
      state.selectedDag.dag_id,
      state.selectedRun.dag_run_id,
      state.selectedTask.task_id,
      state.selectedTask.map_index
    );
    state.taskTries = result.task_tries || [];
  } catch (error) {
    state.taskTries = buildFallbackTaskTries(state.selectedTask);
    showToast(
      `Task Tries 加载失败，已用 try_number 兜底：${error.message}`,
      "error"
    );
  }
  renderTasks();
}

function buildFallbackTaskTries(task) {
  const maxTryNumber = Math.max(1, Number(task?.try_number || 1));
  return Array.from({ length: maxTryNumber }, (_, index) => {
    const tryNumber = index + 1;
    return {
      ...task,
      try_number: tryNumber,
      state: tryNumber === maxTryNumber ? task.state : "failed",
    };
  }).sort((a, b) => Number(b.try_number || 0) - Number(a.try_number || 0));
}

async function selectTask(
  task,
  { resetLog = true, restored = false, preferredTryNumber = "" } = {}
) {
  const isSameTask = task.task_id === state.selectedTask?.task_id;
  state.selectedTask = task;
  if (resetLog) {
    state.logText = "";
    state.logMeta = { offset: 0, endOfLog: false };
  }
  const desiredTryNumber = Number(
    preferredTryNumber ||
      (isSameTask && state.selectedTryNumber) ||
      task.try_number ||
      1
  );
  renderTasks();
  await refreshTaskTries();
  const nextTry =
    state.taskTries.find(
      (taskTry) => Number(taskTry.try_number || 1) === desiredTryNumber
    ) ||
    state.taskTries[0] ||
    task;
  state.selectedTryNumber = Number(nextTry.try_number || desiredTryNumber || 1);
  renderTasks();
  renderLog();
  saveLastView({
    selectedTaskId: task.task_id,
    tryNumber: Math.max(
      1,
      Number(state.selectedTryNumber || task.try_number || 1)
    ),
    mapIndex: Number.isFinite(Number(task.map_index))
      ? Number(task.map_index)
      : -1,
  });

  if (state.selectedRun) {
    await loadLog({ reset: true });
    if (restored) {
      showToast("已恢复上一次查看的 task log");
    }
  }
}

async function selectTry(taskTry, { resetLog = true } = {}) {
  state.selectedTryNumber = Math.max(1, Number(taskTry.try_number || 1));
  if (resetLog) {
    state.logText = "";
    state.logMeta = { offset: 0, endOfLog: false };
  }
  renderTasks();
  renderLog();
  saveLastView({ tryNumber: state.selectedTryNumber, logOffset: 0 });
  await loadLog({ reset: true });
}

async function triggerSelectedDag(event) {
  event.preventDefault();
  if (!state.selectedDag || state.isTriggering) {
    return;
  }

  let conf;
  try {
    conf = buildDagRunConf();
  } catch (error) {
    showToast(error.message, "error");
    return;
  }

  state.isTriggering = true;
  elements.triggerButton.disabled = true;
  stopPolling();

  try {
    const run = await api.triggerDag(
      state.selectedDag.dag_id,
      conf,
      elements.runIdInput.value.trim()
    );
    showToast(`已触发 ${run.dag_run_id}`);
    await api.notify(`Airflow DAG 已触发：${state.selectedDag.dag_id}`);

    state.dagRuns = [
      run,
      ...state.dagRuns.filter((item) => item.dag_run_id !== run.dag_run_id),
    ];
    renderRuns();
    await selectRun(run);
    startPolling();
  } catch (error) {
    showToast(`Trigger 失败：${error.message}`, "error");
  } finally {
    state.isTriggering = false;
    elements.triggerButton.disabled = false;
  }
}

function getConfFieldConfig(dagId) {
  const mapping = state.dagConfFieldMapping.find(
    (item) => item.dag_name === dagId
  );
  if (!mapping) {
    return [];
  }
  return (mapping.keys || []).map((key, index) => ({
    key,
    alias: mapping.alias?.[index] || key,
    tip: mapping.tips?.[index] || "",
  }));
}

function renderDagConfFields() {
  const dagId = state.selectedDag?.dag_id || "";
  const fields = getConfFieldConfig(dagId);
  if (!fields.length) {
    elements.mappedConfFields.hidden = true;
    elements.mappedConfFields.innerHTML = "";
    elements.confJsonField.hidden = false;
    return;
  }

  elements.confJsonField.hidden = true;
  elements.mappedConfFields.hidden = false;
  elements.mappedConfFields.innerHTML = `
    <div class="mapped-conf-title">dag_run conf</div>
    <div class="mapped-conf-grid">
      ${fields
        .map(
          (field) => `
            <label class="mapped-conf-field">
              <span class="mapped-conf-input-wrap">
                <input
                  type="text"
                  data-conf-key="${escapeAttr(field.key)}"
                  placeholder="请输入 ${escapeAttr(field.alias)}"
                  aria-label="${escapeAttr(field.alias)}"
                  autocomplete="off"
                />
                ${renderConfFieldTip(field)}
              </span>
            </label>
          `
        )
        .join("")}
    </div>
  `;
}

function renderConfFieldTip(field) {
  if (!field.tip) {
    return "";
  }
  return `
    <span class="field-info" tabindex="0" aria-label="${escapeAttr(field.tip)}">
      i
      <span class="field-tooltip" role="tooltip">${escapeHtml(field.tip)}</span>
    </span>
  `;
}

function buildDagRunConf() {
  const dagId = state.selectedDag?.dag_id || "";
  const fields = getConfFieldConfig(dagId);
  if (!fields.length) {
    let conf;
    try {
      conf = JSON.parse(elements.confInput.value || "{}");
    } catch (error) {
      elements.confInput.focus();
      throw new Error(`conf JSON 格式错误：${error.message}`);
    }
    if (!conf || typeof conf !== "object" || Array.isArray(conf)) {
      elements.confInput.focus();
      throw new Error("conf 必须是 JSON object");
    }
    return conf;
  }

  const conf = {};
  for (const field of fields) {
    const input = findConfInput(field.key);
    const value = input?.value.trim() || "";
    if (!value) {
      input?.focus();
      throw new Error(`${field.alias} 不能为空`);
    }
    conf[field.key] = value;
  }
  return conf;
}

function findConfInput(key) {
  return [
    ...elements.mappedConfFields.querySelectorAll("[data-conf-key]"),
  ].find((input) => input.dataset.confKey === key);
}

async function terminateSelectedTask() {
  if (state.isTerminating) {
    return;
  }
  if (!state.selectedDag || !state.selectedRun || !state.selectedTask) {
    showToast("请先选择一个正在运行的 DAG Run / Task", "error");
    return;
  }

  const taskId = state.selectedTask.task_id;
  const runId = state.selectedRun.dag_run_id;
  const confirmed = window.confirm(
    `确认终止当前 task 吗？\n\nDAG: ${state.selectedDag.dag_id}\nRun: ${runId}\nTask: ${taskId}\n\n此操作会将 task instance 标记为 failed。`
  );
  if (!confirmed) {
    return;
  }

  state.isTerminating = true;
  elements.terminateTaskButton.disabled = true;
  stopPolling();

  try {
    await api.terminateTask({
      dagId: state.selectedDag.dag_id,
      dagRunId: runId,
      taskId,
      mapIndex: Number.isFinite(Number(state.selectedTask.map_index))
        ? Number(state.selectedTask.map_index)
        : -1,
    });
    showToast(`已终止 task：${taskId}`);
    await api.notify(`Airflow task 已终止：${taskId}`);

    const freshRun = await api
      .getDagRun(state.selectedDag.dag_id, runId)
      .catch(() => null);
    if (freshRun) {
      state.selectedRun = { ...state.selectedRun, ...freshRun };
      state.dagRuns = state.dagRuns.map((run) =>
        run.dag_run_id === runId ? { ...run, ...state.selectedRun } : run
      );
    }

    await refreshTaskInstances();
    const updatedTask = state.taskInstances.find(
      (task) => task.task_id === taskId
    );
    if (updatedTask) {
      state.selectedTask = updatedTask;
    }
    await refreshTaskTries();
    await loadLog({ reset: true });
    renderRuns();

    if (
      !terminalStates.has(String(state.selectedRun?.state || "").toLowerCase())
    ) {
      startPolling();
    }
  } catch (error) {
    showToast(`终止 task 失败：${error.message}`, "error");
    if (
      state.selectedRun &&
      !terminalStates.has(String(state.selectedRun.state || "").toLowerCase())
    ) {
      startPolling();
    }
  } finally {
    state.isTerminating = false;
    elements.terminateTaskButton.disabled = false;
  }
}

function startPolling() {
  stopPolling();
  state.pollingTimer = setInterval(pollSelectedRun, 2500);
  pollSelectedRun();
}

function stopPolling() {
  if (state.pollingTimer) {
    clearInterval(state.pollingTimer);
    state.pollingTimer = null;
  }
}

async function pollSelectedRun() {
  if (state.pollInFlight || !state.selectedDag || !state.selectedRun) {
    return;
  }
  state.pollInFlight = true;

  try {
    const dagId = state.selectedDag.dag_id;
    const runId = state.selectedRun.dag_run_id;
    const [freshRun] = await Promise.all([
      api.getDagRun(dagId, runId),
      refreshTaskInstances(),
    ]);
    state.selectedRun = {
      ...state.selectedRun,
      ...freshRun,
    };
    state.dagRuns = state.dagRuns.map((run) =>
      run.dag_run_id === runId ? { ...run, ...state.selectedRun } : run
    );

    const activeTask = pickCurrentTask(state.taskInstances);
    const selectedFreshTask = state.taskInstances.find(
      (task) => task.task_id === state.selectedTask?.task_id
    );
    if (
      state.autoFollowTask &&
      activeTask &&
      activeTask.task_id !== state.selectedTask?.task_id
    ) {
      await selectTask(activeTask, { resetLog: true });
    } else if (selectedFreshTask) {
      state.selectedTask = selectedFreshTask;
      await refreshTaskTries();
      await loadLog({ reset: false });
    } else if (activeTask) {
      await selectTask(activeTask, { resetLog: true });
    }

    renderRuns();
    renderLogHeader();
    saveLastView();

    if (
      terminalStates.has(String(state.selectedRun.state || "").toLowerCase())
    ) {
      await loadLog({ reset: false });
      stopPolling();
      showToast(`Run 已结束：${state.selectedRun.state}`);
    }
  } catch (error) {
    showToast(`自动刷新失败：${error.message}`, "error");
  } finally {
    state.pollInFlight = false;
  }
}

async function loadLog({ reset = false } = {}) {
  if (!state.selectedDag || !state.selectedRun || !state.selectedTask) {
    renderLog();
    return;
  }

  const offset = reset ? 0 : Number(state.logMeta.offset || 0);
  const params = {
    dagId: state.selectedDag.dag_id,
    dagRunId: state.selectedRun.dag_run_id,
    taskId: state.selectedTask.task_id,
    executionDate: state.selectedRun.execution_date,
    offset,
    tryNumber: Math.max(
      1,
      Number(state.selectedTryNumber || state.selectedTask.try_number || 1)
    ),
    mapIndex: Number.isFinite(Number(state.selectedTask.map_index))
      ? Number(state.selectedTask.map_index)
      : -1,
  };

  try {
    const result = await api.getTaskLog(params);
    const nextOffset = Number(result.next_offset ?? offset);
    const logChunk = stripAnsi(String(result.log || ""));

    if (result.source === "api") {
      state.logText = logChunk || state.logText || "(暂无日志)";
    } else if (reset) {
      state.logText = logChunk || "(暂无日志)";
    } else if (logChunk && nextOffset !== offset) {
      state.logText =
        state.logText === "(暂无日志)" ? logChunk : state.logText + logChunk;
    }
    state.logMeta = {
      offset: nextOffset,
      endOfLog: Boolean(result.end_of_log),
      source: result.source,
    };
    renderLog();
    saveLastView({ logOffset: nextOffset });
  } catch (error) {
    state.logText = state.logText || `日志加载失败：${error.message}`;
    renderLog();
    showToast(`日志加载失败：${error.message}`, "error");
  }
}

function pickCurrentTask(tasks) {
  if (!tasks.length) {
    return null;
  }

  const withPriority = tasks
    .map((task) => ({
      task,
      priority: activeStatePriority.indexOf(
        String(task.state || "none").toLowerCase()
      ),
    }))
    .filter((item) => item.priority !== -1)
    .sort((a, b) => a.priority - b.priority);

  if (withPriority.length) {
    return withPriority[0].task;
  }

  return [...tasks].sort((a, b) => {
    const aTime = Date.parse(a.end_date || a.start_date || "") || 0;
    const bTime = Date.parse(b.end_date || b.start_date || "") || 0;
    return bTime - aTime;
  })[0];
}

function renderLog() {
  renderLogHeader();
  elements.logViewer.innerHTML = renderLogLines(
    state.logText || "等待 task log..."
  );
  if (state.autoTail) {
    requestAnimationFrame(() => {
      elements.logViewer.scrollTop = elements.logViewer.scrollHeight;
    });
  }
}

function renderLogLines(rawText) {
  return splitLogLines(rawText)
    .map((line) => {
      const level = detectLogLevel(line);
      const emptyClass = line.trim() ? "" : " log-line-empty";
      return `<div class="log-line log-line-${level}${emptyClass}">${escapeHtml(
        line || " "
      )}</div>`;
    })
    .join("");
}

function getSelectedTry() {
  return state.taskTries.find(
    (taskTry) =>
      Number(taskTry.try_number || 1) === Number(state.selectedTryNumber || 0)
  );
}

function splitLogLines(rawText) {
  const normalized = String(rawText || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");
  const lines = normalized.split("\n");
  while (lines.length > 1 && !lines[lines.length - 1].trim()) {
    lines.pop();
  }
  return lines.length ? lines : [""];
}

function detectLogLevel(line) {
  const text = String(line || "").toLowerCase();
  const explicitLevel = text.match(
    /\b(critical|fatal|error|warning|warn|info|debug)\b/
  );
  if (explicitLevel) {
    const level = explicitLevel[1];
    if (level === "critical" || level === "fatal" || level === "error") {
      return "error";
    }
    if (level === "warning" || level === "warn") {
      return "warning";
    }
    if (level === "debug") {
      return "debug";
    }
    return "info";
  }
  if (/\b(traceback|exception|failed|failure)\b/.test(text)) {
    return "error";
  }
  if (/\b(retry)\b/.test(text)) {
    return "warning";
  }
  if (
    /\b(success|succeeded|complete|completed|done)\b/.test(text) ||
    text.includes("成功")
  ) {
    return "success";
  }
  return "default";
}

function renderLogHeader() {
  if (!state.selectedDag || !state.selectedTask) {
    elements.logContextText.textContent = "未选择 task";
    elements.statusStrip.innerHTML = "";
    return;
  }

  const runText = state.selectedRun ? state.selectedRun.dag_run_id : "no run";
  elements.logContextText.textContent = `${state.selectedDag.dag_id} / ${runText} / ${state.selectedTask.task_id}`;
  const chips = [];
  if (state.selectedRun) {
    chips.push(
      `<span class="chip ${stateClass(
        state.selectedRun.state
      )}">run ${escapeHtml(state.selectedRun.state)}</span>`
    );
  }
  if (state.selectedTask.state) {
    chips.push(
      `<span class="chip ${stateClass(
        state.selectedTask.state
      )}">task ${escapeHtml(state.selectedTask.state)}</span>`
    );
  }
  const selectedTry = getSelectedTry();
  const selectedTryState = selectedTry?.state || state.selectedTask.state || "";
  const tryClass = selectedTryState ? stateClass(selectedTryState) : "";
  chips.push(
    `<span class="chip ${tryClass}">try ${Math.max(
      1,
      Number(state.selectedTryNumber || state.selectedTask.try_number || 1)
    )}</span>`
  );
  if (state.logMeta.source) {
    chips.push(
      `<span class="chip">log ${escapeHtml(state.logMeta.source)}</span>`
    );
  }
  elements.statusStrip.innerHTML = chips.join("");
}

function saveLastView(extra = {}) {
  const patch = {
    airflowServiceId: state.config?.serviceId || "",
    owner: state.owner,
    selectedDagId: state.selectedDag?.dag_id || "",
    selectedDagRunId: state.selectedRun?.dag_run_id || "",
    selectedTaskId: state.selectedTask?.task_id || "",
    executionDate: state.selectedRun?.execution_date || "",
    dagCategory: state.dagCategory,
    tryNumber: state.selectedTask
      ? Math.max(
          1,
          Number(state.selectedTryNumber || state.selectedTask.try_number || 1)
        )
      : 1,
    mapIndex:
      state.selectedTask &&
      Number.isFinite(Number(state.selectedTask.map_index))
        ? Number(state.selectedTask.map_index)
        : -1,
    logOffset: state.logMeta.offset || 0,
    autoTail: state.autoTail,
    autoFollowTask: state.autoFollowTask,
    ...extra,
  };

  api.saveState(patch).catch(() => {});
}

function renderError(container, error) {
  container.innerHTML = `<div class="error-state">${escapeHtml(
    error.message || String(error)
  )}</div>`;
}

function showToast(message, type = "default") {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.textContent = message;
  elements.toastHost.appendChild(toast);
  setTimeout(() => {
    toast.remove();
  }, 4200);
}

function stateClass(value) {
  return String(value || "none")
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_");
}

function formatTime(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function formatDuration(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) {
    return "";
  }
  if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = Math.round(seconds % 60);
  if (minutes < 60) {
    return `${minutes}m ${remainingSeconds}s`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours}h ${remainingMinutes}m`;
}

function formatRunConfPreview(conf) {
  if (
    !conf ||
    (typeof conf === "object" &&
      !Array.isArray(conf) &&
      !Object.keys(conf).length)
  ) {
    return "{}";
  }
  try {
    return JSON.stringify(conf);
  } catch {
    return String(conf);
  }
}

function formatRunConfTitle(conf) {
  if (
    !conf ||
    (typeof conf === "object" &&
      !Array.isArray(conf) &&
      !Object.keys(conf).length)
  ) {
    return "{}";
  }
  try {
    return JSON.stringify(conf, null, 2);
  } catch {
    return String(conf);
  }
}

function stripAnsi(value) {
  return value.replace(/\u001b\[[0-9;]*m/g, "");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function escapeAttr(value) {
  return escapeHtml(value).replace(/`/g, "&#096;");
}

document.addEventListener("DOMContentLoaded", init);
