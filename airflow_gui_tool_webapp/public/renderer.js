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

const DAG_CATEGORY_ALL = { key: "all", label: "全部" };
const DAG_CATEGORY_OTHER = { key: "other", label: "其他" };
const WORKSPACE_MIN_WIDTHS = {
  dag: 240,
  detail: 320,
  log: 380,
};
const VIEWPORT_WARNING_THRESHOLD = {
  width: 1200,
  height: 640,
};
const MOBILE_VIEW_MEDIA_QUERY = "(max-width: 900px)";
const ADD_DAG_TEMPLATES = {
  bash: "Bash 命令",
  python: "Python 调用",
};
const ADD_DAG_INPUT_IDS = [
  "newDagIdInput",
  "newDagDescriptionInput",
  "newDagTagsInput",
  "newDagFileNameInput",
  "newDagPythonImportInput",
];

const elements = {};
const state = {
  config: null,
  dagConfFieldMapping: [],
  systemUsername: "",
  canToggleDag: false,
  lastView: {},
  owner: "Shaun",
  dags: [],
  dagSearch: "",
  dagCategory: "all",
  selectedDag: null,
  dagCodeModalDagId: "",
  dagCodeText: "",
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
  logSearch: {
    open: false,
    query: "",
    currentIndex: -1,
    matches: [],
  },
  pollingTimer: null,
  pollInFlight: false,
  autoTail: true,
  autoFollowTask: true,
  updateInfo: null,
  updateStatus: "idle",
  updateProgress: 0,
  isCheckingUpdate: false,
  workspaceLayout: {},
  workspaceResize: null,
  mobileView: "dag",
  addDagTemplate: "bash",
  addDagCodeDirty: false,
  addDagLastGeneratedCode: "",
  isRenderingAddDagCode: false,
  isTriggering: false,
  isTerminating: false,
  isLoading: false,
  hasInitialized: false,
  viewportIsTooSmall: false,
  viewportWarningDismissed: false,
  viewportResizeTimer: null,
  isMobileViewport: false,
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
    "addDagButton",
    "checkUpdateButton",
    "guideTipsButton",
    "dagCategoryBar",
    "mobileViewBar",
    "workspace",
    "dagPane",
    "detailPane",
    "logPane",
    "resizeDagDetail",
    "resizeDetailLog",
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
    "dagCodeModal",
    "dagCodeModalTitle",
    "dagCodeModalMeta",
    "dagCodeModalText",
    "copyDagCodeModalButton",
    "closeDagCodeModalButton",
    "addDagModal",
    "addDagModalTitle",
    "addDagOutputHint",
    "addDagForm",
    "addDagTemplateGroup",
    "newDagIdInput",
    "newDagDescriptionInput",
    "newDagTagsInput",
    "newDagFileNameInput",
    "newDagPythonImportField",
    "newDagPythonImportInput",
    "copyDagCodeButton",
    "saveDagFileButton",
    "regenerateDagCodeButton",
    "closeAddDagModalButton",
    "addDagPreviewMeta",
    "addDagCodePreview",
    "updateModal",
    "updateModalTitle",
    "updateModalMeta",
    "updateVersionText",
    "updateProgress",
    "updateProgressBar",
    "updateProgressText",
    "updateNotes",
    "downloadUpdateButton",
    "closeUpdateModalButton",
    "guideTipsModal",
    "closeGuideTipsButton",
    "confirmGuideTipsButton",
    "viewportWarningModal",
    "viewportWarningSize",
    "closeViewportWarningButton",
    "runList",
    "taskModeText",
    "taskList",
    "autoFollowToggle",
    "autoTailToggle",
    "logSearchButton",
    "refreshLogButton",
    "terminateTaskButton",
    "clearLogButton",
    "logContextText",
    "statusStrip",
    "logSearchBar",
    "logSearchInput",
    "logSearchCount",
    "prevLogMatchButton",
    "nextLogMatchButton",
    "closeLogSearchButton",
    "logViewer",
    "toastHost",
  ].forEach((id) => {
    elements[id] = $(id);
  });
}

async function init() {
  cacheElements();
  state.isMobileViewport = isMobileViewport();
  if (state.isMobileViewport) {
    setMobileView("dag", { save: false });
  }
  bindEvents();
  checkViewportSize({ force: true });
  setBusy(true, "正在初始化");

  try {
    const initData = await api.getInit();
    state.config = initData.config;
    state.dagConfFieldMapping = state.config.dagConfFieldMapping || [];
    state.systemUsername = state.config.systemUsername || "";
    state.canToggleDag = Boolean(state.config.canToggleDag);
    state.lastView = initData.state || {};
    state.owner = state.lastView.owner || state.config.owner;
    state.dagCategory = state.lastView.dagCategory || DAG_CATEGORY_ALL.key;
    state.autoTail = state.lastView.autoTail !== false;
    state.autoFollowTask = state.lastView.autoFollowTask !== false;
    state.workspaceLayout = normalizeWorkspaceLayout(
      state.lastView.workspaceLayout,
    );

    elements.ownerInput.value = state.owner;
    elements.runIdInput.placeholder = `${formatDagRunIdUsername()}_YYYYMMDD_HHMMSS`;
    elements.autoTailToggle.checked = state.autoTail;
    elements.autoFollowToggle.checked = state.autoFollowTask;
    setMobileView(
      state.isMobileViewport ? "dag" : state.lastView.mobileView || "dag",
      { save: false },
    );
    renderAirflowServiceOptions();
    applyWorkspaceLayout();
    requestAnimationFrame(() => {
      clampAndApplyWorkspaceLayout({ save: false });
    });

    await refreshDags({ restore: true });
    if (state.isMobileViewport) {
      setMobileView("dag", { save: false });
    }
  } catch (error) {
    showToast(`初始化失败：${error.message}`, "error");
    renderError(elements.dagList, error);
  } finally {
    setBusy(false);
    state.hasInitialized = true;
    checkViewportSize({ force: true });
    maybeOpenGuideTips();
    maybeCheckUpdateOnStart();
  }
}

function bindEvents() {
  elements.refreshAllButton.addEventListener("click", () =>
    refreshDags({ restore: false }),
  );
  elements.guideTipsButton.addEventListener("click", openGuideTips);
  elements.addDagButton.addEventListener("click", openAddDagModal);
  elements.checkUpdateButton.addEventListener("click", () =>
    checkForUpdate({ manual: true }),
  );
  elements.mobileViewBar.addEventListener("click", (event) => {
    const button = event.target.closest("[data-mobile-view]");
    if (!button) {
      return;
    }
    setMobileView(button.dataset.mobileView);
  });
  elements.reloginButton.addEventListener("click", relogin);
  elements.refreshDagButton.addEventListener("click", () =>
    refreshSelectedDag({ keepSelection: true }),
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
      button.dataset.category || "all",
    );
    renderDagCategories();
    renderDagList();
    saveLastView({ dagCategory: state.dagCategory });
    const visibleDags = getVisibleDags();
    const currentIsVisible = visibleDags.some(
      (dag) => dag.dag_id === state.selectedDag?.dag_id,
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
    state.owner = elements.ownerInput.value.trim();
    refreshDags({ restore: false });
  });
  elements.triggerForm.addEventListener("submit", triggerSelectedDag);
  elements.copyRunConfButton.addEventListener("click", copySelectedRunConf);
  elements.closeRunConfModalButton.addEventListener("click", closeRunConfModal);
  elements.copyDagCodeModalButton.addEventListener("click", copyOpenedDagCode);
  elements.closeDagCodeModalButton.addEventListener("click", closeDagCodeModal);
  elements.copyDagCodeButton.addEventListener("click", copyGeneratedDagCode);
  elements.saveDagFileButton.addEventListener("click", saveGeneratedDagFile);
  elements.regenerateDagCodeButton.addEventListener("click", () =>
    renderAddDagPreview({ force: true }),
  );
  elements.closeAddDagModalButton.addEventListener("click", closeAddDagModal);
  elements.addDagTemplateGroup.addEventListener(
    "change",
    handleAddDagTemplateChange,
  );
  elements.addDagCodePreview.addEventListener("input", handleAddDagCodeInput);
  ADD_DAG_INPUT_IDS.forEach((id) => {
    elements[id]?.addEventListener("input", renderAddDagPreview);
  });
  elements.closeUpdateModalButton.addEventListener("click", closeUpdateModal);
  elements.downloadUpdateButton.addEventListener("click", downloadUpdate);
  elements.closeGuideTipsButton.addEventListener("click", closeGuideTips);
  elements.confirmGuideTipsButton.addEventListener("click", closeGuideTips);
  elements.closeViewportWarningButton.addEventListener(
    "click",
    closeViewportWarning,
  );
  elements.runConfModal.addEventListener("click", (event) => {
    if (event.target === elements.runConfModal) {
      closeRunConfModal();
    }
  });
  elements.dagCodeModal.addEventListener("click", (event) => {
    if (event.target === elements.dagCodeModal) {
      closeDagCodeModal();
    }
  });
  elements.addDagModal.addEventListener("click", (event) => {
    if (event.target === elements.addDagModal) {
      closeAddDagModal();
    }
  });
  elements.guideTipsModal.addEventListener("click", (event) => {
    if (event.target === elements.guideTipsModal) {
      closeGuideTips();
    }
  });
  elements.updateModal.addEventListener("click", (event) => {
    if (event.target === elements.updateModal) {
      closeUpdateModal();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (isLogSearchShortcut(event) && !isAnyModalOpen()) {
      event.preventDefault();
      openLogSearch();
      return;
    }
    if (event.key !== "Escape") {
      return;
    }
    if (!elements.viewportWarningModal.hidden) {
      closeViewportWarning();
      return;
    }
    if (!elements.addDagModal.hidden) {
      closeAddDagModal();
      return;
    }
    if (!elements.dagCodeModal.hidden) {
      closeDagCodeModal();
      return;
    }
    if (!elements.updateModal.hidden) {
      closeUpdateModal();
      return;
    }
    if (!elements.guideTipsModal.hidden) {
      closeGuideTips();
      return;
    }
    if (!elements.runConfModal.hidden) {
      closeRunConfModal();
      return;
    }
    if (state.logSearch.open) {
      closeLogSearch();
    }
  });
  elements.logSearchButton.addEventListener("click", openLogSearch);
  elements.logSearchInput.addEventListener("input", (event) => {
    state.logSearch.query = event.target.value;
    state.logSearch.currentIndex = event.target.value ? 0 : -1;
    renderLog({ scrollToMatch: Boolean(event.target.value) });
  });
  elements.logSearchInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      moveLogSearchMatch(event.shiftKey ? -1 : 1);
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      closeLogSearch();
    }
  });
  elements.prevLogMatchButton.addEventListener("click", () =>
    moveLogSearchMatch(-1),
  );
  elements.nextLogMatchButton.addEventListener("click", () =>
    moveLogSearchMatch(1),
  );
  elements.closeLogSearchButton.addEventListener("click", closeLogSearch);
  elements.refreshLogButton.addEventListener("click", () =>
    loadLog({ reset: true }),
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
  if (api.onUpdateEvent) {
    api.onUpdateEvent(handleUpdateEvent);
  }
  bindLiquidInteractions();
  bindWorkspaceResizers();
  window.addEventListener("resize", () => {
    clampAndApplyWorkspaceLayout({ save: false });
    syncMobileViewForViewport();
    scheduleViewportSizeCheck();
  });
}

function isMobileViewport() {
  return window.matchMedia(MOBILE_VIEW_MEDIA_QUERY).matches;
}

function syncMobileViewForViewport() {
  const mobileNow = isMobileViewport();
  const enteredMobile = mobileNow && !state.isMobileViewport;
  state.isMobileViewport = mobileNow;
  if (enteredMobile) {
    setMobileView("dag", { save: false });
  }
}

function getViewportSize() {
  return {
    width: Math.round(window.innerWidth || document.documentElement.clientWidth),
    height: Math.round(
      window.innerHeight || document.documentElement.clientHeight,
    ),
  };
}

function scheduleViewportSizeCheck() {
  window.clearTimeout(state.viewportResizeTimer);
  state.viewportResizeTimer = window.setTimeout(checkViewportSize, 180);
}

function checkViewportSize({ force = false } = {}) {
  const viewport = getViewportSize();
  const isTooSmall =
    viewport.width < VIEWPORT_WARNING_THRESHOLD.width ||
    viewport.height < VIEWPORT_WARNING_THRESHOLD.height;

  if (!isTooSmall) {
    const wasTooSmall = state.viewportIsTooSmall;
    state.viewportIsTooSmall = false;
    state.viewportWarningDismissed = false;
    if (!elements.viewportWarningModal.hidden) {
      closeViewportWarning({ dismissed: false });
    } else if (wasTooSmall && state.hasInitialized) {
      maybeOpenGuideTips();
    }
    return;
  }

  const crossedThreshold = !state.viewportIsTooSmall;
  state.viewportIsTooSmall = true;
  elements.viewportWarningSize.textContent =
    `当前 ${viewport.width} × ${viewport.height} px · ` +
    `建议至少 ${VIEWPORT_WARNING_THRESHOLD.width} × ${VIEWPORT_WARNING_THRESHOLD.height} px`;

  if (
    (force || crossedThreshold) &&
    !state.viewportWarningDismissed &&
    elements.viewportWarningModal.hidden
  ) {
    openViewportWarning();
  }
}

function openViewportWarning() {
  elements.viewportWarningModal.hidden = false;
  requestAnimationFrame(() => {
    elements.closeViewportWarningButton?.focus();
  });
}

function closeViewportWarning({ dismissed = true } = {}) {
  if (elements.viewportWarningModal.hidden) {
    return;
  }
  elements.viewportWarningModal.hidden = true;
  if (dismissed) {
    state.viewportWarningDismissed = true;
  }
  if (!state.viewportIsTooSmall && state.hasInitialized) {
    maybeOpenGuideTips();
  }
}

function setMobileView(view, { save = true } = {}) {
  const normalized = ["dag", "detail", "log"].includes(view) ? view : "dag";
  state.mobileView = normalized;
  document.body.dataset.mobileView = normalized;
  elements.mobileViewBar
    ?.querySelectorAll("[data-mobile-view]")
    .forEach((button) => {
      button.classList.toggle(
        "active",
        button.dataset.mobileView === normalized,
      );
    });
  if (save) {
    saveLastView({ mobileView: normalized });
  }
}

function bindLiquidInteractions() {
  const interactiveSelector = [
    ".modal",
    ".category-button",
    ".button",
    ".icon-button",
    ".dag-code-button",
    ".guide-card",
    ".guide-shot",
    ".add-dag-template-option",
    ".toast",
  ].join(",");
  let frameId = 0;
  let pointerTarget = null;
  let pointerX = 0;
  let pointerY = 0;

  document.addEventListener(
    "pointermove",
    (event) => {
      if (
        event.target.closest(".dag-list, .run-list, .task-list, .log-viewer")
      ) {
        return;
      }

      pointerTarget = event.target.closest(interactiveSelector);
      if (!pointerTarget) {
        return;
      }
      pointerX = event.clientX;
      pointerY = event.clientY;

      if (frameId) {
        return;
      }
      frameId = requestAnimationFrame(() => {
        frameId = 0;
        if (!pointerTarget?.isConnected) {
          return;
        }
        const rect = pointerTarget.getBoundingClientRect();
        if (!rect.width || !rect.height) {
          return;
        }
        pointerTarget.style.setProperty("--mx", `${pointerX - rect.left}px`);
        pointerTarget.style.setProperty("--my", `${pointerY - rect.top}px`);
      });
    },
    { passive: true },
  );
}

function bindWorkspaceResizers() {
  const bindings = [
    [elements.resizeDagDetail, "dag-detail"],
    [elements.resizeDetailLog, "detail-log"],
  ];

  bindings.forEach(([handle, kind]) => {
    if (!handle) {
      return;
    }
    handle.addEventListener("pointerdown", (event) =>
      startWorkspaceResize(kind, handle, event),
    );
    handle.addEventListener("keydown", (event) =>
      nudgeWorkspaceResize(kind, event),
    );
  });
}

function normalizeWorkspaceLayout(layout) {
  const next = {};
  const dagWidth = Number(layout?.dag);
  const detailWidth = Number(layout?.detail);
  if (Number.isFinite(dagWidth) && dagWidth > 0) {
    next.dag = Math.round(dagWidth);
  }
  if (Number.isFinite(detailWidth) && detailWidth > 0) {
    next.detail = Math.round(detailWidth);
  }
  return next;
}

function clamp(value, min, max) {
  if (max < min) {
    return min;
  }
  return Math.max(min, Math.min(max, value));
}

function applyWorkspaceLayout(layout = state.workspaceLayout) {
  if (!elements.workspace) {
    return;
  }

  const next = normalizeWorkspaceLayout(layout);
  if (next.dag) {
    elements.workspace.style.setProperty(
      "--workspace-dag-width",
      `${next.dag}px`,
    );
  }
  if (next.detail) {
    elements.workspace.style.setProperty(
      "--workspace-detail-width",
      `${next.detail}px`,
    );
  }
  state.workspaceLayout = {
    ...state.workspaceLayout,
    ...next,
  };
}

function workspacePaneWidths() {
  if (
    !elements.workspace ||
    !elements.dagPane ||
    !elements.detailPane ||
    !elements.logPane
  ) {
    return null;
  }

  return {
    dag: elements.dagPane.getBoundingClientRect().width,
    detail: elements.detailPane.getBoundingClientRect().width,
    log: elements.logPane.getBoundingClientRect().width,
  };
}

function workspaceAvailablePaneWidth() {
  if (!elements.workspace) {
    return 0;
  }

  const style = getComputedStyle(elements.workspace);
  const horizontalPadding =
    Number.parseFloat(style.paddingLeft || "0") +
    Number.parseFloat(style.paddingRight || "0");
  const handleWidth =
    (elements.resizeDagDetail?.offsetWidth || 0) +
    (elements.resizeDetailLog?.offsetWidth || 0);
  return elements.workspace.clientWidth - horizontalPadding - handleWidth;
}

function fitWorkspaceLayout(widths) {
  const current = workspacePaneWidths() || {};
  let dag = Number(widths?.dag || current.dag);
  let detail = Number(widths?.detail || current.detail);
  if (!Number.isFinite(dag) || !Number.isFinite(detail)) {
    return normalizeWorkspaceLayout(widths);
  }

  const available = workspaceAvailablePaneWidth();
  const minTotal =
    WORKSPACE_MIN_WIDTHS.dag +
    WORKSPACE_MIN_WIDTHS.detail +
    WORKSPACE_MIN_WIDTHS.log;
  if (available >= minTotal) {
    dag = clamp(
      dag,
      WORKSPACE_MIN_WIDTHS.dag,
      available - WORKSPACE_MIN_WIDTHS.detail - WORKSPACE_MIN_WIDTHS.log,
    );
    detail = clamp(
      detail,
      WORKSPACE_MIN_WIDTHS.detail,
      available - dag - WORKSPACE_MIN_WIDTHS.log,
    );
  }

  return normalizeWorkspaceLayout({ dag, detail });
}

function setWorkspacePaneWidths(widths, { save = false } = {}) {
  const next = fitWorkspaceLayout(widths);
  applyWorkspaceLayout(next);
  if (save) {
    state.workspaceLayout = next;
    saveLastView({ workspaceLayout: next });
  }
}

function clampWorkspacePair(kind, start, delta) {
  if (kind === "dag-detail") {
    const pairTotal = start.dag + start.detail;
    const dag = clamp(
      start.dag + delta,
      WORKSPACE_MIN_WIDTHS.dag,
      pairTotal - WORKSPACE_MIN_WIDTHS.detail,
    );
    return {
      dag,
      detail: pairTotal - dag,
    };
  }

  const pairTotal = start.detail + start.log;
  const detail = clamp(
    start.detail + delta,
    WORKSPACE_MIN_WIDTHS.detail,
    pairTotal - WORKSPACE_MIN_WIDTHS.log,
  );
  return {
    dag: start.dag,
    detail,
  };
}

function startWorkspaceResize(kind, handle, event) {
  if (event.button !== 0) {
    return;
  }
  const start = workspacePaneWidths();
  if (!start) {
    return;
  }

  event.preventDefault();
  state.workspaceResize = {
    kind,
    handle,
    startX: event.clientX,
    start,
  };
  elements.workspace.classList.add("is-resizing");
  document.body.classList.add("is-workspace-resizing");
  handle.classList.add("dragging");
  handle.setPointerCapture?.(event.pointerId);
  window.addEventListener("pointermove", handleWorkspaceResizeMove);
  window.addEventListener("pointerup", finishWorkspaceResize);
  window.addEventListener("pointercancel", finishWorkspaceResize);
}

function handleWorkspaceResizeMove(event) {
  const resize = state.workspaceResize;
  if (!resize) {
    return;
  }
  const delta = event.clientX - resize.startX;
  setWorkspacePaneWidths(clampWorkspacePair(resize.kind, resize.start, delta));
}

function finishWorkspaceResize() {
  const resize = state.workspaceResize;
  if (!resize) {
    return;
  }
  const widths = workspacePaneWidths();
  if (widths) {
    const next = {
      dag: Math.round(widths.dag),
      detail: Math.round(widths.detail),
    };
    state.workspaceLayout = next;
    saveLastView({ workspaceLayout: next });
  }

  resize.handle?.classList.remove("dragging");
  elements.workspace.classList.remove("is-resizing");
  document.body.classList.remove("is-workspace-resizing");
  state.workspaceResize = null;
  window.removeEventListener("pointermove", handleWorkspaceResizeMove);
  window.removeEventListener("pointerup", finishWorkspaceResize);
  window.removeEventListener("pointercancel", finishWorkspaceResize);
}

function nudgeWorkspaceResize(kind, event) {
  if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") {
    return;
  }
  const start = workspacePaneWidths();
  if (!start) {
    return;
  }

  event.preventDefault();
  const direction = event.key === "ArrowRight" ? 1 : -1;
  const step = event.shiftKey ? 48 : 16;
  const next = clampWorkspacePair(kind, start, direction * step);
  setWorkspacePaneWidths(
    {
      dag: Math.round(next.dag),
      detail: Math.round(next.detail),
    },
    { save: true },
  );
}

function clampAndApplyWorkspaceLayout({ save = false } = {}) {
  const current = workspacePaneWidths();
  if (!current) {
    return;
  }

  setWorkspacePaneWidths(current, { save });
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
            label,
          )}</option>`;
        })
        .join("")
    : `<option value="">未配置 Airflow</option>`;
  elements.airflowServiceSelect.value = activeServiceId;
  const activeService = services.find(
    (service) => service.id === activeServiceId,
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
    state.systemUsername = state.config.systemUsername || state.systemUsername;
    state.canToggleDag = Boolean(state.config.canToggleDag);
    state.owner = state.config.owner;
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
    elements.runIdInput.placeholder = `${formatDagRunIdUsername()}_YYYYMMDD_HHMMSS`;
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
  setBusy(true, `正在加载 ${state.owner || "所有"}  的 DAG`);
  elements.dagList.innerHTML = `<div class="empty-state">加载 DAG 中...</div>`;
  saveLastView({ owner: state.owner });

  try {
    await reloadDagConfMapping();
    const result = await api.listDags(state.owner);
    state.dags = result.dags || [];
    ensureDagCategoryExists();
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

async function reloadDagConfMapping() {
  if (!api.reloadDagConfMapping) {
    return;
  }

  try {
    const result = await api.reloadDagConfMapping();
    state.dagConfFieldMapping = result.dagConfFieldMapping || [];
    renderDagConfFields();
  } catch (error) {
    showToast(`DAG conf 配置刷新失败：${error.message}`, "error");
  }
}

function renderDagList() {
  const filtered = getVisibleDags();
  const categoryTotal = state.dags.filter((dag) =>
    matchesDagCategory(dag, state.dagCategory),
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
          dag.owners.join(", ") || "no owner",
        )}</span>`,
        `<span class="chip category">${escapeHtml(category.label)}</span>`,
        `<button
          class="dag-code-button"
          type="button"
          data-dag-code-id="${escapeAttr(dag.dag_id)}"
          title="DAG Code"
          aria-label="查看 ${escapeAttr(dag.dag_id)} 的 DAG Code"
        >DAG Code</button>`,
      ].join("");
      const switchClass = dag.is_paused ? " paused" : " active";
      const switchLabel = dag.is_paused ? "启用 DAG" : "暂停 DAG";
      return `
        <div class="dag-row${activeClass}" role="button" tabindex="0" data-dag-id="${escapeAttr(
          dag.dag_id,
        )}">
          <button
            class="dag-switch${switchClass}"
            type="button"
            data-dag-switch-id="${escapeAttr(dag.dag_id)}"
            role="switch"
            aria-checked="${dag.is_paused ? "false" : "true"}"
            aria-label="${escapeAttr(switchLabel)}"
            title="${escapeAttr(switchLabel)}"
          >
            <span></span>
          </button>
          <span class="dag-row-body">
            <span class="dag-id">${escapeHtml(dag.dag_id)}</span>
            <span class="dag-description">${escapeHtml(
              dag.description || dag.fileloc || "-",
            )}</span>
            <span class="tag-row">${tags}</span>
          </span>
        </div>
      `;
    })
    .join("");

  elements.dagList.querySelectorAll(".dag-row").forEach((row) => {
    const chooseDag = () => {
      const dag = state.dags.find((item) => item.dag_id === row.dataset.dagId);
      if (dag) {
        selectDag(dag);
      }
    };
    row.addEventListener("click", chooseDag);
    row.addEventListener("keydown", (event) => {
      if (event.target.closest("button")) {
        return;
      }
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        chooseDag();
      }
    });
  });

  elements.dagList.querySelectorAll(".dag-code-button").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const dag = state.dags.find(
        (item) => item.dag_id === button.dataset.dagCodeId,
      );
      if (dag) {
        openDagCodeModal(dag);
      }
    });
  });

  elements.dagList.querySelectorAll(".dag-switch").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const dag = state.dags.find(
        (item) => item.dag_id === button.dataset.dagSwitchId,
      );
      if (dag) {
        toggleDagPaused(dag, button);
      }
    });
  });
}

function renderDagCategories() {
  const categories = getDagCategoryFilters();
  elements.dagCategoryBar.innerHTML = categories
    .map((category) => {
      const activeClass = category.key === state.dagCategory ? " active" : "";
      return `
      <button class="category-button${activeClass}" type="button" data-category="${escapeAttr(
        category.key,
      )}">
        <span>${escapeHtml(category.label)}</span>
        <strong>${escapeHtml(category.count)}</strong>
      </button>
    `;
    })
    .join("");
}

function getVisibleDags() {
  return state.dags.filter((dag) => {
    if (!matchesDagCategory(dag, state.dagCategory)) {
      return false;
    }
    const searchText = `${dag.dag_id} ${dag.description || ""}`.toLowerCase();
    return !state.dagSearch || searchText.includes(state.dagSearch);
  });
}

function matchesDagCategory(dag, categoryKey) {
  const normalizedCategory = normalizeDagCategoryKey(categoryKey);
  if (normalizedCategory === "all") {
    return true;
  }
  if (normalizedCategory === "other") {
    return !getDagTagValues(dag).length;
  }
  return getDagTagValues(dag).some(
    (tag) => dagTagCategoryKey(tag) === normalizedCategory,
  );
}

function getDagCategory(dag) {
  const firstTag = getDagTagValues(dag)[0];
  if (!firstTag) {
    return DAG_CATEGORY_OTHER;
  }
  return {
    key: dagTagCategoryKey(firstTag),
    label: firstTag,
  };
}

function getDagCategoryConfig(categoryKey) {
  return (
    getDagCategoryFilters().find((category) => category.key === categoryKey) ||
    DAG_CATEGORY_ALL
  );
}

function normalizeDagCategoryKey(categoryKey) {
  const key = String(categoryKey || DAG_CATEGORY_ALL.key);
  return getDagCategoryFilters().some((category) => category.key === key)
    ? key
    : DAG_CATEGORY_ALL.key;
}

function normalizeDagTag(tag) {
  return String(tag || "")
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, " ");
}

function getDagTagValues(dag) {
  return (dag?.tags || [])
    .map((tag) => {
      if (typeof tag === "string") {
        return tag;
      }
      return (
        tag?.name ||
        tag?.tag_name ||
        tag?.label ||
        tag?.display_name ||
        tag?.value ||
        ""
      );
    })
    .map((tag) => String(tag || "").trim())
    .filter((tag) => tag && !isEmailTag(tag));
}

function isEmailTag(tag) {
  return /(?:^|[\s:<([{,;])[\w.+-]+@[\w.-]+\.[a-z]{2,}(?:$|[\s:>\])},;])/i.test(
    String(tag || ""),
  );
}

function dagTagCategoryKey(tag) {
  const normalized = normalizeDagTag(tag).replace(
    /[^a-z0-9\u4e00-\u9fa5]+/g,
    "-",
  );
  return `tag:${normalized.replace(/^-+|-+$/g, "") || "untagged"}`;
}

function getDagCategoryFilters() {
  const tagCategories = new Map();
  let otherCount = 0;

  for (const dag of state.dags) {
    const tags = getDagTagValues(dag);
    if (!tags.length) {
      otherCount += 1;
      continue;
    }

    const countedKeys = new Set();
    for (const tag of tags) {
      const key = dagTagCategoryKey(tag);
      if (countedKeys.has(key)) {
        continue;
      }
      const existing = tagCategories.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        tagCategories.set(key, {
          key,
          label: tag,
          count: 1,
        });
      }
      countedKeys.add(key);
    }
  }

  const dynamicCategories = [...tagCategories.values()].sort((left, right) => {
    if (right.count !== left.count) {
      return right.count - left.count;
    }
    return left.label.localeCompare(right.label, "zh-Hans-CN");
  });

  return [
    { ...DAG_CATEGORY_ALL, count: state.dags.length },
    ...dynamicCategories,
    { ...DAG_CATEGORY_OTHER, count: otherCount },
  ];
}

function ensureDagCategoryExists() {
  const normalized = normalizeDagCategoryKey(state.dagCategory);
  if (normalized !== state.dagCategory) {
    state.dagCategory = normalized;
    saveLastView({ dagCategory: state.dagCategory });
  }
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
  setMobileView("detail");

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

async function toggleDagPaused(dag, control) {
  if (!state.canToggleDag) {
    const message = `警报：当前系统用户 ${state.systemUsername || "unknown"} 无权操作 DAG 开关`;
    // window.alert(message);
    showToast(message, "error");
    return;
  }

  const dagId = dag.dag_id;
  const nextPaused = !Boolean(dag.is_paused);
  if (control) {
    control.disabled = true;
  }
  try {
    const updatedDag = await api.setDagPaused(dagId, nextPaused);
    state.dags = state.dags.map((dag) =>
      dag.dag_id === dagId ? { ...dag, ...updatedDag } : dag,
    );
    if (state.selectedDag?.dag_id === dagId) {
      state.selectedDag = {
        ...state.selectedDag,
        ...updatedDag,
      };
      renderSelectedDag();
    }
    renderDagList();
    showToast(`${dagId} 已${nextPaused ? "暂停" : "启用"}`);
  } catch (error) {
    showToast(`DAG 开关操作失败：${error.message}`, "error");
  } finally {
    if (control) {
      control.disabled = false;
    }
  }
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
          run.dag_run_id,
        )}">
          <div class="run-row-main">
            <span class="run-id" title="${escapeAttr(
              run.dag_run_id,
            )}">${escapeHtml(run.dag_run_id)}</span>
            <span class="run-meta">${formatTime(
              run.execution_date,
            )} · ${formatTime(run.start_date)} · ${escapeHtml(
              run.run_type || "-",
            )}</span>
            <span class="run-conf" data-conf-run-id="${escapeAttr(
              run.dag_run_id,
            )}" title="${escapeAttr(confTitle)}">${escapeHtml(
              confPreview,
            )}</span>
            <span class="tag-row"><span class="chip ${stateClass(
              run.state,
            )}">${escapeHtml(run.state || "unknown")}</span></span>
          </div>
          <button class="icon-button small run-conf-button" type="button" data-conf-run-id="${escapeAttr(
            run.dag_run_id,
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
        (item) => item.dag_run_id === row.dataset.runId,
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
          (item) => item.dag_run_id === button.dataset.confRunId,
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

async function openDagCodeModal(dag) {
  const dagId = String(dag?.dag_id || "").trim();
  if (!dagId) {
    return;
  }

  state.dagCodeModalDagId = dagId;
  state.dagCodeText = "";
  elements.dagCodeModalTitle.textContent = "DAG Code";
  elements.dagCodeModalMeta.textContent = dagId;
  elements.dagCodeModalText.textContent = "正在加载 DAG Code...";
  elements.copyDagCodeModalButton.disabled = true;
  elements.dagCodeModal.hidden = false;

  try {
    const result = await api.getDagCode(dagId);
    if (state.dagCodeModalDagId !== dagId || elements.dagCodeModal.hidden) {
      return;
    }

    const code = String(result?.code || "");
    state.dagCodeText = code;
    elements.dagCodeModalMeta.textContent = [dagId, result?.fileloc]
      .filter(Boolean)
      .join(" · ");
    elements.dagCodeModalText.textContent = code || "未获取到 DAG Code";
    elements.copyDagCodeModalButton.disabled = !code;
  } catch (error) {
    if (state.dagCodeModalDagId !== dagId || elements.dagCodeModal.hidden) {
      return;
    }

    state.dagCodeText = "";
    elements.copyDagCodeModalButton.disabled = true;
    elements.dagCodeModalText.textContent = `DAG Code 加载失败：${error.message}`;
    showToast(`DAG Code 加载失败：${error.message}`, "error");
  }
}

function closeDagCodeModal() {
  elements.dagCodeModal.hidden = true;
  state.dagCodeModalDagId = "";
  state.dagCodeText = "";
}

async function copyOpenedDagCode() {
  try {
    if (!state.dagCodeText.trim()) {
      throw new Error("暂无 DAG Code 可复制");
    }
    await copyText(state.dagCodeText);
    showToast("DAG Code 已复制");
  } catch (error) {
    showToast(`复制失败：${error.message}`, "error");
  }
}

function openAddDagModal() {
  elements.addDagOutputHint.textContent =
    "生成 DAG Python 文件；WebApp 会下载 .py 文件，发布仍走代码同步流程。";
  updateAddDagTemplateFields();
  renderAddDagPreview({ force: !elements.addDagCodePreview.value.trim() });
  elements.addDagModal.hidden = false;
  requestAnimationFrame(() => {
    elements.newDagIdInput?.focus();
  });
}

function closeAddDagModal() {
  elements.addDagModal.hidden = true;
}

function collectAddDagDraft() {
  const dagId = elements.newDagIdInput.value.trim();
  return {
    template: getSelectedAddDagTemplate(),
    dagId,
    taskId: dagId,
    description: elements.newDagDescriptionInput.value.trim(),
    tags: splitCommaList(elements.newDagTagsInput.value),
    owner: "Shaun",
    email: ["shaun.pan@shopee.com"],
    schedule: "",
    startDate: "",
    retries: 0,
    fileName: elements.newDagFileNameInput.value.trim(),
    bashCommand: "",
    pythonPath: "/home/toc/SSE/shaun",
    pythonImport: elements.newDagPythonImportInput.value.trim(),
    pythonCallable: "",
  };
}

function normalizeAddDagTemplate(value) {
  return ADD_DAG_TEMPLATES[value] ? value : "bash";
}

function getSelectedAddDagTemplate() {
  const selected = elements.addDagTemplateGroup.querySelector(
    'input[name="dagTemplate"]:checked',
  );
  return normalizeAddDagTemplate(selected?.value);
}

function handleAddDagTemplateChange(event) {
  const radio = event.target.closest('input[name="dagTemplate"]');
  if (!radio) {
    return;
  }
  state.addDagTemplate = normalizeAddDagTemplate(radio.value);
  updateAddDagTemplateFields();
  renderAddDagPreview({ force: true });
}

function updateAddDagTemplateFields() {
  const isPythonTemplate = getSelectedAddDagTemplate() === "python";
  elements.newDagPythonImportField.hidden = !isPythonTemplate;

  if (isPythonTemplate) {
    if (!elements.newDagPythonImportInput.value.trim()) {
      elements.newDagPythonImportInput.value = "from listing.lvt_tool import ";
    }
  }
}

function handleAddDagCodeInput() {
  if (state.isRenderingAddDagCode) {
    return;
  }
  state.addDagCodeDirty =
    elements.addDagCodePreview.value !== state.addDagLastGeneratedCode;
  updateAddDagPreviewMeta();
}

function splitCommaList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function pythonString(value) {
  return JSON.stringify(String(value || ""));
}

function pythonList(values) {
  return `[${values.map(pythonString).join(", ")}]`;
}

function pythonSchedule(value) {
  const text = String(value || "").trim();
  if (!text || /^none|null|manual$/i.test(text)) {
    return "None";
  }
  return pythonString(text);
}

function parseDagStartDate(value) {
  const match = String(value || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    const now = new Date();
    return {
      year: now.getFullYear(),
      month: now.getMonth() + 1,
      day: now.getDate(),
    };
  }
  return {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
  };
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

function validateDagIdentifier(value, label) {
  if (!value) {
    throw new Error(`${label} 不能为空`);
  }
  if (!/^[A-Za-z_][A-Za-z0-9_.-]*$/.test(value)) {
    throw new Error(
      `${label} 只能包含字母、数字、下划线、点和短横线，且必须以字母或下划线开头`,
    );
  }
}

function buildDagCode(draft, { strict = false } = {}) {
  return draft.template === "python"
    ? buildPythonDagCode(draft, { strict })
    : buildBashDagCode(draft, { strict });
}

function buildCommonDagValues(draft) {
  const dagId = draft.dagId || "new_dag";
  const taskId = draft.taskId || dagId;
  const retries =
    Number.isFinite(draft.retries) && draft.retries >= 0 ? draft.retries : 0;
  const startDate = parseDagStartDate(draft.startDate);
  const tags = draft.tags.length ? draft.tags : ["Lovito"];
  const emails = draft.email.length ? draft.email : ["shaun.pan@shopee.com"];
  const description = draft.description || `${dagId} task`;

  return {
    dagId,
    taskId,
    retries,
    startDate,
    tags,
    emails,
    description,
  };
}

function buildBashCommandExpression(command) {
  const lines = String(command || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const commandLines = lines.length
    ? lines
    : [
        "/home/toc/SSE/your_project/venv/bin/python",
        "/home/toc/SSE/your_project/main.py",
        "--conf '{{ dag_run.conf | tojson if dag_run else \"{}\" }}'",
      ];
  const stringLines = commandLines.map((line, index) => {
    const value = index === commandLines.length - 1 ? line : `${line} `;
    return `        ${pythonString(value)}`;
  });

  return `(
${stringLines.join("\n")}
    )`;
}

function buildBashDagCode(draft, { strict = false } = {}) {
  const common = buildCommonDagValues(draft);
  if (strict) {
    validateDagIdentifier(draft.dagId, "dag_id");
    validateDagIdentifier(common.taskId, "task_id");
  }

  return `import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": ${pythonString(draft.owner || "Shaun")},
    "depends_on_past": False,
    "start_date": pendulum.datetime(${common.startDate.year}, ${common.startDate.month}, ${common.startDate.day}, tz="Asia/Shanghai"),
    "email": ${pythonList(common.emails)},
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": ${common.retries},
}

dag = DAG(
    dag_id=${pythonString(common.dagId)},
    default_args=default_args,
    description=${pythonString(common.description)},
    schedule_interval=${pythonSchedule(draft.schedule)},
    # schedule_interval=timedelta(minutes=60),
    catchup=False,
    tags=${pythonList(common.tags)},
)

op = BashOperator(
    task_id=${pythonString(common.taskId)},
    bash_command=${buildBashCommandExpression(draft.bashCommand)},
    dag=dag,
)
`;
}

function derivePythonEntrypointFromImport(importText) {
  const text = String(importText || "").trim();
  const fromImportMatch = text.match(
    /^from\s+[\w.]+\s+import\s+([A-Za-z_][\w]*)(?:\s+as\s+([A-Za-z_][\w]*))?/m,
  );
  if (fromImportMatch) {
    const target = fromImportMatch[2] || fromImportMatch[1];
    return `${target}.main`;
  }

  const importMatch = text.match(
    /^import\s+([A-Za-z_][\w.]*)(?:\s+as\s+([A-Za-z_][\w]*))?/m,
  );
  if (importMatch) {
    const target = importMatch[2] || importMatch[1];
    return `${target}.main`;
  }

  return "your_task_module.main";
}

function buildPythonDagCode(draft, { strict = false } = {}) {
  const common = buildCommonDagValues(draft);
  if (strict) {
    validateDagIdentifier(draft.dagId, "dag_id");
    validateDagIdentifier(common.taskId, "task_id");
  }

  const pythonPath = draft.pythonPath || "/home/toc/SSE/shaun";
  const pythonImport =
    draft.pythonImport ||
    "from your_package.your_module import your_task_module";
  const pythonCallable =
    draft.pythonCallable || derivePythonEntrypointFromImport(pythonImport);

  return `from datetime import timedelta
import inspect
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os

sys.path.append(${pythonString(pythonPath)})
os.chdir(${pythonString(pythonPath)})
${pythonImport}

default_args = {
    "owner": ${pythonString(draft.owner || "Shaun")},
    "depends_on_past": False,
    "start_date": pendulum.datetime(${common.startDate.year}, ${common.startDate.month}, ${common.startDate.day}, tz="Asia/Shanghai"),
    "email": ${pythonList(common.emails)},
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": ${common.retries},
}

dag = DAG(
    dag_id=${pythonString(common.dagId)},
    default_args=default_args,
    description=${pythonString(common.description)},
    schedule_interval=${pythonSchedule(draft.schedule)},
    # schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=2,
    tags=${pythonList(common.tags)},
)

def call_entrypoint(func, task_config):
    try:
        params = list(inspect.signature(func).parameters.values())
    except (TypeError, ValueError):
        return func(task_config)

    positional = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.VAR_POSITIONAL,
    }
    keyword_only_names = {"task_config", "conf", "dag_run_conf", "config", "airflow_conf", "task_conf"}

    if any(param.kind in positional and param.name in keyword_only_names for param in params):
        return func(task_config)
    # for param in params:
    #     if param.kind == inspect.Parameter.KEYWORD_ONLY and param.name in keyword_only_names:
    #         return func(**{param.name: task_config})
    # if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params):
    #     return func(task_config=task_config)
    return func()

def receive_param(**context):
    task_config = context["dag_run"].conf if context.get("dag_run") else {}
    return call_entrypoint(${pythonCallable}, task_config)

operator = PythonOperator(
    task_id=${pythonString(common.taskId)},
    python_callable=receive_param,
    priority_weight=100,
    # execution_timeout=timedelta(minutes=90),
    dag=dag,
)
`;
}

function updateAddDagPreviewMeta(fileName) {
  const draft = collectAddDagDraft();
  const normalizedFileName =
    fileName || normalizeDagFileName(draft.fileName, draft.dagId);
  const templateLabel =
    ADD_DAG_TEMPLATES[draft.template] || ADD_DAG_TEMPLATES.bash;
  const editStatus = state.addDagCodeDirty ? "已手动修改" : "自动生成";
  elements.addDagPreviewMeta.textContent = `${normalizedFileName} · ${templateLabel} · ${editStatus}`;
}

function setAddDagCode(code) {
  state.isRenderingAddDagCode = true;
  elements.addDagCodePreview.value = code;
  elements.addDagCodePreview.scrollTop = 0;
  elements.addDagCodePreview.scrollLeft = 0;
  state.addDagLastGeneratedCode = code;
  state.addDagCodeDirty = false;
  state.isRenderingAddDagCode = false;
}

function renderAddDagPreview(options = {}) {
  const force = Boolean(options.force);
  const draft = collectAddDagDraft();
  const fileName = normalizeDagFileName(draft.fileName, draft.dagId);
  elements.newDagFileNameInput.placeholder = `默认 ${fileName}`;
  const generatedCode = buildDagCode(draft);
  const canReplaceCode =
    force ||
    !state.addDagCodeDirty ||
    elements.addDagCodePreview.value === state.addDagLastGeneratedCode;

  if (canReplaceCode) {
    setAddDagCode(generatedCode);
  } else {
    state.addDagLastGeneratedCode = generatedCode;
  }
  updateAddDagPreviewMeta(fileName);
}

function getEditedDagCode() {
  const code = elements.addDagCodePreview.value.trimEnd();
  if (!code.trim()) {
    throw new Error("DAG 代码为空");
  }
  return code;
}

function validateAddDagBasics(draft) {
  validateDagIdentifier(draft.dagId, "dag_id");
  validateDagIdentifier(draft.taskId, "task_id");
}

async function copyGeneratedDagCode() {
  try {
    const code = getEditedDagCode();
    await copyText(code);
    showToast("DAG 代码已复制");
  } catch (error) {
    showToast(`复制失败：${error.message}`, "error");
  }
}

async function saveGeneratedDagFile() {
  try {
    const draft = collectAddDagDraft();
    validateAddDagBasics(draft);
    const code = getEditedDagCode();
    const result = await api.saveDagFile({
      dagId: draft.dagId,
      fileName: draft.fileName,
      code,
      overwrite: false,
    });
    showToast(
      result.downloaded
        ? `DAG 文件已下载：${result.filePath}`
        : `DAG 文件已保存：${result.filePath}`,
    );
  } catch (error) {
    showToast(`保存失败：${error.message}`, "error");
  }
}

function maybeCheckUpdateOnStart() {
  const updateConfig = state.config?.update || {};
  if (!updateConfig.checkOnStart) {
    return;
  }

  const intervalMs =
    Number(updateConfig.checkIntervalHours || 24) * 60 * 60 * 1000;
  const lastCheckedAt = Date.parse(state.lastView.lastUpdateCheckAt || "");
  if (lastCheckedAt && Date.now() - lastCheckedAt < intervalMs) {
    return;
  }

  window.setTimeout(() => {
    checkForUpdate({ manual: false });
  }, 1200);
}

async function checkForUpdate({ manual = false } = {}) {
  if (state.isCheckingUpdate) {
    return;
  }

  state.isCheckingUpdate = true;
  elements.checkUpdateButton.disabled = true;
  const previousText = elements.connectionText.textContent;
  if (manual) {
    elements.connectionText.textContent = "正在检查更新";
  }

  try {
    const result = await api.checkUpdate();
    state.lastView = {
      ...state.lastView,
      lastUpdateCheckAt: new Date().toISOString(),
    };
    saveLastView({ lastUpdateCheckAt: state.lastView.lastUpdateCheckAt });

    if (!result.enabled) {
      if (manual) {
        showToast(result.message || "未配置更新地址");
      }
      return;
    }

    if (result.hasUpdate) {
      state.updateInfo = result;
      state.updateStatus = result.downloaded ? "downloaded" : "available";
      openUpdateModal(result);
      return;
    }

    if (manual) {
      showToast(
        `当前已是最新版本：${result.currentVersion || state.config?.appVersion || "-"}`,
      );
    }
  } catch (error) {
    if (manual) {
      showToast(`检查更新失败：${error.message}`, "error");
    }
  } finally {
    state.isCheckingUpdate = false;
    elements.checkUpdateButton.disabled = false;
    if (manual) {
      elements.connectionText.textContent = previousText;
    }
  }
}

function handleUpdateEvent(event) {
  if (!event?.type) {
    return;
  }

  if (event.type === "checking") {
    state.updateStatus = "checking";
    return;
  }
  if (event.type === "available") {
    state.updateInfo = {
      ...state.updateInfo,
      ...event,
    };
    state.updateStatus = event.downloaded ? "downloaded" : "available";
    openUpdateModal(state.updateInfo);
    return;
  }
  if (event.type === "not-available") {
    state.updateStatus = "idle";
    return;
  }
  if (event.type === "downloading") {
    state.updateStatus = "downloading";
    state.updateProgress = Number(event.percent || 0);
    renderUpdateProgress(event);
    return;
  }
  if (event.type === "downloaded") {
    state.updateInfo = {
      ...state.updateInfo,
      ...event,
      downloaded: true,
    };
    state.updateStatus = "downloaded";
    state.updateProgress = 100;
    renderUpdateProgress({ percent: 100 });
    renderUpdateAction();
    showToast("更新包已下载完成，重启后即可安装");
    openUpdateModal(state.updateInfo);
    return;
  }
  if (event.type === "error") {
    state.updateStatus = "available";
    renderUpdateAction();
    showToast(`更新失败：${event.message || "未知错误"}`, "error");
  }
}

function openUpdateModal(info) {
  const currentVersion = info.currentVersion || state.config?.appVersion || "-";
  const latestVersion = info.latestVersion || "-";
  elements.updateModalTitle.textContent = info.force
    ? "发现重要更新"
    : "发现新版本";
  elements.updateModalMeta.textContent = [
    info.releaseDate ? `发布日期 ${info.releaseDate}` : "",
    info.platform ? `平台 ${info.platform}` : "",
  ]
    .filter(Boolean)
    .join(" · ");
  elements.updateVersionText.innerHTML = `
    <div>当前版本：${escapeHtml(currentVersion)}</div>
    <div>最新版本：${escapeHtml(latestVersion)}</div>
  `;
  elements.updateNotes.innerHTML = (info.notes || []).length
    ? info.notes
        .map((note) => `<div class="update-note">${escapeHtml(note)}</div>`)
        .join("")
    : `<div class="update-note">这个版本包含新的功能和问题修复。</div>`;
  renderUpdateProgress();
  renderUpdateAction();
  elements.updateModal.hidden = false;
  requestAnimationFrame(() => {
    elements.downloadUpdateButton?.focus();
  });
}

function closeUpdateModal() {
  elements.updateModal.hidden = true;
}

function renderUpdateProgress(progress = {}) {
  const visible =
    state.updateStatus === "downloading" || state.updateStatus === "downloaded";
  elements.updateProgress.hidden = !visible;
  const percent = clamp(
    Number(progress.percent ?? state.updateProgress ?? 0),
    0,
    100,
  );
  state.updateProgress = percent;
  elements.updateProgressBar.style.width = `${percent.toFixed(1)}%`;
  elements.updateProgressText.textContent =
    state.updateStatus === "downloaded"
      ? "下载完成，准备重启安装"
      : `正在下载更新 ${percent.toFixed(1)}%`;
}

function renderUpdateAction() {
  if (state.updateStatus === "downloaded") {
    elements.downloadUpdateButton.disabled = false;
    elements.downloadUpdateButton.textContent = "重启安装";
    elements.downloadUpdateButton.title = "重启应用并安装更新";
    return;
  }
  if (state.updateStatus === "downloading") {
    elements.downloadUpdateButton.disabled = true;
    elements.downloadUpdateButton.textContent = "下载中";
    elements.downloadUpdateButton.title = "更新包正在下载";
    return;
  }
  elements.downloadUpdateButton.disabled = false;
  elements.downloadUpdateButton.textContent = "下载更新";
  elements.downloadUpdateButton.title = "下载更新包";
}

async function downloadUpdate() {
  try {
    if (state.updateStatus === "downloaded") {
      await api.installUpdate();
      return;
    }
    state.updateStatus = "downloading";
    renderUpdateProgress({ percent: 0 });
    renderUpdateAction();
    await api.downloadUpdate();
    showToast("开始下载更新包");
  } catch (error) {
    state.updateStatus = "available";
    renderUpdateAction();
    showToast(`更新下载失败：${error.message}`, "error");
  }
}

function maybeOpenGuideTips() {
  if (
    !state.lastView.guideTipsSeen &&
    !state.viewportIsTooSmall &&
    elements.viewportWarningModal.hidden
  ) {
    openGuideTips();
  }
}

function openGuideTips() {
  elements.guideTipsModal.hidden = false;
  requestAnimationFrame(() => {
    elements.confirmGuideTipsButton?.focus();
  });
}

function closeGuideTips() {
  if (elements.guideTipsModal.hidden) {
    return;
  }
  elements.guideTipsModal.hidden = true;
  if (!state.lastView.guideTipsSeen) {
    state.lastView = {
      ...state.lastView,
      guideTipsSeen: true,
    };
    saveLastView({ guideTipsSeen: true });
  }
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
  { preferredTaskId = "", preferredTryNumber = "", restored = false } = {},
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
      (task) => task.task_id === preferredTaskId,
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
    state.selectedRun.dag_run_id,
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
          tryNumber,
        )}">
          <span class="task-row-grid">
            <span>
              <span class="task-id">try ${escapeHtml(tryNumber)}</span>
              <span class="task-meta">${escapeHtml(
                metaParts.join(" · ") || "尚无时间记录",
              )}</span>
            </span>
            <span class="chip ${stateClass(taskTry.state)}">${escapeHtml(
              taskTry.state || "none",
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
          Number(item.try_number || 1) === Number(row.dataset.tryNumber || 1),
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
      state.selectedTask.map_index,
    );
    state.taskTries = result.task_tries || [];
  } catch (error) {
    state.taskTries = buildFallbackTaskTries(state.selectedTask);
    showToast(
      `Task Tries 加载失败，已用 try_number 兜底：${error.message}`,
      "error",
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
  { resetLog = true, restored = false, preferredTryNumber = "" } = {},
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
      1,
  );
  renderTasks();
  await refreshTaskTries();
  const nextTry =
    state.taskTries.find(
      (taskTry) => Number(taskTry.try_number || 1) === desiredTryNumber,
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
      Number(state.selectedTryNumber || task.try_number || 1),
    ),
    mapIndex: Number.isFinite(Number(task.map_index))
      ? Number(task.map_index)
      : -1,
  });

  if (state.selectedRun) {
    await loadLog({ reset: true });
    setMobileView("log");
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
  setMobileView("log");
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
    const runId = elements.runIdInput.value.trim() || buildDefaultDagRunId();
    const run = await api.triggerDag(state.selectedDag.dag_id, conf, runId);
    elements.runIdInput.value = "";
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
    if (isDagTriggerLimitError(error)) {
      const message = formatDagTriggerLimitMessage(error);
      window.alert(`警报：${message}`);
      showToast(message, "error");
    } else {
      showToast(`Trigger 失败：${error.message}`, "error");
    }
  } finally {
    state.isTriggering = false;
    elements.triggerButton.disabled = false;
  }
}

function isDagTriggerLimitError(error) {
  return String(error?.message || error || "").includes("DAG_TRIGGER_LIMIT:");
}

function formatDagTriggerLimitMessage(error) {
  const message = String(error?.message || error || "");
  const marker = "DAG_TRIGGER_LIMIT:";
  const markerIndex = message.indexOf(marker);
  return markerIndex >= 0
    ? message.slice(markerIndex + marker.length).trim()
    : message;
}

function formatDagRunIdUsername() {
  return (
    String(state.systemUsername || "unknown")
      .trim()
      .replace(/[^A-Za-z0-9_.-]+/g, "_") || "unknown"
  );
}

function formatDagRunIdTime(date = new Date()) {
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
  return `${formatDagRunIdUsername()}_${formatDagRunIdTime()}`;
}

function getConfFieldConfig(dagId) {
  const mapping = state.dagConfFieldMapping.find(
    (item) => item.dag_name === dagId,
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
          `,
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
    </span>
    <span class="field-tooltip" role="tooltip">${escapeHtml(field.tip)}</span>
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
    `确认终止当前 task 吗？\n\nDAG: ${state.selectedDag.dag_id}\nRun: ${runId}\nTask: ${taskId}\n\n此操作会将 task instance 标记为 failed。`,
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
        run.dag_run_id === runId ? { ...run, ...state.selectedRun } : run,
      );
    }

    await refreshTaskInstances();
    const updatedTask = state.taskInstances.find(
      (task) => task.task_id === taskId,
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
      run.dag_run_id === runId ? { ...run, ...state.selectedRun } : run,
    );

    const activeTask = pickCurrentTask(state.taskInstances);
    const selectedFreshTask = state.taskInstances.find(
      (task) => task.task_id === state.selectedTask?.task_id,
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
      Number(state.selectedTryNumber || state.selectedTask.try_number || 1),
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
        String(task.state || "none").toLowerCase(),
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

function renderLog({ keepScroll = false, scrollToMatch = false } = {}) {
  const previousScrollTop = elements.logViewer.scrollTop;
  const previousScrollLeft = elements.logViewer.scrollLeft;
  renderLogHeader();
  rebuildLogSearchMatches(state.logText || "等待 task log...");
  elements.logViewer.innerHTML = renderLogLines(
    state.logText || "等待 task log...",
  );
  renderLogSearchControls();
  const hasActiveLogSearch = Boolean(getLogSearchQuery());
  if (scrollToMatch) {
    requestAnimationFrame(scrollActiveLogMatchIntoView);
  } else if (keepScroll || hasActiveLogSearch) {
    requestAnimationFrame(() => {
      elements.logViewer.scrollTop = previousScrollTop;
      elements.logViewer.scrollLeft = previousScrollLeft;
    });
  } else if (state.autoTail) {
    requestAnimationFrame(() => {
      elements.logViewer.scrollTop = elements.logViewer.scrollHeight;
    });
  }
}

function renderLogLines(rawText) {
  let matchIndex = 0;
  const query = getLogSearchQuery();
  return splitLogLines(rawText)
    .map((line, lineIndex) => {
      const level = detectLogLevel(line);
      const emptyClass = line.trim() ? "" : " log-line-empty";
      const content = renderLogLineContent(line, query, lineIndex, () => {
        const currentMatchIndex = matchIndex;
        matchIndex += 1;
        return currentMatchIndex;
      });
      return `<div class="log-line log-line-${level}${emptyClass}">${content}</div>`;
    })
    .join("");
}

function renderLogLineContent(line, query, lineIndex, getNextMatchIndex) {
  const source = String(line || " ");
  if (!query) {
    return escapeHtml(source);
  }

  const lowerSource = source.toLowerCase();
  const lowerQuery = query.toLowerCase();
  let cursor = 0;
  let matchAt = lowerSource.indexOf(lowerQuery, cursor);
  if (matchAt === -1) {
    return escapeHtml(source);
  }

  const parts = [];
  while (matchAt !== -1) {
    const matchIndex = getNextMatchIndex();
    const matchEnd = matchAt + query.length;
    const activeClass =
      matchIndex === state.logSearch.currentIndex ? " log-match-active" : "";
    parts.push(escapeHtml(source.slice(cursor, matchAt)));
    parts.push(
      `<mark class="log-match${activeClass}" data-log-match-index="${matchIndex}" data-log-line-index="${lineIndex}">${escapeHtml(
        source.slice(matchAt, matchEnd),
      )}</mark>`,
    );
    cursor = matchEnd;
    matchAt = lowerSource.indexOf(lowerQuery, cursor);
  }
  parts.push(escapeHtml(source.slice(cursor)));
  return parts.join("");
}

function getLogSearchQuery() {
  return String(state.logSearch.query || "");
}

function rebuildLogSearchMatches(rawText) {
  const query = getLogSearchQuery();
  const matches = [];
  if (query) {
    const lowerQuery = query.toLowerCase();
    splitLogLines(rawText).forEach((line, lineIndex) => {
      const source = String(line || " ");
      const lowerSource = source.toLowerCase();
      let matchAt = lowerSource.indexOf(lowerQuery);
      while (matchAt !== -1) {
        matches.push({ lineIndex, start: matchAt });
        matchAt = lowerSource.indexOf(lowerQuery, matchAt + query.length);
      }
    });
  }

  state.logSearch.matches = matches;
  if (!query || !matches.length) {
    state.logSearch.currentIndex = -1;
    return;
  }
  if (state.logSearch.currentIndex < 0) {
    state.logSearch.currentIndex = 0;
  } else if (state.logSearch.currentIndex >= matches.length) {
    state.logSearch.currentIndex = matches.length - 1;
  }
}

function renderLogSearchControls() {
  elements.logSearchBar.hidden = !state.logSearch.open;
  if (elements.logSearchInput.value !== state.logSearch.query) {
    elements.logSearchInput.value = state.logSearch.query;
  }

  const matchCount = state.logSearch.matches.length;
  const hasQuery = Boolean(getLogSearchQuery());
  elements.logSearchCount.textContent =
    hasQuery && matchCount
      ? `${state.logSearch.currentIndex + 1} / ${matchCount}`
      : "0 / 0";
  elements.logSearchCount.classList.toggle(
    "is-empty",
    hasQuery && matchCount === 0,
  );
  elements.prevLogMatchButton.disabled = !matchCount;
  elements.nextLogMatchButton.disabled = !matchCount;
}

function openLogSearch() {
  state.logSearch.open = true;
  renderLog({ keepScroll: true });
  requestAnimationFrame(() => {
    elements.logSearchInput.focus();
    elements.logSearchInput.select();
  });
}

function closeLogSearch() {
  const hadQuery = Boolean(state.logSearch.query);
  state.logSearch.open = false;
  state.logSearch.query = "";
  state.logSearch.currentIndex = -1;
  state.logSearch.matches = [];
  if (hadQuery) {
    renderLog({ keepScroll: true });
  } else {
    renderLogSearchControls();
  }
}

function moveLogSearchMatch(direction) {
  if (!state.logSearch.open) {
    openLogSearch();
    return;
  }
  if (!getLogSearchQuery()) {
    elements.logSearchInput.focus();
    return;
  }

  const matchCount = state.logSearch.matches.length;
  if (!matchCount) {
    renderLogSearchControls();
    return;
  }

  const currentIndex =
    state.logSearch.currentIndex < 0 ? 0 : state.logSearch.currentIndex;
  state.logSearch.currentIndex =
    (currentIndex + direction + matchCount) % matchCount;
  renderLog({ scrollToMatch: true });
}

function scrollActiveLogMatchIntoView() {
  const activeMatch = elements.logViewer.querySelector(".log-match-active");
  if (!activeMatch) {
    return;
  }
  activeMatch.scrollIntoView({
    block: "center",
    inline: "center",
  });
}

function isLogSearchShortcut(event) {
  return (
    (event.ctrlKey || event.metaKey) &&
    !event.shiftKey &&
    !event.altKey &&
    String(event.key || "").toLowerCase() === "f"
  );
}

function isAnyModalOpen() {
  return [
    elements.runConfModal,
    elements.dagCodeModal,
    elements.addDagModal,
    elements.updateModal,
    elements.guideTipsModal,
    elements.viewportWarningModal,
  ].some((modal) => modal && !modal.hidden);
}

function getSelectedTry() {
  return state.taskTries.find(
    (taskTry) =>
      Number(taskTry.try_number || 1) === Number(state.selectedTryNumber || 0),
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
    /\b(critical|fatal|error|warning|warn|info|debug)\b/,
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
  // elements.logContextText.textContent = `${state.selectedDag.dag_id} / ${runText} / ${state.selectedTask.task_id}`;
  elements.logContextText.textContent = `${runText}`;
  const chips = [];
  // if (state.selectedRun) {
  //   chips.push(
  //     `<span class="chip ${stateClass(
  //       state.selectedRun.state,
  //     )}">run ${escapeHtml(state.selectedRun.state)}</span>`,
  //   );
  // }
  if (state.selectedTask.state) {
    chips.push(
      `<span class="chip ${stateClass(
        state.selectedTask.state,
      )}">task ${escapeHtml(state.selectedTask.state)}</span>`,
    );
  }
  const selectedTry = getSelectedTry();
  const selectedTryState = selectedTry?.state || state.selectedTask.state || "";
  const tryClass = selectedTryState ? stateClass(selectedTryState) : "";
  chips.push(
    `<span class="chip ${tryClass}">try ${Math.max(
      1,
      Number(state.selectedTryNumber || state.selectedTask.try_number || 1),
    )}</span>`,
  );
  // if (state.logMeta.source) {
  //   chips.push(
  //     `<span class="chip">log ${escapeHtml(state.logMeta.source)}</span>`,
  //   );
  // }
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
          Number(state.selectedTryNumber || state.selectedTask.try_number || 1),
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
    error.message || String(error),
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
