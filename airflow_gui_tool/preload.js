const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("airflowApp", {
  getInit: () => ipcRenderer.invoke("app:get-init"),
  reloadDagConfMapping: () => ipcRenderer.invoke("app:reload-dag-conf-mapping"),
  switchAirflowService: (serviceId) => ipcRenderer.invoke("airflow:switch-service", serviceId),
  relogin: () => ipcRenderer.invoke("airflow:relogin"),
  listDags: (owner) => ipcRenderer.invoke("airflow:list-dags", owner),
  setDagPaused: (dagId, isPaused) => ipcRenderer.invoke("airflow:set-dag-paused", dagId, isPaused),
  listTasks: (dagId) => ipcRenderer.invoke("airflow:list-tasks", dagId),
  listDagRuns: (dagId, limit) => ipcRenderer.invoke("airflow:list-dag-runs", dagId, limit),
  getDagRun: (dagId, dagRunId) => ipcRenderer.invoke("airflow:get-dag-run", dagId, dagRunId),
  listTaskInstances: (dagId, dagRunId) =>
    ipcRenderer.invoke("airflow:list-task-instances", dagId, dagRunId),
  listTaskTries: (dagId, dagRunId, taskId, mapIndex) =>
    ipcRenderer.invoke("airflow:list-task-tries", dagId, dagRunId, taskId, mapIndex),
  triggerDag: (dagId, conf, runId) => ipcRenderer.invoke("airflow:trigger-dag", dagId, conf, runId),
  terminateTask: (params) => ipcRenderer.invoke("airflow:terminate-task", params),
  getTaskLog: (params) => ipcRenderer.invoke("airflow:get-task-log", params),
  saveState: (patch) => ipcRenderer.invoke("state:save", patch),
  clearState: () => ipcRenderer.invoke("state:clear"),
  notify: (message) => ipcRenderer.invoke("app:notify", message),
});
