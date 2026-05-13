# Airflow GUI Tool

精简版 Airflow 桌面工具，基于 `utils/airflow_tools.py` 和现有 Airflow MCP Server 的登录/API 调用方式实现。

## 功能

- 查看 owner 为 `Shaun` 的 DAG 列表。
- 查看每个 DAG 的 task 列表、最近 DAG Run、task instance 状态。
- 手动 trigger DAG，支持填写 `dag_run_id` 和 JSON 格式 `dag_run conf`。
- 触发后自动跟随当前运行中的 task，并持续拉取日志。
- 关闭后重新打开，会自动恢复到上一次查看的 DAG/task log 界面。

## 运行

```bash
cd /Users/shaun.pan/shaun/airflow_gui_tool
npm install
npm start
```

如需修改或追加 Airflow 服务，把 `.env.example` 复制为 `.env` 后调整 `AIRFLOW_LIST`：

```bash
cp .env.example .env
```

```env
AIRFLOW_LIST=[{"id":"main","alias":"Shaun Airflow","name":"34.142.225.103","baseUrl":"http://34.142.225.103:8080","username":"shaun","password":"shaun2024","owner":"Shaun"}]
AIRFLOW_ACTIVE_SERVICE_ID=main
```

`AIRFLOW_LIST` 是 JSON 数组，后续有多个 Airflow 服务时继续追加对象即可。每个服务的 `id` 必须唯一；`alias` 用于顶部下拉框展示；每个服务会按 `baseUrl + username` 单独缓存 cookie。
