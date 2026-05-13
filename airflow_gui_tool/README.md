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
cd /Users/shaun.pan/shaun/projects_sync_to_github/airflow_gui_tool
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

如需为不同 DAG 定制 trigger conf 表单，把 `.dag-conf.env.example` 复制为 `.dag-conf.env` 后调整 `DAG_CONF_FIELD_MAPPING`：

```bash
cp .dag-conf.env.example .dag-conf.env
```

```env
DAG_CONF_FIELD_MAPPING=[
  {
    "dag_name": "ads_credit_moniter",
    "keys": ["location"],
    "alias": ["站点"],
    "tips": ["对应 conf.location，可填写单个站点或脚本约定的站点值"]
  }
]
```

`keys` 是提交到 Airflow 的 conf 字段名，`alias` 是输入框 placeholder 里的展示名，`tips` 是输入框右侧 `i` 图标的悬浮说明。没有出现在 mapping 里的 DAG 会继续使用原始 JSON 输入框。

也可以把 `.dag-conf.env` 放到公网，例如 GitHub raw，然后在 `.env` 里配置：

```env
DAG_CONF_ENV_URL=https://raw.githubusercontent.com/XXXShaunPan/ScriptRepo/refs/heads/quick_matching_tool/airflow_gui_tool/.dag-conf.env
```

启动时会优先读取远程 env；远程读取失败时，会自动回退到本地 `.dag-conf.env`。读取远程 env 时会自动追加 cache-busting query，并带上 no-cache 请求头，尽量避开 GitHub raw 的 CDN 延迟。
