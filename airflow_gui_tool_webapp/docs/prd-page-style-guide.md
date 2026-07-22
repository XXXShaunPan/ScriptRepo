# Lovito 任务中心页面样式 PRD

## 1. 背景

Lovito 任务中心是一个面向内部运营和开发同学使用的 Airflow 精简操作台。用户需要在一个桌面窗口内完成 DAG 筛选、触发、运行记录查看、Task Try 查看、日志追踪、任务终止、服务切换、帮助说明等高频操作。

本 PRD 面向外部应用或页面生成工具，用于根据文档生成与当前产品一致的页面样式。重点是页面信息架构、视觉风格、组件样式、交互状态和验收标准，不包含具体 Airflow API 实现。

## 2. 页面目标

1. 提供一个比 Airflow Web 更轻量、更聚焦的任务中心界面。
2. 页面第一屏直接进入可操作工作台，不做营销页或说明页。
3. 保持专业、清晰、可扫描，适合长时间查看 DAG、Runs 和 Logs。
4. 使用 macOS / iOS 风格的轻量“液态玻璃”视觉，但不能牺牲列表滚动性能。
5. 支持外部生成应用复刻主要页面风格、布局、组件状态和动效。

## 3. 目标用户

- 内部运营同学：筛选 DAG、填写参数、触发任务、查看结果。
- 开发或数据同学：查看 Task Try、排查日志、终止异常任务。
- 管理者或维护者：切换 Airflow 服务、刷新状态、检查版本、查看帮助。

## 4. 页面信息架构

页面采用桌面应用工作台结构，从上到下分为三层：

1. 顶部品牌与操作区
   - 左侧：品牌标识 `L`、产品名 `Lovito 任务中心`、服务下拉选择。
   - 右侧：Owner 输入框、重连、刷新、添加 DAG、更新、帮助按钮。

2. DAG 分类导航区
   - 横向按钮组。
   - 分类根据 DAG tag 自动生成。
   - 必须包含 `全部` 和 `其他`。
   - 每个分类按钮显示分类名称和数量徽标。

3. 三栏工作区
   - 左栏：DAG 列表。
   - 中栏：DAG 详情、Trigger 表单、Recent Runs、Task Tries。
   - 右栏：Task Log。
   - 左栏/中栏/右栏之间有可拖拽竖向分隔线，用于调整宽度。

## 5. 整体视觉风格

### 5.1 风格关键词

- 液态玻璃
- 轻量桌面工具
- 高信息密度
- 蓝白主色
- 操作台而非营销页
- 柔和阴影
- 克制动效
- 长列表性能优先

### 5.2 色彩规范

主背景使用浅蓝白渐变，不使用大面积纯色块。

推荐色值：

| Token | 用途 | 色值 |
| --- | --- | --- |
| `--bg` | 页面底色 | `#edf4fb` |
| `--text` | 正文 | `#17202c` |
| `--ink` | 标题/强文本 | `#101828` |
| `--muted` | 次级文本 | `#667085` |
| `--blue` | 主品牌蓝 | `#2166e8` |
| `--green` | 成功/运行中 | `#16845b` |
| `--orange` | 暂停/警告 | `#b76511` |
| `--red` | 失败/危险操作 | `#c63333` |
| `--log-bg` | 日志背景 | `#0f1720` |
| `--log-info` | INFO 日志 | `#9ecbff` |
| `--log-warning` | WARNING 日志 | `#f2cc60` |
| `--log-error` | ERROR 日志 | `#ff7b72` |

背景建议：

```css
background:
  linear-gradient(135deg, rgba(111, 181, 255, 0.22) 0%, transparent 34%),
  linear-gradient(225deg, rgba(60, 184, 160, 0.16) 0%, transparent 38%),
  linear-gradient(315deg, rgba(255, 181, 102, 0.13) 0%, transparent 40%),
  linear-gradient(180deg, #f8fbff 0%, #eaf2f9 48%, #f5f9fc 100%);
```

### 5.3 字体

- 首选：`-apple-system`, `BlinkMacSystemFont`, `Segoe UI`, `sans-serif`。
- 日志和 JSON 使用等宽字体：`SFMono-Regular`, `Consolas`, `Liberation Mono`, `monospace`。
- 不使用负字距。
- 不按视口宽度动态缩放字体。

推荐字号：

| 场景 | 字号 | 字重 |
| --- | --- | --- |
| 顶部 H1 | 28px | 820 |
| 面板标题 | 15-16px | 680-780 |
| DAG 名称 | 16-18px | 760-820 |
| 正文/输入框 | 13-14px | 500-650 |
| 辅助文本 | 11-12px | 560-650 |
| 日志 | 12-13px | 500 |

### 5.4 圆角与阴影

- 面板圆角：8px。
- 按钮/输入框圆角：7-8px。
- 徽标圆角：999px。
- 页面主阴影：`0 18px 46px rgba(31, 48, 76, 0.13)`。
- Hover 阴影只用于按钮、弹窗、帮助卡片等低频元素。
- 长列表行不要使用复杂大阴影，避免滚动卡顿。

## 6. 布局规格

### 6.1 画布尺寸

- 默认作为桌面应用页面设计。
- 最小宽度：1040px。
- 最小高度：720px。
- 推荐设计基准：1600px × 900px 或更宽。

### 6.2 App Shell

页面高度占满视口。

结构：

```text
App Shell
├── Topbar: 92px
├── Category Bar: auto
└── Workspace: 1fr
```

### 6.3 工作区三栏

推荐初始宽度：

| 区域 | 初始宽度 | 最小宽度 |
| --- | --- | --- |
| DAG 列表 | 300-360px | 240px |
| 详情区 | 520-680px | 320px |
| 日志区 | 剩余宽度 | 380px |

三栏之间加入可拖拽竖线，视觉上需要明显但克制：

- 宽度：10-12px。
- 中间有 2px 宽竖线。
- Hover 时竖线变为品牌蓝。
- Dragging 时显示蓝色发光或更深蓝。

## 7. 核心模块规格

### 7.1 顶部品牌区

左侧内容：

- 方形品牌标识：`L`。
- 小标题：`AIRFLOW TASK HUB`，蓝色，大写。
- 主标题：`Lovito 任务中心`，其中 Lovito 使用品牌蓝。
- 服务下拉框：显示服务别名，例如 `146 Airflow`。

品牌标识样式：

- 44px × 44px。
- 8px 圆角。
- 浅蓝玻璃底。
- 文字 `L` 使用品牌蓝，字重 820。

右侧操作区：

- Owner 输入框。
- `重连` 按钮。
- `刷新` 主按钮。
- `添加 DAG` 按钮。
- `更新` 按钮。
- `帮助` 按钮。

按钮应使用图标 + 文案，不建议只用文字。图标风格为 2px 线性图标。

### 7.2 DAG 分类栏

横向按钮组，按钮之间间距 10-12px。

按钮内容：

```text
分类名  数量徽标
```

状态：

- 默认：白色玻璃底，灰蓝边框。
- Hover：轻微上浮 `translateY(-1px)`。
- Active：蓝色边框、浅蓝底、品牌蓝文字。

分类生成规则：

- 从 DAG tags 中读取非邮箱 tag。
- 邮箱 tag 如 `PIC: xxx@lovito.com` 不作为分类。
- 无有效 tag 的 DAG 归类到 `其他`。
- `全部` 展示所有 DAG 数量。

### 7.3 DAG 列表面板

面板头部：

- 标题 `DAGs`。
- 数量文案，例如 `Listing · 16 / 16 items`。
- 搜索框 placeholder：`搜索 DAG / 描述`。

DAG 行内容：

- 左侧开关，样式接近 Airflow DAG pause/unpause switch。
- DAG 名称。
- DAG 描述。
- 徽标行：Owner、分类、状态。

DAG 行状态：

- 默认：轻量白色背景。
- Hover：背景轻微变亮，不做复杂阴影。
- Active：浅蓝底，左侧 4px 品牌蓝竖线。
- Paused 状态：橙色徽标。
- Active 状态：绿色徽标。

性能要求：

- DAG 列表是长列表，不要为每行添加 backdrop-filter、复杂伪元素、高成本阴影。
- 快速滚动时不能出现明显空白、延迟加载或闪烁。

### 7.4 DAG 详情与 Trigger 表单

详情面板头部：

- DAG 名称。
- DAG 描述。
- 右侧刷新当前 DAG 的 icon button。

Trigger 表单：

- 第一行：`dag_run_id` 输入框 + `Trigger` 按钮。
- 未填写 dag_run_id 时，placeholder 展示默认格式：`shaun.pan_YYYYMMDD_HHMMSS`。
- Trigger 按钮为绿色强调按钮，包含播放图标。

dag_run conf：

- 如果该 DAG 有专属 conf fields，则显示字段输入表单。
- 如果没有专属 fields，则显示 JSON textarea，默认 `{}`。
- 专属字段每行最多两个输入框。
- 字段不额外显示左侧 label，使用 placeholder：`请输入 xxx`。
- 字段右侧可显示 info 图标，hover/focus 后弹出说明 tooltip。

Tooltip 要求：

- 背景接近不透明深色。
- z-index 高于 Runs、Task Tries、状态徽标。
- 支持较长内容滚动。
- 鼠标移到 tooltip 内不能消失。
- 不遮挡输入框首行内容。

### 7.5 Recent Runs

展示最近 DAG Runs。

每条 Run 包含：

- dag_run_id。
- 开始/结束时间。
- run_type，如 `manual` 或 `scheduled`。
- run conf 摘要。
- 状态徽标。
- 复制按钮，用于复制完整 run conf。

交互：

- 点击 Run 后选中并刷新 Task Tries。
- 点击 conf 摘要或复制按钮可查看/复制完整 JSON。
- 完整 conf 建议使用弹窗放大展示，不要直接占用列表空间。

样式：

- Active run 使用浅蓝底和左侧蓝色竖线。
- Run 列表要比 Task Tries 占用更大空间。

### 7.6 Task Tries

展示当前 selected run 下某个 task 的 try 记录。

内容：

- try 序号，例如 `try 1`。
- 开始/结束时间。
- 持续时间。
- 状态徽标。

空间策略：

- Task Tries 是辅助区，高度应小于 Recent Runs。
- 默认可显示 1-3 条。
- 点击 try 后右侧日志切换到对应日志。

### 7.7 Task Log

右侧日志面板是核心排查区域。

头部：

- 标题 `Task Log`。
- 当前上下文：`dag_id / dag_run_id / task_id`。
- 开关：`跟随当前 task`、`自动滚动`。
- 操作：搜索日志、刷新日志、终止、清空。

状态条：

- 显示 run 状态、task 状态、try 序号、log 来源。
- 状态使用 chip 样式。

日志区：

- 深色背景。
- 等宽字体。
- 每行一条 log。
- 支持横向滚动，不自动换行。
- 日志等级着色：
  - INFO：浅蓝
  - WARNING / WARN：黄色
  - ERROR / CRITICAL / FATAL：红色
  - SUCCESS：绿色
  - DEBUG：紫色或灰紫

搜索条：

- 支持打开/关闭。
- 显示匹配计数，例如 `3 / 12`。
- 支持上一个/下一个匹配。
- 当前匹配高亮更明显。

### 7.8 弹窗

通用弹窗包括：

- 使用指南。
- Run Conf 放大查看。
- 添加 DAG。
- 检查更新。

弹窗风格：

- 居中显示。
- 大圆角 8px。
- 玻璃底 + 柔和阴影。
- 遮罩使用半透明深色 + 背景模糊。
- 弹窗内容区域可滚动。

帮助弹窗要求：

- 第一次打开自动展示。
- 之后可通过右上角 `帮助` 查看。
- 内容需要讲解顶部区域、DAG 列表、Trigger 区、Recent Runs、Task Tries、Log 区。
- 可以放入产品截图式插画，但不要让截图元素层级盖住真实弹窗内容。

### 7.9 Toast

右上角或右下角显示轻量提示。

类型：

- 普通：蓝色左边线。
- 错误：红色左边线。

要求：

- 不阻断操作。
- 3-5 秒自动消失。
- z-index 高于主页面，低于模态弹窗。

## 8. 交互与动效

### 8.1 鼠标动态效果

可在按钮、分类按钮、弹窗、帮助卡片上加入基于鼠标位置的径向高光。

限制：

- 不要在长列表行上使用高成本动态高光。
- 不要在三大滚动面板上使用 backdrop-filter 动态 hover。
- 不要让 hover 动画影响滚动性能。

### 8.2 Hover / Active / Focus

- Hover：轻微上浮 1px，阴影增强。
- Active：轻微按压 `scale(0.985)`。
- Focus：使用蓝色 focus ring，不能只靠颜色变化。
- Disabled：降低透明度，禁止点击态。

### 8.3 Loading / Empty / Error

列表加载：

- 文案：`加载 DAG 中...`、`加载 runs 中...`。

空状态：

- `没有匹配的 DAG`
- `暂无 DAG`
- `暂无 Task Tries`

错误：

- 使用 toast + 面板内错误文案。
- 错误颜色使用红色，不要使用弹窗打断所有流程，除非是危险操作确认。

## 9. 可访问性要求

1. 所有 icon button 必须有 `title` 或 `aria-label`。
2. 重要按钮支持键盘 focus。
3. 可拖拽分隔线应使用 `role="separator"`，并支持键盘方向键微调。
4. 日志区域使用 `role="log"`。
5. Tooltip 支持 hover 和 focus。
6. 色彩状态不能只依赖颜色，状态 chip 需要有文本。
7. 输入框 placeholder 不能被 icon 或 tooltip 挡住。

## 10. 性能要求

1. DAG 列表、Run 列表、Task Try 列表快速滚动时不能明显掉帧。
2. 长列表行不使用 `backdrop-filter`。
3. 长列表行不使用复杂 hover 伪元素。
4. 日志区域支持大量文本横向滚动。
5. UI 动效总时长建议 120-180ms。
6. 页面初始化后首屏应优先展示框架和 loading 状态。

## 11. 文案规范

保留英文技术名词：

- DAG
- Task
- Task Log
- Recent Runs
- Task Tries
- Trigger
- Owner
- Airflow

中文操作词：

- 重连
- 刷新
- 添加 DAG
- 更新
- 帮助
- 终止
- 自动滚动
- 跟随当前 task

输入框 placeholder：

- DAG 搜索：`搜索 DAG / 描述`
- dag_run_id：`shaun.pan_YYYYMMDD_HHMMSS`
- mapped conf field：`请输入 xxx`
- 日志搜索：`搜索日志`

## 12. 验收标准

外部应用生成页面后，需要满足以下标准：

1. 第一屏包含顶部品牌区、分类栏、三栏工作区。
2. 页面整体呈现浅蓝白液态玻璃风格，但长列表滚动流畅。
3. DAG 列表行包含开关、名称、描述、Owner chip、分类 chip、状态 chip。
4. 分类栏能展示 `全部`、动态 tag 分类和 `其他`。
5. Trigger 区能同时支持 mapped fields 和 JSON textarea 两种形态。
6. Tooltip 展开时不被 Recent Runs 或状态 chip 遮挡。
7. Recent Runs 能显示 run conf 摘要，并提供查看/复制完整 conf 的入口。
8. Task Tries 区域小于 Recent Runs 区域，不抢占主要空间。
9. Log 区为深色等宽字体，支持横向滚动和按日志等级着色。
10. 顶部按钮、分类按钮、弹窗等低频元素具备轻量 hover 动效。
11. 所有主要操作按钮都有图标或清晰文字。
12. 页面宽度收缩到 1040px 时不出现文字互相覆盖。

## 13. 非目标

1. 不要求外部应用实现真实 Airflow API。
2. 不要求实现 Electron 打包逻辑。
3. 不要求实现账号 cookie、服务切换、更新下载等后端能力。
4. 不做营销 landing page。
5. 不做移动端优先布局。

