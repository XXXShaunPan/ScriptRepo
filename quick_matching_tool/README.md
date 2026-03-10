# Quick Matching Tool

## 运行方式

### 开发环境

```bash
PYTHONPATH=quick_matching_tool/src python -m quick_matching_tool
```

### 打包

```bash
python quick_matching_tool/build.py
```

## 配置与凭证

本项目不再内置 Google 服务账号密钥，需自行提供：

1. 填入真实服务账号内容
   - `quick_matching_tool/config/credentials.json`
2. 或通过环境变量指定：
   - `QUICK_MATCHING_CREDENTIALS_PATH=/path/to/credentials.json`

## 目录结构（新）

```
quick_matching_tool/
  src/quick_matching_tool/
    core/        # 业务编排
    infra/       # Google/Shopee/浏览器
    ui/          # PyQt UI
    config/      # 配置
    utils/       # 工具
  tests/
  build.py
  QuickMatchingTool.spec
```

## 依赖

依赖见 `quick_matching_tool/requirements.txt`。建议使用虚拟环境安装。
