# 项目同步至 GitHub 的说明

本目录是代码的 GitHub 同步副本。

由于脚本程序的检查和自动更新功能依赖于 GitHub 上的资源（如代码包和 manifest 文件等），因此需将本项目同步上传至 GitHub 仓库，确保自动更新功能可以正常运行。

请在每次本地更新并发布 package 后，将相关文件推送到 GitHub，以便其他环境可以获取和检查最新版本。

同步流程示例：
1. 在本地生成发布包（如 `package_<version>.zip` 与 `manifest.json`）。
2. 将上述文件放入本目录。
3. git add、commit 并同步推送到 GitHub 仓库。

如有更多细节需求，请参考脚本工具的发布说明或脚本内的帮助信息。