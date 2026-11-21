# Changelog

## [0.1.0] - 2026-11-21

### Added

- 新增 `antigravity` 模型后端注册，支持通过官方 `agy acp` 接入 Gemini 与 Claude 等可用模型。
- 新增模型家族自动聚合与 Thinking 档位（`low`、`medium`、`high`、`xhigh`、`max`）联动支持。
- 新增 `pi-tools`（默认）、`agy-tools` 与 `hybrid` 三种可切换工具运行模式。
- 新增 `agy-tools` 模式原生工具卡片与红绿对比 Diff 主题渲染器，支持交互式权限审批与结构化提问弹窗。
- 新增 `bridgePiMcp`、`bridgePiSkills`、`bridgePiAgentsMd` 独立桥接控制开关及反向 Skills 发现支持。
- 新增跨会话恢复、分支回退同步、上下文压缩自动轮换及 `/agy sessions` 本机历史会话导入能力。
- 新增 `/agy` 斜杠命令族与底部状态栏状态标识，支持 Windows、Linux 与 macOS。
