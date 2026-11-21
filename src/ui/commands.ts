import { spawnSync } from "node:child_process";
import type {
	AssistantMessage,
	ImageContent,
	TextContent,
	ThinkingContent,
	UserMessage,
} from "@earendil-works/pi-ai";
import type {
	ExtensionAPI,
	ExtensionCommandContext,
	ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import type { AutocompleteItem } from "@earendil-works/pi-tui";
import { probeAgyVersion, resolveAgyBinary } from "../acp/binary-resolver.ts";
import type { AcpProcessManager } from "../acp/process-manager.ts";
import type {
	AcpSessionListResult,
	AcpSessionLoadResult,
	AcpSessionSummary,
} from "../acp/protocol.ts";
import {
	ANTIGRAVITY_API_ID,
	ANTIGRAVITY_PROVIDER_ID,
} from "../catalog/model-discovery.ts";
import {
	type AgyExecutionMode,
	type AutoBoolean,
	type BridgeConfig,
	parseAutoBoolean,
	resolveEffectiveFlags,
	saveGlobalBridgeConfigPatch,
	type ToolMode,
} from "../config.ts";
import { isPiMcpRelatedTool } from "../mcp/pi-mcp-forwarder.ts";
import type { TurnCoordinator } from "../session/turn-coordinator.ts";
import { createZeroUsage } from "../stream/event-mapper.ts";
import { AGY_MIRROR_TOOL_NAMES } from "../tools/agy-renderers.ts";
import { updateAntigravityStatusBar } from "./status-bar.ts";

const OVERLAPPING_PI_BASE_TOOLS = new Set<string>([
	"read",
	"edit",
	"write",
	"bash",
	"powershell",
	"grep",
	"find",
	"ls",
]);

/**
 * 管理进入或退出 Antigravity 不同工具模式时的 Pi 激活工具集快照与自动恢复。
 */
export class ToolActivationController {
	private readonly pi: ExtensionAPI;
	private savedUserActiveTools?: string[];

	constructor(pi: ExtensionAPI) {
		this.pi = pi;
	}

	syncForContext(ctx: ExtensionContext | undefined, config: BridgeConfig): void {
		const isAgyModel = ctx?.model?.provider === ANTIGRAVITY_PROVIDER_ID;

		if (!isAgyModel || config.toolMode === "pi-tools") {
			this.restoreUserTools();
			return;
		}

		let current: string[];
		try {
			current = this.pi.getActiveTools();
		} catch {
			return;
		}

		const mirrorSet = new Set<string>(AGY_MIRROR_TOOL_NAMES);
		const nonMirrorCurrent = current.filter((t) => !mirrorSet.has(t));

		if (!this.savedUserActiveTools && nonMirrorCurrent.length > 0) {
			this.savedUserActiveTools = nonMirrorCurrent;
		}

		const baseUserTools = this.savedUserActiveTools ?? nonMirrorCurrent;
		const flags = resolveEffectiveFlags(config);

		if (config.toolMode === "agy-tools") {
			if (flags.bridgePiMcp && config.piMcpBridgeStrategy === "via-host") {
				let allRegisteredNames: string[] = [];
				try {
					allRegisteredNames = this.pi.getAllTools().map((t) => t.name);
				} catch {
					// 忽略未初始化运行态
				}
				const candidatePool = Array.from(new Set([...baseUserTools, ...allRegisteredNames]));
				const mcpBridgedTools = candidatePool.filter(
					(name) =>
						isPiMcpRelatedTool(name) || name === "codemode" || name === "tool_search",
				);
				this.pi.setActiveTools([...AGY_MIRROR_TOOL_NAMES, ...mcpBridgedTools]);
			} else {
				this.pi.setActiveTools([...AGY_MIRROR_TOOL_NAMES]);
			}
		} else {
			// hybrid：激活镜像工具 + 不与 AGY 内置基础读写冲突的 Pi 扩展工具
			const extraTools = baseUserTools.filter((name) => !OVERLAPPING_PI_BASE_TOOLS.has(name));
			this.pi.setActiveTools([...AGY_MIRROR_TOOL_NAMES, ...extraTools]);
		}
	}

	private restoreUserTools(): void {
		let current: string[];
		try {
			current = this.pi.getActiveTools();
		} catch {
			return;
		}

		const mirrorSet = new Set<string>(AGY_MIRROR_TOOL_NAMES);
		const hasAnyMirror = current.some((t) => mirrorSet.has(t));

		if (this.savedUserActiveTools) {
			const restored = this.savedUserActiveTools;
			this.savedUserActiveTools = undefined;
			this.pi.setActiveTools(restored);
		} else if (hasAnyMirror) {
			const cleaned = current.filter((t) => !mirrorSet.has(t));
			this.pi.setActiveTools(
				cleaned.length > 0 ? cleaned : ["read", "bash", "edit", "write"],
			);
		}
	}
}

export interface CommandDependencies {
	pi: ExtensionAPI;
	processManager: AcpProcessManager;
	coordinator: TurnCoordinator;
	toolActivation: ToolActivationController;
	onRefreshProviderModels: (ctx: ExtensionContext) => Promise<number>;
}

const SUBCOMMANDS: AutocompleteItem[] = [
	{ value: "status", label: "status", description: "查看 Antigravity 连接、订阅等级与桥接状态" },
	{ value: "mode", label: "mode", description: "切换工具模式 (pi-tools | agy-tools | hybrid)" },
	{ value: "bridge", label: "bridge", description: "设置 Pi MCP / Skills / AGENTS.md 桥接开关" },
	{ value: "agent-mode", label: "agent-mode", description: "切换 AGY 原生执行模式 (default | accept-edits | plan)" },
	{ value: "refresh", label: "refresh", description: "重新探测当前账号可用的模型与思考档位" },
	{ value: "sessions", label: "sessions", description: "浏览本机 AGY 会话并导入至当前 Pi 会话" },
	{ value: "login", label: "login", description: "执行官方 agy 终端登录授权" },
	{ value: "logout", label: "logout", description: "登出当前 AGY 账号凭据" },
];

function buildCompletions(prefix: string): AutocompleteItem[] | null {
	const trimmed = prefix.trimStart();
	const parts = trimmed.split(/\s+/);

	if (parts.length <= 1) {
		const q = (parts[0] ?? "").toLowerCase();
		const matches = SUBCOMMANDS.filter((c) => c.value.startsWith(q));
		return matches.length > 0 ? matches : null;
	}

	const sub = parts[0].toLowerCase();
	const arg1 = (parts[1] ?? "").toLowerCase();

	if (sub === "mode" && parts.length === 2) {
		return [
			{ value: "mode pi-tools", label: "pi-tools", description: "通过 MCP 挂载 Pi 工具集主控模式（默认）" },
			{ value: "mode agy-tools", label: "agy-tools", description: "AGY 原生工具与 Subagent 模式" },
			{ value: "mode hybrid", label: "hybrid", description: "AGY 原生工具 + Pi 专属扩展工具混合模式" },
		].filter((i) => i.label.startsWith(arg1) || i.value.startsWith(`mode ${arg1}`));
	}

	if (sub === "agent-mode" && parts.length === 2) {
		return [
			{ value: "agent-mode default", label: "default", description: "修改与执行命令前按权限规则询问" },
			{ value: "agent-mode accept-edits", label: "accept-edits", description: "自动批准工作区内文件编辑" },
			{ value: "agent-mode plan", label: "plan", description: "先规划方案再执行操作" },
		].filter((i) => i.value.startsWith(`agent-mode ${arg1}`));
	}

	if (sub === "bridge") {
		if (parts.length === 2) {
			return [
				{ value: "bridge mcp", label: "mcp", description: "控制是否将 Pi MCP 桥接给 AGY" },
				{ value: "bridge skills", label: "skills", description: "控制是否将 Pi Skills 桥接给 AGY" },
				{ value: "bridge agents-md", label: "agents-md", description: "控制是否将 Pi AGENTS.md 桥接给 AGY" },
			].filter((i) => i.value.startsWith(`bridge ${arg1}`));
		}
		if (parts.length === 3) {
			const target = parts[1].toLowerCase();
			const valPrefix = parts[2].toLowerCase();
			return ["on", "off", "auto"]
				.filter((v) => v.startsWith(valPrefix))
				.map((v) => ({
					value: `bridge ${target} ${v}`,
					label: v,
					description: v === "auto" ? "跟随当前工具模式默认行为" : v === "on" ? "强制开启" : "强制关闭",
				}));
		}
	}

	return null;
}

function formatAutoBoolLabel(raw: AutoBoolean, effective: boolean): string {
	if (raw === "auto") {
		return `auto (${effective ? "开启" : "关闭"})`;
	}
	return raw ? "开启 (on)" : "关闭 (off)";
}

/** 注册统一的 `/agy` 斜杠命令入口 */
export function registerAntigravityCommands(deps: CommandDependencies): void {
	const { pi, processManager, coordinator, toolActivation, onRefreshProviderModels } = deps;

	pi.registerCommand("agy", {
		description: "管理 Antigravity 模型后端、工具模式、生态桥接与历史会话",
		getArgumentCompletions: (prefix) => buildCompletions(prefix),
		handler: async (rawArgs: string, ctx: ExtensionCommandContext) => {
			coordinator.setActiveContext(ctx);
			const tokens = rawArgs
				.trim()
				.split(/\s+/)
				.filter((s) => s.length > 0);
			const sub = (tokens[0] ?? "status").toLowerCase();

			switch (sub) {
				case "status": {
					await handleStatusSubcommand(ctx, coordinator);
					return;
				}
				case "mode": {
					await handleModeSubcommand(tokens.slice(1), ctx, coordinator, toolActivation);
					return;
				}
				case "bridge": {
					await handleBridgeSubcommand(tokens.slice(1), ctx, coordinator);
					return;
				}
				case "agent-mode": {
					await handleAgentModeSubcommand(tokens.slice(1), ctx, coordinator);
					return;
				}
				case "refresh": {
					ctx.ui.notify("正在探测 Antigravity 可用模型目录...", "info");
					try {
						const count = await onRefreshProviderModels(ctx);
						ctx.ui.notify(`已刷新 Antigravity 模型目录（共 ${count} 个可用模型）。`, "info");
					} catch (error) {
						ctx.ui.notify(
							`模型刷新失败：${error instanceof Error ? error.message : String(error)}`,
							"error",
						);
					}
					return;
				}
				case "login": {
					await handleLoginSubcommand(ctx, coordinator, onRefreshProviderModels);
					return;
				}
				case "logout": {
					await handleLogoutSubcommand(ctx, processManager, coordinator);
					return;
				}
				case "sessions":
				case "import": {
					await handleSessionsImportSubcommand(ctx, processManager, coordinator);
					return;
				}
				default: {
					ctx.ui.notify(
						`未知子命令 "${sub}"。可用子命令：status, mode, bridge, agent-mode, refresh, sessions, login, logout`,
						"warning",
					);
				}
			}
		},
	});
}

async function handleStatusSubcommand(
	ctx: ExtensionCommandContext,
	coordinator: TurnCoordinator,
): Promise<void> {
	const cfg = coordinator.config;
	const flags = resolveEffectiveFlags(cfg);
	const telemetry = coordinator.getTelemetrySnapshot();
	const bin = resolveAgyBinary({ configuredBin: cfg.agyBin, geminiDir: cfg.geminiDir });
	const ver = bin ? probeAgyVersion(bin) : undefined;

	const lines = [
		`Antigravity CLI：${bin ? `${bin.binPath}${ver ? ` (v${ver})` : ""}` : "未找到可执行文件"}`,
		`订阅层级：${telemetry.tier ? `${telemetry.tier.name}` : "待首次连接探测"}`,
		`当前工具模式：${cfg.toolMode}`,
		`AGY 原生执行策略：${cfg.agyExecutionMode}`,
		`Pi MCP 桥接：${formatAutoBoolLabel(cfg.bridgePiMcp, flags.bridgePiMcp)}`,
		`Pi Skills 桥接：${formatAutoBoolLabel(cfg.bridgePiSkills, flags.bridgePiSkills)}`,
		`Pi AGENTS.md 桥接：${formatAutoBoolLabel(cfg.bridgePiAgentsMd, flags.bridgePiAgentsMd)}`,
		`活跃会话：${telemetry.agySessionId ?? "无（将在下次提问时按需创建）"}`,
	];

	if (typeof telemetry.usedTokens === "number") {
		const winText = telemetry.windowSize ? ` / ${telemetry.windowSize.toLocaleString()}` : "";
		lines.push(`实时后端上下文占用：${telemetry.usedTokens.toLocaleString()}${winText} tokens`);
	}

	if (ctx.hasUI) {
		await ctx.ui.select("Antigravity 状态总览（按 Esc 关闭）", lines);
	} else {
		ctx.ui.notify(lines.join("\n"), "info");
	}
}

async function handleModeSubcommand(
	args: string[],
	ctx: ExtensionCommandContext,
	coordinator: TurnCoordinator,
	toolActivation: ToolActivationController,
): Promise<void> {
	let targetMode = args[0]?.toLowerCase() as ToolMode | undefined;
	const validModes: ToolMode[] = ["pi-tools", "agy-tools", "hybrid"];

	if (!targetMode || !validModes.includes(targetMode)) {
		if (!ctx.hasUI) {
			ctx.ui.notify("请指定模式：/agy mode <pi-tools | agy-tools | hybrid>", "warning");
			return;
		}
		const options = [
			"pi-tools — 通过 MCP 挂载并使用 Pi 工具集与审批流（默认）",
			"agy-tools — 使用 AGY 原生工具、Subagents 与沙箱能力",
			"hybrid — AGY 原生工具 + Pi 专属第三方扩展工具",
		];
		const picked = await ctx.ui.select(
			`切换工具模式（当前：${coordinator.config.toolMode}）`,
			options,
		);
		if (!picked) return;
		targetMode = picked.split(" ")[0] as ToolMode;
	}

	if (targetMode === coordinator.config.toolMode) {
		ctx.ui.notify(`当前已处于 ${targetMode} 模式。`, "info");
		return;
	}

	const nextConfig: BridgeConfig = {
		...coordinator.config,
		toolMode: targetMode,
	};
	coordinator.setConfig(nextConfig);
	saveGlobalBridgeConfigPatch({ toolMode: targetMode });
	toolActivation.syncForContext(ctx, nextConfig);
	updateAntigravityStatusBar(ctx, nextConfig, coordinator.getTelemetrySnapshot());

	ctx.ui.notify(`已切换至 ${targetMode} 模式。`, "info");
}

async function handleBridgeSubcommand(
	args: string[],
	ctx: ExtensionCommandContext,
	coordinator: TurnCoordinator,
): Promise<void> {
	const keyMap: Record<string, keyof Pick<BridgeConfig, "bridgePiMcp" | "bridgePiSkills" | "bridgePiAgentsMd">> = {
		mcp: "bridgePiMcp",
		skills: "bridgePiSkills",
		"agents-md": "bridgePiAgentsMd",
		agentsmd: "bridgePiAgentsMd",
	};

	let targetFieldKey = args[0] ? keyMap[args[0].toLowerCase()] : undefined;
	let rawValStr = args[1]?.toLowerCase();

	if (!targetFieldKey) {
		if (!ctx.hasUI) {
			ctx.ui.notify(
				"用法：/agy bridge <mcp | skills | agents-md> <on | off | auto>",
				"warning",
			);
			return;
		}
		const flags = resolveEffectiveFlags(coordinator.config);
		const targetLabels = [
			`mcp — 桥接 Pi MCP（当前：${formatAutoBoolLabel(coordinator.config.bridgePiMcp, flags.bridgePiMcp)}）`,
			`skills — 桥接 Pi Skills（当前：${formatAutoBoolLabel(coordinator.config.bridgePiSkills, flags.bridgePiSkills)}）`,
			`agents-md — 桥接 Pi AGENTS.md（当前：${formatAutoBoolLabel(coordinator.config.bridgePiAgentsMd, flags.bridgePiAgentsMd)}）`,
		];
		const pickedTarget = await ctx.ui.select("选择要调整的桥接项", targetLabels);
		if (!pickedTarget) return;
		const prefix = pickedTarget.split(" ")[0];
		targetFieldKey = keyMap[prefix];
	}

	if (!rawValStr || !["on", "off", "auto", "true", "false"].includes(rawValStr)) {
		if (!ctx.hasUI) {
			ctx.ui.notify("请指定开关状态：on, off 或 auto", "warning");
			return;
		}
		const pickedVal = await ctx.ui.select("选择桥接策略", [
			"auto — 跟随当前工具模式默认策略",
			"on — 始终开启桥接",
			"off — 始终关闭桥接",
		]);
		if (!pickedVal) return;
		rawValStr = pickedVal.split(" ")[0];
	}

	const parsedVal = parseAutoBoolean(rawValStr, "auto");
	const nextConfig: BridgeConfig = {
		...coordinator.config,
		[targetFieldKey]: parsedVal,
	};
	coordinator.setConfig(nextConfig);
	saveGlobalBridgeConfigPatch({ [targetFieldKey]: parsedVal });

	const updatedFlags = resolveEffectiveFlags(nextConfig);
	const effectiveNow =
		targetFieldKey === "bridgePiMcp"
			? updatedFlags.bridgePiMcp
			: targetFieldKey === "bridgePiSkills"
				? updatedFlags.bridgePiSkills
				: updatedFlags.bridgePiAgentsMd;

	ctx.ui.notify(
		`已更新 ${targetFieldKey} 为 ${formatAutoBoolLabel(parsedVal, effectiveNow)}。`,
		"info",
	);
}

async function handleAgentModeSubcommand(
	args: string[],
	ctx: ExtensionCommandContext,
	coordinator: TurnCoordinator,
): Promise<void> {
	const valid: AgyExecutionMode[] = ["default", "accept-edits", "plan"];
	let chosen = args[0]?.toLowerCase() as AgyExecutionMode | undefined;

	if (!chosen || !valid.includes(chosen)) {
		if (!ctx.hasUI) {
			ctx.ui.notify("用法：/agy agent-mode <default | accept-edits | plan>", "warning");
			return;
		}
		const picked = await ctx.ui.select(
			`选择 AGY 原生执行模式（当前：${coordinator.config.agyExecutionMode}）`,
			[
				"default — 修改文件与执行命令前按权限设置询问",
				"accept-edits — 自动批准工作区内文件修改",
				"plan — 先输出规划方案再执行",
			],
		);
		if (!picked) return;
		chosen = picked.split(" ")[0] as AgyExecutionMode;
	}

	const nextConfig: BridgeConfig = {
		...coordinator.config,
		agyExecutionMode: chosen,
	};
	coordinator.setConfig(nextConfig);
	saveGlobalBridgeConfigPatch({ agyExecutionMode: chosen });
	await coordinator.applyExecutionModeLive(chosen);
	updateAntigravityStatusBar(ctx, nextConfig, coordinator.getTelemetrySnapshot());

	ctx.ui.notify(`已将 AGY 原生执行模式设为 ${chosen}。`, "info");
}

async function handleLoginSubcommand(
	ctx: ExtensionCommandContext,
	coordinator: TurnCoordinator,
	onRefreshProviderModels: (ctx: ExtensionContext) => Promise<number>,
): Promise<void> {
	const bin = resolveAgyBinary({
		configuredBin: coordinator.config.agyBin,
		geminiDir: coordinator.config.geminiDir,
	});
	if (!bin) {
		ctx.ui.notify("未找到本机 agy 可执行文件，请先安装 Antigravity CLI。", "error");
		return;
	}

	if (ctx.mode === "tui") {
		const confirmed = await ctx.ui.confirm(
			"Antigravity 登录说明",
			"认证凭据由官方 agy CLI 本地管理，本扩展不读取或保存任何令牌。\n是否在当前终端启动官方 `agy acp --login` 完成授权？",
		);
		if (!confirmed) return;

		const spawnLoginCmd =
			bin.isWindowsWrapperScript && bin.binPath.includes(" ")
				? `"${bin.binPath}"`
				: bin.binPath;

		const exitStatus = await ctx.ui.custom<number | null>((tui, _theme, _kb, done) => {
			tui.stop();
			process.stdout.write("\x1b[2J\x1b[H");
			let status: number | null = null;
			try {
				const res = spawnSync(
					spawnLoginCmd,
					["acp", "--login", "--gemini-dir", coordinator.config.geminiDir],
					{
						stdio: "inherit",
						shell: bin.isWindowsWrapperScript,
					},
				);
				status = res.status;
			} finally {
				tui.start();
				tui.requestRender(true);
				done(status);
			}
			return { render: () => [], invalidate: () => {} };
		});

		if (exitStatus === 0) {
			const count = await onRefreshProviderModels(ctx);
			ctx.ui.notify(`登录完成，已加载 ${count} 个可用模型。`, "info");
		} else {
			ctx.ui.notify("登录流程未完成，也可在独立终端执行 `agy acp --login` 后运行 `/agy refresh`。", "warning");
		}
		return;
	}

	ctx.ui.notify("请在终端执行 `agy acp --login` 完成登录，随后运行 `/agy refresh` 刷新模型。", "info");
}

async function handleLogoutSubcommand(
	ctx: ExtensionCommandContext,
	processManager: AcpProcessManager,
	coordinator: TurnCoordinator,
): Promise<void> {
	try {
		const conn = await processManager.acquire(coordinator.config);
		await conn.request("auth/logout", {}, { timeoutMs: 10_000 });
		await coordinator.dispose();
		ctx.ui.notify("已登出 Antigravity 账号。", "info");
	} catch (error) {
		ctx.ui.notify(
			`登出失败：${error instanceof Error ? error.message : String(error)}`,
			"error",
		);
	}
}

async function handleSessionsImportSubcommand(
	ctx: ExtensionCommandContext,
	processManager: AcpProcessManager,
	coordinator: TurnCoordinator,
): Promise<void> {
	if (!ctx.hasUI) {
		ctx.ui.notify("浏览并导入历史会话需要交互式界面支持。", "warning");
		return;
	}

	const conn = await processManager.acquire(coordinator.config);
	const listRes = await conn.request<AcpSessionListResult>(
		"session/list",
		{ cwd: ctx.cwd },
		{ timeoutMs: 15_000 },
	);

	const sessions: AcpSessionSummary[] = listRes.sessions ?? [];
	if (sessions.length === 0) {
		ctx.ui.notify("未找到可导入的本机 Antigravity 历史会话。", "info");
		return;
	}

	const topSessions = sessions.slice(0, 30);
	const labels = topSessions.map((s) => {
		const title = s.title?.trim() || "(无标题会话)";
		const time = s.updatedAt ? ` · ${s.updatedAt}` : "";
		return `${title}${time} [${s.sessionId.slice(0, 8)}]`;
	});

	const pickedLabel = await ctx.ui.select("选择要导入到新 Pi 会话的 AGY 会话", labels);
	if (!pickedLabel) return;

	const idx = labels.indexOf(pickedLabel);
	const target = topSessions[idx >= 0 ? idx : 0];

	// 通过 session/load 收集回放事件流并组装为标准 Pi 消息序列
	const replayedMessages: Array<UserMessage | AssistantMessage> = [];
	let currentUserParts: Array<TextContent | ImageContent> = [];
	let currentAssistantBlocks: Array<ThinkingContent | TextContent> = [];

	const flushUser = () => {
		if (currentUserParts.length === 0) return;
		replayedMessages.push({
			role: "user",
			content: currentUserParts,
			timestamp: Date.now(),
		});
		currentUserParts = [];
	};

	const flushAssistant = () => {
		if (currentAssistantBlocks.length === 0) return;
		replayedMessages.push({
			role: "assistant",
			content: currentAssistantBlocks,
			api: ANTIGRAVITY_API_ID,
			provider: ANTIGRAVITY_PROVIDER_ID,
			model: ctx.model?.id ?? "gemini-4-neon",
			responseId: target.sessionId,
			usage: createZeroUsage(),
			stopReason: "stop",
			timestamp: Date.now(),
		});
		currentAssistantBlocks = [];
	};

	const unsubscribe = coordinator.registerAuxiliaryUpdateListener(target.sessionId, (update) => {
		if (update.sessionUpdate === "user_message_chunk") {
			flushAssistant();
			if (update.content.type === "text") {
				const prev = currentUserParts[currentUserParts.length - 1];
				if (prev?.type === "text") {
					prev.text += update.content.text;
				} else {
					currentUserParts.push({ type: "text", text: update.content.text });
				}
			} else if (update.content.type === "image") {
				currentUserParts.push({
					type: "image",
					data: update.content.data,
					mimeType: update.content.mimeType,
				});
			}
			return;
		}

		if (update.sessionUpdate === "agent_thought_chunk" && update.content.type === "text") {
			flushUser();
			const prev = currentAssistantBlocks[currentAssistantBlocks.length - 1];
			if (prev?.type === "thinking") {
				prev.thinking += update.content.text;
			} else {
				currentAssistantBlocks.push({ type: "thinking", thinking: update.content.text });
			}
			return;
		}

		if (update.sessionUpdate === "agent_message_chunk" && update.content.type === "text") {
			flushUser();
			const prev = currentAssistantBlocks[currentAssistantBlocks.length - 1];
			if (prev?.type === "text") {
				prev.text += update.content.text;
			} else {
				currentAssistantBlocks.push({ type: "text", text: update.content.text });
			}
		}
	});

	try {
		await conn.request<AcpSessionLoadResult>(
			"session/load",
			{
				sessionId: target.sessionId,
				cwd: target.cwd || ctx.cwd,
				mcpServers: [],
			},
			{ timeoutMs: 25_000 },
		);
		flushUser();
		flushAssistant();
	} finally {
		unsubscribe();
		await conn.closeSessionSafe(target.sessionId);
	}

	await ctx.newSession({
		setup: async (sessionManager) => {
			for (const msg of replayedMessages) {
				sessionManager.appendMessage(msg);
			}
			if (target.title?.trim()) {
				sessionManager.appendSessionInfo(target.title.trim());
			}
		},
		withSession: async (replacedCtx) => {
			coordinator.registerImportedSessionAnchor({
				agySessionId: target.sessionId,
				visibleMessages: replayedMessages,
				mountDigest: "",
				importedRaw: true,
			});
			replacedCtx.ui.notify(
				`已导入会话 "${target.title || target.sessionId.slice(0, 8)}"（共 ${replayedMessages.length} 条消息）。`,
				"info",
			);
		},
	});
}
