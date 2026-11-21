import {
	getCurrentTools,
	type Api,
	type AssistantMessage,
	type AssistantMessageEventStream,
	type Model,
	type SimpleStreamOptions,
	type Tool,
	type ToolResultMessage,
	type TranscriptContext,
	type UserMessage,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AcpClientConnection, AcpProcessManager } from "../acp/process-manager.ts";
import type {
	AcpAvailableModel,
	AcpConfigOption,
	AcpMcpServerSpec,
	AcpPromptBlock,
	AcpPromptResponse,
	AcpSessionNewResult,
	AcpSessionResumeResult,
	AcpSessionUpdateNotification,
	AcpSessionUpdatePayload,
	AcpSetConfigOptionResult,
	AcpTierInfo,
} from "../acp/protocol.ts";
import {
	recordObservedContextWindow,
	resolveSessionModelSelection,
} from "../catalog/model-discovery.ts";
import { type BridgeConfig, type ToolMode } from "../config.ts";
import { LocalHttpMcpBridge, type ParkedToolCall } from "../mcp/http-mcp-bridge.ts";
import {
	computeToolsFingerprint,
	filterToolsForHostMcpBridge,
	loadDirectPiMcpServerSpecs,
} from "../mcp/pi-mcp-forwarder.ts";
import { StepStreamWriter } from "../stream/event-mapper.ts";
import type { AgyTurnMirrorBroker } from "../tools/agy-mirror-tools.ts";
import {
	updateAntigravityStatusBar,
	type RuntimeTelemetrySnapshot,
} from "../ui/status-bar.ts";
import {
	AGY_SESSION_ANCHOR_CUSTOM_TYPE,
	type AgySessionAnchor,
	captureSqliteCheckpoint,
	findBranchAnchorForHistory,
	forkSqliteConversationAtCheckpoint,
} from "./checkpoint-sync.ts";
import { prepareCustomizationMount } from "./customization-mounter.ts";
import {
	buildDetachedToolResultBlocks,
	buildFreshReplayPromptBlocks,
	computeTranscriptDigest,
	convertUserMessagesToPromptBlocks,
	extractVisibleMessages,
	isAuxiliarySingleTurnRequest,
} from "./reconcile.ts";

type PumpQueueItem =
	| { kind: "acp_update"; update: AcpSessionUpdatePayload }
	| { kind: "mcp_parked"; call: ParkedToolCall }
	| { kind: "prompt_done"; response: AcpPromptResponse }
	| { kind: "prompt_error"; error: unknown };

/** 异步 FIFO 事件队列，驱动单个原生 ACP Turn 跨多个 Pi `streamSimple` 步无缝流转 */
class AsyncEventQueue<T> {
	private readonly items: T[] = [];
	private readonly resolvers: Array<(item: T) => void> = [];

	push(item: T): void {
		const waiter = this.resolvers.shift();
		if (waiter) {
			waiter(item);
		} else {
			this.items.push(item);
		}
	}

	shift(signal?: AbortSignal): Promise<T> {
		const next = this.items.shift();
		if (next !== undefined) {
			return Promise.resolve(next);
		}
		if (signal?.aborted) {
			return Promise.reject(signal.reason ?? new Error("Aborted"));
		}

		return new Promise<T>((resolve, reject) => {
			const wrapped = (val: T) => {
				cleanup?.();
				resolve(val);
			};
			let cleanup: (() => void) | undefined;
			if (signal) {
				const onAbort = () => {
					const idx = this.resolvers.indexOf(wrapped);
					if (idx !== -1) this.resolvers.splice(idx, 1);
					reject(signal.reason ?? new Error("Aborted"));
				};
				signal.addEventListener("abort", onAbort, { once: true });
				cleanup = () => signal.removeEventListener("abort", onAbort);
			}
			this.resolvers.push(wrapped);
		});
	}

	peek(): T | undefined {
		return this.items[0];
	}

	drainAll(): T[] {
		return this.items.splice(0, this.items.length);
	}
}

interface ActiveTurnPump {
	readonly queue: AsyncEventQueue<PumpQueueItem>;
	finished: boolean;
	cancelledByHoldTimeout: boolean;
}

interface ManagedSessionLedger {
	piSessionKey: string;
	agySessionId: string;
	conn: AcpClientConnection;
	mcpToken?: string;
	hostMcpTools: Tool[];
	toolsDigest: string;
	mountDigest: string;
	toolMode: ToolMode;
	configOptions: AcpConfigOption[];
	availableModels: AcpAvailableModel[];
	appliedModelValue?: string;
	appliedThoughtLevel?: string;
	appliedExecMode?: string;
	syncedMessageCount: number;
	syncedTranscriptDigest: string;
	usedTokens?: number;
	windowSize?: number;
	tier?: AcpTierInfo;
	backendCompactedRecently?: boolean;
	parkedCalls: Map<string, ParkedToolCall>;
	detachedCallIds: Set<string>;
	activePump?: ActiveTurnPump;
}

const DEFAULT_SESSION_KEY = "__default_pi_session__";

/**
 * 双模会话与流式推理总协调器。
 * 统一调度模式 B（`pi-tools`）、模式 A（`agy-tools`）与混合模式（`hybrid`）。
 */
export class TurnCoordinator {
	private readonly pi: ExtensionAPI;
	private readonly processManager: AcpProcessManager;
	private readonly mirrorBroker: AgyTurnMirrorBroker;
	private readonly ledgers = new Map<string, ManagedSessionLedger>();
	private readonly ledgersByAgyId = new Map<string, ManagedSessionLedger>();
	private readonly auxUpdateListeners = new Map<
		string,
		(update: AcpSessionUpdatePayload) => void
	>();
	private readonly httpMcpBridge: LocalHttpMcpBridge;
	private currentConfig: BridgeConfig;
	private activeContext?: ExtensionContext;
	private latestTier?: AcpTierInfo;

	constructor(
		pi: ExtensionAPI,
		processManager: AcpProcessManager,
		mirrorBroker: AgyTurnMirrorBroker,
		initialConfig: BridgeConfig,
	) {
		this.pi = pi;
		this.processManager = processManager;
		this.mirrorBroker = mirrorBroker;
		this.currentConfig = initialConfig;
		this.httpMcpBridge = new LocalHttpMcpBridge(initialConfig.mcpHoldLimitMs);

		this.processManager.setHandlers({
			onSessionUpdate: (notification) => this.handleSessionUpdate(notification),
			onRequestPermission: (params) => this.mirrorBroker.handlePermissionRequest(params),
			onElicitationCreate: (params) => this.mirrorBroker.handleElicitationRequest(params),
		});
	}

	get config(): BridgeConfig {
		return this.currentConfig;
	}

	setConfig(nextConfig: BridgeConfig): void {
		this.currentConfig = nextConfig;
		this.httpMcpBridge.setHoldLimitMs(nextConfig.mcpHoldLimitMs);
		updateAntigravityStatusBar(this.activeContext, this.currentConfig, this.getTelemetrySnapshot());
	}

	setActiveContext(ctx: ExtensionContext | undefined): void {
		this.activeContext = ctx;
		this.mirrorBroker.setFallbackUiContext(ctx);
		updateAntigravityStatusBar(ctx, this.currentConfig, this.getTelemetrySnapshot());
	}

	setLatestTier(tier: AcpTierInfo | undefined): void {
		if (tier) {
			this.latestTier = tier;
		}
	}

	getTelemetrySnapshot(piSessionId?: string): RuntimeTelemetrySnapshot {
		const key = piSessionId || this.resolveCurrentPiSessionKey();
		const ledger = this.ledgers.get(key);
		return {
			agySessionId: ledger?.agySessionId,
			tier: ledger?.tier ?? this.latestTier,
			usedTokens: ledger?.usedTokens,
			windowSize: ledger?.windowSize,
			backendCompactedRecently: ledger?.backendCompactedRecently,
		};
	}

	/** 用户主动取消某个正在执行的 AGY 原生会话 */
	cancelAgySessionById(agySessionId: string): void {
		const ledger = this.ledgersByAgyId.get(agySessionId);
		if (!ledger) return;
		this.httpMcpBridge.markSessionTurnCancelled(agySessionId);
		this.mirrorBroker.abortSessionCalls(agySessionId, "Cancelled by user");
		void ledger.conn.request("session/cancel", { sessionId: agySessionId }).catch(() => {});
	}

	/** 实时切换当前活跃会话的 AGY 原生执行模式（`default` | `accept-edits` | `plan`） */
	async applyExecutionModeLive(mode: string): Promise<void> {
		const ledger = this.ledgers.get(this.resolveCurrentPiSessionKey());
		if (!ledger || !ledger.conn.isAlive || ledger.toolMode === "pi-tools") return;
		try {
			const res = await ledger.conn.request<AcpSetConfigOptionResult>(
				"session/set_config_option",
				{
					sessionId: ledger.agySessionId,
					configId: "mode",
					value: mode,
				},
				{ timeoutMs: 8_000 },
			);
			if (res.configOptions) {
				ledger.configOptions = res.configOptions;
			}
			ledger.appliedExecMode = mode;
		} catch {
			// 忽略离线会话设置失败，下一轮请求会自动重新对齐
		}
	}

	/** 当发生 `/compact`、`/new` 或切换会话时清理旧后端会话绑定 */
	async invalidatePiSession(piSessionId?: string): Promise<void> {
		const key = piSessionId || this.resolveCurrentPiSessionKey();
		const ledger = this.ledgers.get(key);
		if (!ledger) return;
		await this.teardownLedger(ledger);
	}

	/** 在每轮对话稳定完结时，将最新会话锚点（含可选 SQLite 检查点）追加进 Pi `.jsonl` 分支 */
	async persistTurnAnchor(
		messagesSnapshot?: readonly (UserMessage | AssistantMessage | ToolResultMessage)[],
	): Promise<void> {
		const key = this.resolveCurrentPiSessionKey();
		const ledger = this.ledgers.get(key);
		if (!ledger || ledger.activePump || ledger.parkedCalls.size > 0) {
			return;
		}

		if (messagesSnapshot) {
			ledger.syncedMessageCount = messagesSnapshot.length;
			ledger.syncedTranscriptDigest = computeTranscriptDigest(messagesSnapshot);
		}

		const dbCheckpoint = await captureSqliteCheckpoint(this.currentConfig, ledger.agySessionId);
		const anchor: AgySessionAnchor = {
			version: 1,
			agySessionId: ledger.agySessionId,
			toolMode: ledger.toolMode,
			transcriptDigest: ledger.syncedTranscriptDigest,
			messageCount: ledger.syncedMessageCount,
			mountDigest: ledger.mountDigest,
			toolsDigest: ledger.toolsDigest,
			dbCheckpoint,
		};

		try {
			this.pi.appendEntry(AGY_SESSION_ANCHOR_CUSTOM_TYPE, anchor);
		} catch {
			// 非持久化模式或只读上下文下忽略
		}
	}

	/** 供 `/agy sessions` 浏览导入后直接建立内存与分支锚点绑定 */
	registerImportedSessionAnchor(params: {
		agySessionId: string;
		visibleMessages: readonly (UserMessage | AssistantMessage | ToolResultMessage)[];
		mountDigest: string;
	}): AgySessionAnchor {
		const anchor: AgySessionAnchor = {
			version: 1,
			agySessionId: params.agySessionId,
			toolMode: this.currentConfig.toolMode,
			transcriptDigest: computeTranscriptDigest(params.visibleMessages),
			messageCount: params.visibleMessages.length,
			mountDigest: params.mountDigest,
			toolsDigest: "",
		};
		try {
			this.pi.appendEntry(AGY_SESSION_ANCHOR_CUSTOM_TYPE, anchor);
		} catch {
			// 忽略
		}
		return anchor;
	}

	/** Pi `registerProvider` 核心流式入口 */
	streamSimple(
		model: Model<Api>,
		context: TranscriptContext,
		options?: SimpleStreamOptions,
	): AssistantMessageEventStream {
		const writer = new StepStreamWriter(model, {
			thinkingLevel: options?.reasoning,
		});

		void this.runStreamStep(writer, model, context, options).catch((error) => {
			if (!writer.isTerminated) {
				const isAborted = Boolean(options?.signal?.aborted);
				const msg = error instanceof Error ? error.message : String(error);
				writer.fail(isAborted ? "aborted" : "error", msg);
			}
		});

		return writer.stream;
	}

	async dispose(): Promise<void> {
		for (const ledger of Array.from(this.ledgers.values())) {
			await this.teardownLedger(ledger);
		}
		this.ledgers.clear();
		this.ledgersByAgyId.clear();
		await this.httpMcpBridge.dispose();
		await this.processManager.disposeAll();
	}

	/** 借用带回放监听器的临时监听通道（供 `/agy sessions` 的 `session/load` 使用） */
	registerAuxiliaryUpdateListener(
		agySessionId: string,
		listener: (update: AcpSessionUpdatePayload) => void,
	): () => void {
		this.auxUpdateListeners.set(agySessionId, listener);
		return () => {
			this.auxUpdateListeners.delete(agySessionId);
		};
	}

	private resolveCurrentPiSessionKey(explicitSessionId?: string): string {
		if (explicitSessionId) return explicitSessionId;
		try {
			const sm = this.activeContext?.sessionManager;
			const id = sm?.getSessionId?.();
			if (id) return id;
		} catch {
			// 忽略
		}
		return DEFAULT_SESSION_KEY;
	}

	private handleSessionUpdate(notification: AcpSessionUpdateNotification): void {
		const { sessionId, update } = notification;

		const auxListener = this.auxUpdateListeners.get(sessionId);
		if (auxListener) {
			auxListener(update);
			return;
		}

		const ledger = this.ledgersByAgyId.get(sessionId);
		if (!ledger) return;

		if (update.sessionUpdate === "usage_update") {
			if (typeof update.used === "number") {
				if (
					typeof ledger.usedTokens === "number" &&
					ledger.usedTokens > 64_000 &&
					update.used < ledger.usedTokens * 0.65
				) {
					ledger.backendCompactedRecently = true;
				}
				ledger.usedTokens = update.used;
			}
			if (typeof update.size === "number") {
				ledger.windowSize = update.size;
			}
			updateAntigravityStatusBar(
				this.activeContext,
				this.currentConfig,
				this.getTelemetrySnapshot(ledger.piSessionKey),
			);
			return;
		}

		if (update.sessionUpdate === "session_info_update") {
			const newTitle = update.title?.trim();
			if (newTitle) {
				try {
					if (!this.pi.getSessionName()) {
						this.pi.setSessionName(newTitle);
					}
				} catch {
					// 忽略未绑定运行态
				}
			}
			return;
		}

		if (update.sessionUpdate === "config_option_update" && update.configOptions) {
			ledger.configOptions = update.configOptions;
			return;
		}

		// 处理工具调用事件路由：区分宿主 MCP 工具前置宣告与 AGY 原生工具调用
		if (update.sessionUpdate === "tool_call") {
			const isHostMcpAnnouncement = this.isHostMcpToolEvent(ledger, update.title);
			if (isHostMcpAnnouncement) {
				this.httpMcpBridge.recordAcpToolAnnouncement(sessionId, update.title || "");
				return;
			}

			// 模式 A / Hybrid 下的 AGY 原生工具调用
			ledger.activePump?.queue.push({ kind: "acp_update", update });
			return;
		}

		if (update.sessionUpdate === "tool_call_update") {
			const isHostMcpAnnouncement = this.isHostMcpToolEvent(ledger, update.title);
			if (!isHostMcpAnnouncement) {
				this.mirrorBroker.registerOrUpdateCall(sessionId, update);
			}
			return;
		}

		ledger.activePump?.queue.push({ kind: "acp_update", update });
	}

	private isHostMcpToolEvent(
		ledger: ManagedSessionLedger,
		rawTitle: string | undefined,
	): boolean {
		if (ledger.toolMode === "pi-tools") {
			return true;
		}
		if (ledger.hostMcpTools.length === 0) {
			return false;
		}
		if (!rawTitle) {
			return false;
		}
		const lower = rawTitle.toLowerCase();
		if (lower.includes("pi_tools")) {
			return true;
		}
		return ledger.hostMcpTools.some((t) => lower === t.name.toLowerCase());
	}

	private async runStreamStep(
		writer: StepStreamWriter,
		model: Model<Api>,
		context: TranscriptContext,
		options?: SimpleStreamOptions,
	): Promise<void> {
		const piSessionKey = this.resolveCurrentPiSessionKey(options?.sessionId);
		const primaryLedger = this.ledgers.get(piSessionKey);
		const hasBusyPrimary = Boolean(
			primaryLedger && (primaryLedger.activePump || primaryLedger.parkedCalls.size > 0),
		);

		// 1. 旁路隔离单轮无工具辅助请求（压缩摘要、分支总结、预热等）
		if (isAuxiliarySingleTurnRequest(context, options, hasBusyPrimary)) {
			await this.runEphemeralAuxiliaryTurn(writer, model, context, options);
			return;
		}

		const cwd = this.activeContext?.cwd ?? process.cwd();
		const isProjectTrusted = this.activeContext?.isProjectTrusted?.() ?? false;
		const declaredTools = getCurrentTools(context.messages);
		const hostMcpTools = filterToolsForHostMcpBridge(declaredTools, this.currentConfig);
		const toolsDigest = computeToolsFingerprint(hostMcpTools);
		const mountInfo = prepareCustomizationMount(context.messages, this.currentConfig, cwd);
		const visibleMessages = extractVisibleMessages(context.messages);

		// 2. 检查是否正在续接当前活跃会话的工具返回或分片流
		const existing = this.ledgers.get(piSessionKey);
		if (
			existing &&
			existing.conn.isAlive &&
			existing.toolMode === this.currentConfig.toolMode &&
			existing.mountDigest === mountInfo.mountDigest
		) {
			// 2a. 模式 B / Hybrid：结算挂起中的 Parked MCP 工具调用
			if (existing.parkedCalls.size > 0 || existing.detachedCallIds.size > 0) {
				await this.continueFromParkedOrDetachedToolResults(
					existing,
					writer,
					model,
					visibleMessages,
					hostMcpTools,
					toolsDigest,
					options,
				);
				return;
			}

			// 2b. 模式 A / Hybrid：镜像工具执行完毕，无缝续接后台同一活跃 ACP Turn 队列（含已缓存的尾部事件与终态）
			if (existing.activePump) {
				writer.setResponseMetadata({
					responseId: existing.agySessionId,
					providerThinkingLevel: existing.appliedThoughtLevel,
				});
				await this.consumePumpForSingleStep(existing, existing.activePump, writer, model, options);
				if (!existing.activePump && existing.parkedCalls.size === 0) {
					this.recordSyncedTranscript(existing, visibleMessages, writer.message);
				}
				return;
			}

			// 2c. 连续正常追问：前缀摘要匹配时直接复用活跃会话发送新增用户消息
			const continuationPrompts = this.tryExtractIncrementalUserPrompts(existing, visibleMessages);
			if (continuationPrompts && existing.toolsDigest === toolsDigest) {
				await this.executePromptOnLedger(
					existing,
					continuationPrompts,
					visibleMessages,
					writer,
					model,
					options,
				);
				return;
			}
		}

		// 3. 会话不存在、工具集/模式变更或历史分支偏离：安全拆解旧会话并按三级状态机重建/恢复
		if (existing) {
			await this.teardownLedger(existing);
		}

		const { ledger, initialPromptBlocks } = await this.bootstrapOrRestoreLedger({
			piSessionKey,
			cwd,
			isProjectTrusted,
			visibleMessages,
			hostMcpTools,
			toolsDigest,
			mountedPluginPaths: mountInfo.mountedPluginPaths,
			mountDigest: mountInfo.mountDigest,
			additionalDirectories: mountInfo.additionalDirectories,
			workspaceContextBlock: mountInfo.workspaceContextBlock,
			signal: options?.signal,
		});

		await this.executePromptOnLedger(
			ledger,
			initialPromptBlocks,
			visibleMessages,
			writer,
			model,
			options,
		);
	}

	private tryExtractIncrementalUserPrompts(
		ledger: ManagedSessionLedger,
		visibleMessages: readonly (UserMessage | AssistantMessage | ToolResultMessage)[],
	): AcpPromptBlock[] | undefined {
		if (visibleMessages.length <= ledger.syncedMessageCount) {
			return undefined;
		}
		const prefix = visibleMessages.slice(0, ledger.syncedMessageCount);
		if (computeTranscriptDigest(prefix) !== ledger.syncedTranscriptDigest) {
			return undefined;
		}
		const delta = visibleMessages.slice(ledger.syncedMessageCount);
		if (!delta.every((m): m is UserMessage => m.role === "user")) {
			return undefined;
		}
		const blocks = convertUserMessagesToPromptBlocks(delta);
		return blocks.length > 0 ? blocks : undefined;
	}

	private async continueFromParkedOrDetachedToolResults(
		ledger: ManagedSessionLedger,
		writer: StepStreamWriter,
		model: Model<Api>,
		visibleMessages: readonly (UserMessage | AssistantMessage | ToolResultMessage)[],
		hostMcpTools: Tool[],
		toolsDigest: string,
		options?: SimpleStreamOptions,
	): Promise<void> {
		// 更新动态工具表（例如上一轮刚执行了 tool_search 激活了新工具）
		if (ledger.mcpToken && ledger.toolsDigest !== toolsDigest) {
			ledger.hostMcpTools = hostMcpTools;
			ledger.toolsDigest = toolsDigest;
			this.httpMcpBridge.updateBindingTools(ledger.mcpToken, hostMcpTools);
		}

		// 从末尾收集本轮产生的 ToolResultMessage 与可能夹带的 Steering UserMessage
		const recentToolResults = new Map<string, ToolResultMessage>();
		const trailingUsers: UserMessage[] = [];
		for (let i = visibleMessages.length - 1; i >= 0; i--) {
			const msg = visibleMessages[i];
			if (msg.role === "toolResult") {
				recentToolResults.set(msg.toolCallId, msg);
			} else if (msg.role === "user" && recentToolResults.size === 0) {
				trailingUsers.unshift(msg);
			} else if (msg.role === "assistant") {
				break;
			}
		}

		// 分支 A：若存在因超过 165 秒守护阈值已剥离的调用，通过新 session/prompt 补发迟到结果
		if (ledger.detachedCallIds.size > 0) {
			const detachedResults: ToolResultMessage[] = [];
			for (const callId of ledger.detachedCallIds) {
				const found = recentToolResults.get(callId);
				if (found) detachedResults.push(found);
				const parked = ledger.parkedCalls.get(callId);
				parked?.settle({
					isError: true,
					content: [{ type: "text", text: "Detached" }],
				});
				ledger.parkedCalls.delete(callId);
			}
			ledger.detachedCallIds.clear();
			ledger.activePump = undefined;
			this.httpMcpBridge.clearSessionCancelled(ledger.agySessionId);

			const lateBlocks =
				detachedResults.length > 0
					? buildDetachedToolResultBlocks(detachedResults, trailingUsers)
					: buildFreshReplayPromptBlocks(visibleMessages);

			await this.executePromptOnLedger(
				ledger,
				lateBlocks,
				visibleMessages,
				writer,
				model,
				options,
			);
			return;
		}

		// 分支 B：165 秒内正常完成，直接结算挂起的 HTTP MCP 请求并继续消费同一活跃 ACP Turn
		const pump = ledger.activePump;
		if (!pump) {
			const replay = buildFreshReplayPromptBlocks(visibleMessages);
			await this.executePromptOnLedger(ledger, replay, visibleMessages, writer, model, options);
			return;
		}

		// 若用户在工具执行期间插入了实时转向消息，利用 AGY 原生运行中 Steering 发送
		if (trailingUsers.length > 0) {
			const steerBlocks = convertUserMessagesToPromptBlocks(trailingUsers);
			if (steerBlocks.length > 0) {
				void ledger.conn
					.request("session/prompt", {
						sessionId: ledger.agySessionId,
						prompt: steerBlocks,
					})
					.catch(() => {});
			}
		}

		for (const [callId, parked] of Array.from(ledger.parkedCalls.entries())) {
			const tr = recentToolResults.get(callId);
			ledger.parkedCalls.delete(callId);
			if (tr) {
				parked.settle({
					content: tr.content,
					isError: tr.isError,
				});
			} else {
				parked.settle({
					content: [{ type: "text", text: "(tool call skipped)" }],
					isError: true,
				});
			}
		}

		writer.setResponseMetadata({
			responseId: ledger.agySessionId,
			providerThinkingLevel: ledger.appliedThoughtLevel,
		});

		await this.consumePumpForSingleStep(ledger, pump, writer, model, options);
		if (!ledger.activePump && ledger.parkedCalls.size === 0) {
			this.recordSyncedTranscript(ledger, visibleMessages, writer.message);
		}
	}

	private async bootstrapOrRestoreLedger(params: {
		piSessionKey: string;
		cwd: string;
		isProjectTrusted: boolean;
		visibleMessages: readonly (UserMessage | AssistantMessage | ToolResultMessage)[];
		hostMcpTools: Tool[];
		toolsDigest: string;
		mountedPluginPaths: string[];
		mountDigest: string;
		additionalDirectories: string[];
		workspaceContextBlock?: string;
		signal?: AbortSignal;
	}): Promise<{ ledger: ManagedSessionLedger; initialPromptBlocks: AcpPromptBlock[] }> {
		const conn = await this.processManager.acquire(
			this.currentConfig,
			{ mountedPluginPaths: params.mountedPluginPaths },
			params.signal,
		);
		const useHttpMcp = Boolean(
			this.currentConfig.processPerSession && conn.capabilities?.mcpCapabilities?.http,
		);

		let mcpToken: string | undefined;
		const mcpServers: AcpMcpServerSpec[] = [];

		const ledgerDraft: Partial<ManagedSessionLedger> = {
			parkedCalls: new Map(),
			detachedCallIds: new Set(),
		};

		if (
			this.currentConfig.toolMode === "pi-tools" ||
			params.hostMcpTools.length > 0
		) {
			mcpToken = this.httpMcpBridge.registerSessionBinding({
				tools: params.hostMcpTools,
				onParkedCall: (call) => {
					const fullLedger = ledgerDraft as ManagedSessionLedger;
					fullLedger.parkedCalls.set(call.callId, call);
					fullLedger.activePump?.queue.push({ kind: "mcp_parked", call });
				},
				onHoldTimeout: async (call) => {
					const fullLedger = ledgerDraft as ManagedSessionLedger;
					fullLedger.detachedCallIds.add(call.callId);
					if (fullLedger.activePump) {
						fullLedger.activePump.cancelledByHoldTimeout = true;
					}
					if (fullLedger.agySessionId) {
						this.httpMcpBridge.markSessionTurnCancelled(fullLedger.agySessionId);
						await fullLedger.conn
							.request("session/cancel", { sessionId: fullLedger.agySessionId })
							.catch(() => {});
					}
				},
			});
			mcpServers.push(await this.httpMcpBridge.buildAcpMcpServerSpec(mcpToken, useHttpMcp));
		}

		// 在模式 A / Hybrid 且配置为 direct-config 时，追加 Pi mcp.json 配置项
		if (
			this.currentConfig.toolMode !== "pi-tools" &&
			(this.currentConfig.bridgePiMcp === true) &&
			this.currentConfig.piMcpBridgeStrategy === "direct-config"
		) {
			mcpServers.push(
				...loadDirectPiMcpServerSpecs({
					cwd: params.cwd,
					isProjectTrusted: params.isProjectTrusted,
					processPerSession: useHttpMcp,
				}),
			);
		}

		// 拆分前序历史与末尾待发送的新增用户消息，尝试沿会话分支查找锚点恢复
		let tailStartIdx = params.visibleMessages.length;
		while (
			tailStartIdx > 0 &&
			params.visibleMessages[tailStartIdx - 1].role === "user"
		) {
			tailStartIdx--;
		}
		const priorHistory = params.visibleMessages.slice(0, tailStartIdx);
		const pendingUserMessages = params.visibleMessages
			.slice(tailStartIdx)
			.filter((m): m is UserMessage => m.role === "user");

		const branchEntries = this.activeContext?.sessionManager?.getBranch?.() ?? [];
		const anchorMatch =
			priorHistory.length > 0 && pendingUserMessages.length > 0
				? findBranchAnchorForHistory({
						branchEntries,
						priorMessages: priorHistory,
						toolMode: this.currentConfig.toolMode,
						mountDigest: params.mountDigest,
						config: this.currentConfig,
					})
				: { kind: "none" as const };

		const addDirs =
			params.additionalDirectories.length > 0 ? params.additionalDirectories : undefined;

		let sessionInfo: AcpSessionNewResult | undefined;
		let initialPromptBlocks: AcpPromptBlock[] | undefined;

		// 路径 1：精确命中分支末尾锚点 -> 调用 session/resume 零历史回放开销恢复
		if (anchorMatch.kind === "exact" && conn.capabilities?.sessionCapabilities?.resume) {
			try {
				sessionInfo = await conn.request<AcpSessionResumeResult>(
					"session/resume",
					{
						sessionId: anchorMatch.anchor.agySessionId,
						cwd: params.cwd,
						mcpServers,
						additionalDirectories: addDirs,
					},
					{ timeoutMs: 20_000, signal: params.signal },
				);
				initialPromptBlocks = convertUserMessagesToPromptBlocks(pendingUserMessages);
			} catch {
				sessionInfo = undefined;
			}
		}

		// 路径 2：命中历史中间锚点 -> 只读 VACUUM INTO 快照分叉截断 + session/resume
		if (
			!sessionInfo &&
			anchorMatch.kind === "forkable" &&
			anchorMatch.anchor.dbCheckpoint &&
			conn.capabilities?.sessionCapabilities?.resume
		) {
			const forkedId = await forkSqliteConversationAtCheckpoint(
				this.currentConfig,
				anchorMatch.anchor.agySessionId,
				anchorMatch.anchor.dbCheckpoint,
			);
			if (forkedId) {
				try {
					sessionInfo = await conn.request<AcpSessionResumeResult>(
						"session/resume",
						{
							sessionId: forkedId,
							cwd: params.cwd,
							mcpServers,
							additionalDirectories: addDirs,
						},
						{ timeoutMs: 20_000, signal: params.signal },
					);
					const remainingAfterFork = params.visibleMessages.slice(
						anchorMatch.anchor.messageCount,
					);
					initialPromptBlocks = buildFreshReplayPromptBlocks(remainingAfterFork);
				} catch {
					sessionInfo = undefined;
				}
			}
		}

		// 路径 3：新建会话并发送完整结构化图文检查点重放（FreshReplay）
		if (!sessionInfo || !initialPromptBlocks) {
			try {
				sessionInfo = await conn.request<AcpSessionNewResult>(
					"session/new",
					{
						cwd: params.cwd,
						mcpServers,
						additionalDirectories: addDirs,
					},
					{ timeoutMs: 25_000, signal: params.signal },
				);
			} catch (error) {
				if (mcpToken) {
					this.httpMcpBridge.unregisterSessionBinding(mcpToken);
				}
				throw error;
			}
			const replayBlocks = buildFreshReplayPromptBlocks(params.visibleMessages);
			initialPromptBlocks = params.workspaceContextBlock
				? [{ type: "text", text: params.workspaceContextBlock }, ...replayBlocks]
				: replayBlocks;
		}

		conn.trackSession(sessionInfo.sessionId);
		if (mcpToken) {
			this.httpMcpBridge.updateBindingSessionId(mcpToken, sessionInfo.sessionId);
		}

		const tier = sessionInfo._meta?.antigravity?.tier;
		if (tier) {
			this.latestTier = tier;
		}

		const ledger: ManagedSessionLedger = Object.assign(ledgerDraft, {
			piSessionKey: params.piSessionKey,
			agySessionId: sessionInfo.sessionId,
			conn,
			mcpToken,
			hostMcpTools: params.hostMcpTools,
			toolsDigest: params.toolsDigest,
			mountDigest: params.mountDigest,
			toolMode: this.currentConfig.toolMode,
			configOptions: sessionInfo.configOptions ?? [],
			availableModels: sessionInfo.models?.availableModels ?? [],
			syncedMessageCount: priorHistory.length,
			syncedTranscriptDigest: computeTranscriptDigest(priorHistory),
			tier,
			parkedCalls: ledgerDraft.parkedCalls!,
			detachedCallIds: ledgerDraft.detachedCallIds!,
		});

		this.ledgers.set(params.piSessionKey, ledger);
		this.ledgersByAgyId.set(sessionInfo.sessionId, ledger);
		updateAntigravityStatusBar(
			this.activeContext,
			this.currentConfig,
			this.getTelemetrySnapshot(params.piSessionKey),
		);

		return { ledger, initialPromptBlocks };
	}

	private async syncSessionModelAndMode(
		ledger: ManagedSessionLedger,
		model: Model<Api>,
		options?: SimpleStreamOptions,
	): Promise<void> {
		const resolved = resolveSessionModelSelection(
			model.id,
			options?.reasoning,
			ledger.configOptions,
			ledger.availableModels,
		);

		if (ledger.appliedModelValue !== resolved.configModelValue) {
			try {
				const res = await ledger.conn.request<AcpSetConfigOptionResult>(
					"session/set_config_option",
					{
						sessionId: ledger.agySessionId,
						configId: "model",
						value: resolved.configModelValue,
					},
					{ timeoutMs: 10_000, signal: options?.signal },
				);
				if (res.configOptions) {
					ledger.configOptions = res.configOptions;
				}
				ledger.appliedModelValue = resolved.configModelValue;
			} catch {
				// 若特定渠道不支持切换配置项则保持服务端默认选择
			}
		}

		if (resolved.configThoughtLevel && ledger.appliedThoughtLevel !== resolved.configThoughtLevel) {
			// 校验切换模型后最新通告的 thought_level 候选集
			const thoughtOpt = ledger.configOptions.find((o) => o.id === "thought_level");
			const supportedSet = new Set((thoughtOpt?.options ?? []).map((c) => c.value));
			const targetEffort =
				supportedSet.size === 0 || supportedSet.has(resolved.configThoughtLevel)
					? resolved.configThoughtLevel
					: thoughtOpt?.currentValue;

			if (targetEffort) {
				try {
					const res = await ledger.conn.request<AcpSetConfigOptionResult>(
						"session/set_config_option",
						{
							sessionId: ledger.agySessionId,
							configId: "thought_level",
							value: targetEffort,
						},
						{ timeoutMs: 10_000, signal: options?.signal },
					);
					if (res.configOptions) {
						ledger.configOptions = res.configOptions;
					}
					ledger.appliedThoughtLevel = targetEffort;
				} catch {
					ledger.appliedThoughtLevel = resolved.effectiveEffort;
				}
			}
		} else if (resolved.effectiveEffort) {
			ledger.appliedThoughtLevel = resolved.effectiveEffort;
		}

		if (
			ledger.toolMode !== "pi-tools" &&
			ledger.appliedExecMode !== this.currentConfig.agyExecutionMode
		) {
			try {
				const res = await ledger.conn.request<AcpSetConfigOptionResult>(
					"session/set_config_option",
					{
						sessionId: ledger.agySessionId,
						configId: "mode",
						value: this.currentConfig.agyExecutionMode,
					},
					{ timeoutMs: 8_000, signal: options?.signal },
				);
				if (res.configOptions) {
					ledger.configOptions = res.configOptions;
				}
				ledger.appliedExecMode = this.currentConfig.agyExecutionMode;
			} catch {
				// 忽略不支持模式的 Agent
			}
		}
	}

	private async executePromptOnLedger(
		ledger: ManagedSessionLedger,
		promptBlocks: AcpPromptBlock[],
		visibleMessagesBeforeAssistant: readonly (
			| UserMessage
			| AssistantMessage
			| ToolResultMessage
		)[],
		writer: StepStreamWriter,
		model: Model<Api>,
		options?: SimpleStreamOptions,
	): Promise<void> {
		await this.syncSessionModelAndMode(ledger, model, options);

		writer.setResponseMetadata({
			responseId: ledger.agySessionId,
			providerThinkingLevel: ledger.appliedThoughtLevel,
		});

		let outboundPayload: { sessionId: string; prompt: AcpPromptBlock[] } = {
			sessionId: ledger.agySessionId,
			prompt: promptBlocks.length > 0 ? promptBlocks : [{ type: "text", text: "Continue." }],
		};

		if (options?.onPayload) {
			const replaced = await options.onPayload(outboundPayload, model);
			if (
				replaced &&
				typeof replaced === "object" &&
				"prompt" in (replaced as Record<string, unknown>)
			) {
				outboundPayload = replaced as { sessionId: string; prompt: AcpPromptBlock[] };
			}
		}

		this.httpMcpBridge.clearSessionCancelled(ledger.agySessionId);

		const pump: ActiveTurnPump = {
			queue: new AsyncEventQueue<PumpQueueItem>(),
			finished: false,
			cancelledByHoldTimeout: false,
		};
		ledger.activePump = pump;

		void ledger.conn
			.request<AcpPromptResponse>("session/prompt", outboundPayload)
			.then((response) => {
				pump.finished = true;
				pump.queue.push({ kind: "prompt_done", response });
			})
			.catch((error) => {
				pump.finished = true;
				pump.queue.push({ kind: "prompt_error", error });
			});

		if (options?.onResponse) {
			await options.onResponse(
				{
					status: 200,
					headers: { "x-acp-session-id": ledger.agySessionId },
				},
				model,
			);
		}

		writer.ensureStarted();

		await this.consumePumpForSingleStep(ledger, pump, writer, model, options);

		if (!ledger.activePump && ledger.parkedCalls.size === 0) {
			this.recordSyncedTranscript(ledger, visibleMessagesBeforeAssistant, writer.message);
		}
	}

	private async consumePumpForSingleStep(
		ledger: ManagedSessionLedger,
		pump: ActiveTurnPump,
		writer: StepStreamWriter,
		model: Model<Api>,
		options?: SimpleStreamOptions,
	): Promise<void> {
		const signal = options?.signal;

		const abortCurrentTurn = async () => {
			this.httpMcpBridge.markSessionTurnCancelled(ledger.agySessionId);
			this.mirrorBroker.abortSessionCalls(ledger.agySessionId, "Aborted");
			for (const [, parked] of ledger.parkedCalls) {
				parked.settle({
					isError: true,
					content: [{ type: "text", text: "Cancelled" }],
				});
			}
			ledger.parkedCalls.clear();
			ledger.activePump = undefined;
			await ledger.conn
				.request("session/cancel", { sessionId: ledger.agySessionId })
				.catch(() => {});
		};

		while (!writer.isTerminated) {
			let item: PumpQueueItem;
			try {
				item = await pump.queue.shift(signal);
			} catch {
				await abortCurrentTurn();
				writer.fail("aborted", "Request aborted by user");
				return;
			}

			if (item.kind === "acp_update") {
				const update = item.update;
				if (options?.onProviderStreamEvent) {
					await options.onProviderStreamEvent(update, model);
				}

				if (update.sessionUpdate === "agent_thought_chunk") {
					this.mirrorBroker.completePendingCallsImplicitly(ledger.agySessionId);
					if (update.content.type === "text") {
						writer.appendThinkingDelta(update.content.text);
					}
					continue;
				}

				if (update.sessionUpdate === "agent_message_chunk") {
					this.mirrorBroker.completePendingCallsImplicitly(ledger.agySessionId);
					if (update.content.type === "text") {
						const { quotaError } = writer.appendTextDelta(update.content.text);
						if (quotaError) {
							await abortCurrentTurn();
							writer.fail("error", `Antigravity quota exhausted (429): ${quotaError}`);
							return;
						}
					}
					continue;
				}

				if (update.sessionUpdate === "tool_call") {
					const classified = this.mirrorBroker.registerOrUpdateCall(
						ledger.agySessionId,
						update,
					);
					writer.appendToolCall({
						id: update.toolCallId,
						name: classified.mirrorToolName,
						arguments: classified.arguments,
					});

					// 短窗口合并同一帧并发到达的额外原生 tool_call
					await new Promise((r) => setTimeout(r, 15));
					while (true) {
						const nextPeek = pump.queue.peek();
						if (
							nextPeek?.kind === "acp_update" &&
							nextPeek.update.sessionUpdate === "tool_call"
						) {
							const extra = (await pump.queue.shift()) as {
								kind: "acp_update";
								update: Extract<AcpSessionUpdatePayload, { sessionUpdate: "tool_call" }>;
							};
							const extraClassified = this.mirrorBroker.registerOrUpdateCall(
								ledger.agySessionId,
								extra.update,
							);
							writer.appendToolCall({
								id: extra.update.toolCallId,
								name: extraClassified.mirrorToolName,
								arguments: extraClassified.arguments,
							});
						} else {
							break;
						}
					}

					writer.finishWithToolUse();
					return;
				}

				continue;
			}

			if (item.kind === "mcp_parked") {
				writer.appendToolCall({
					id: item.call.callId,
					name: item.call.toolName,
					arguments: item.call.arguments,
				});

				// 微批收集同一推理步内并发触发的多个 MCP 工具调用
				await new Promise((r) => setTimeout(r, 25));
				while (pump.queue.peek()?.kind === "mcp_parked") {
					const nextItem = (await pump.queue.shift()) as {
						kind: "mcp_parked";
						call: ParkedToolCall;
					};
					writer.appendToolCall({
						id: nextItem.call.callId,
						name: nextItem.call.toolName,
						arguments: nextItem.call.arguments,
					});
				}

				writer.finishWithToolUse();
				return;
			}

			if (item.kind === "prompt_done") {
				this.mirrorBroker.completePendingCallsImplicitly(ledger.agySessionId);
				ledger.activePump = undefined;

				if (ledger.windowSize) {
					const changed = recordObservedContextWindow(model.id, ledger.windowSize);
					if (changed) {
						model.contextWindow = ledger.windowSize;
					}
				}

				if (item.response.stopReason === "cancelled" && signal?.aborted) {
					writer.fail("aborted", "Request aborted");
					return;
				}
				if (item.response.stopReason === "refusal") {
					writer.fail("error", "Model refused the request");
					return;
				}

				const { quotaError } = writer.finishTurn(
					item.response.stopReason,
					item.response.usage,
				);
				if (quotaError) {
					writer.fail("error", `Antigravity quota exhausted (429): ${quotaError}`);
				}
				return;
			}

			if (item.kind === "prompt_error") {
				this.mirrorBroker.abortSessionCalls(ledger.agySessionId, "ACP prompt error");
				ledger.activePump = undefined;
				const errMsg =
					item.error instanceof Error ? item.error.message : String(item.error);
				writer.fail(signal?.aborted ? "aborted" : "error", errMsg);
				return;
			}
		}
	}

	private recordSyncedTranscript(
		ledger: ManagedSessionLedger,
		priorMessages: readonly (UserMessage | AssistantMessage | ToolResultMessage)[],
		finalAssistantMessage: AssistantMessage,
	): void {
		if (
			finalAssistantMessage.stopReason === "error" ||
			finalAssistantMessage.stopReason === "aborted"
		) {
			return;
		}
		const full = [...priorMessages, finalAssistantMessage];
		ledger.syncedMessageCount = full.length;
		ledger.syncedTranscriptDigest = computeTranscriptDigest(full);
	}

	/** 执行用完即关的无状态单轮辅助请求（用于 `/compact` 摘要等，绝不修改系统提示词且不干扰主会话） */
	private async runEphemeralAuxiliaryTurn(
		writer: StepStreamWriter,
		model: Model<Api>,
		context: TranscriptContext,
		options?: SimpleStreamOptions,
	): Promise<void> {
		const cwd = this.activeContext?.cwd ?? process.cwd();
		const conn = await this.processManager.acquire(
			this.currentConfig,
			{
				disableBuiltinTools: true,
				inheritCustomizations: false,
				inheritMcp: false,
				mountedPluginPaths: [],
				includeDefaultCustomizations: false,
			},
			options?.signal,
		);

		const sessionRes = await conn.request<AcpSessionNewResult>(
			"session/new",
			{
				cwd,
				mcpServers: [],
			},
			{ timeoutMs: 20_000, signal: options?.signal },
		);

		const tempSessionId = sessionRes.sessionId;
		const resolved = resolveSessionModelSelection(
			model.id,
			options?.reasoning,
			sessionRes.configOptions ?? [],
			sessionRes.models?.availableModels ?? [],
		);

		try {
			await conn
				.request(
					"session/set_config_option",
					{
						sessionId: tempSessionId,
						configId: "model",
						value: resolved.configModelValue,
					},
					{ timeoutMs: 8_000, signal: options?.signal },
				)
				.catch(() => {});

			const unsubscribe = this.registerAuxiliaryUpdateListener(tempSessionId, (update) => {
				if (update.sessionUpdate === "agent_thought_chunk" && update.content.type === "text") {
					writer.appendThinkingDelta(update.content.text);
				} else if (
					update.sessionUpdate === "agent_message_chunk" &&
					update.content.type === "text"
				) {
					writer.appendTextDelta(update.content.text);
				}
			});

			try {
				const visible = extractVisibleMessages(context.messages);
				const promptBlocks = buildFreshReplayPromptBlocks(visible);
				writer.ensureStarted();
				const promptRes = await conn.request<AcpPromptResponse>(
					"session/prompt",
					{
						sessionId: tempSessionId,
						prompt: promptBlocks,
					},
					{ signal: options?.signal },
				);
				writer.finishTurn(promptRes.stopReason, promptRes.usage);
			} finally {
				unsubscribe();
			}
		} finally {
			await conn.closeSessionSafe(tempSessionId);
		}
	}

	private async teardownLedger(ledger: ManagedSessionLedger): Promise<void> {
		this.ledgers.delete(ledger.piSessionKey);
		this.ledgersByAgyId.delete(ledger.agySessionId);

		for (const [, parked] of ledger.parkedCalls) {
			parked.settle({
				isError: true,
				content: [{ type: "text", text: "Session closed" }],
			});
		}
		ledger.parkedCalls.clear();
		ledger.detachedCallIds.clear();
		ledger.activePump = undefined;

		if (ledger.mcpToken) {
			this.httpMcpBridge.unregisterSessionBinding(ledger.mcpToken);
		}
		this.mirrorBroker.abortSessionCalls(ledger.agySessionId, "Session closed");
		await ledger.conn.closeSessionSafe(ledger.agySessionId);
	}
}
