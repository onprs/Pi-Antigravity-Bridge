import type {
	Api,
	AssistantMessage,
	AssistantMessageEventStream,
	JsonObject,
	Model,
	ModelThinkingLevel,
	ThinkingContent,
	TextContent,
	ToolCall,
	Usage,
} from "@earendil-works/pi-ai";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import type { AcpPromptStopReason, AcpPromptUsage } from "../acp/protocol.ts";

/**
 * 精确匹配 AGY CLI 在配额耗尽未恢复时注入正文的告警行（必带 `⚠` 告警前缀，避免误杀正文中普通讨论 429 的说明）。
 */
const QUOTA_ERROR_LINE_REGEX =
	/^\s*⚠\s*(?:API error\s*\(attempt\s+\d+\):\s*)?RESOURCE_EXHAUSTED\s*\(code\s*429\):.*$/i;

const LEGACY_ATTEMPT_QUOTA_REGEX =
	/^\s*⚠\s*API error\s*\(attempt\s+\d+\):.*(?:RESOURCE_EXHAUSTED|code\s*429|quota|rate[_\s-]?limit)/i;

const SUSPICIOUS_LINE_PREFIXES = [
	"⚠",
	"⚠ RESOURCE_EXHAUSTED",
	"⚠ API error",
];

const CJK_CHARS_REGEX = /[\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af\uf900-\ufaff\uff00-\uffef]/g;

/** 与 Pi / OpenTUI 同源的轻量增量 Token 估算器（CJK 1 字符≈1 token，其余代码/英文≈4 字符/token） */
export function estimateTextTokens(text: string): number {
	if (!text) return 0;
	const cjkCount = text.match(CJK_CHARS_REGEX)?.length ?? 0;
	return cjkCount + (text.length - cjkCount) / 4;
}

function isQuotaExhaustedLine(line: string): boolean {
	const trimmed = line.trim();
	if (!trimmed) return false;
	return QUOTA_ERROR_LINE_REGEX.test(trimmed) || LEGACY_ATTEMPT_QUOTA_REGEX.test(trimmed);
}

function mightBecomeQuotaErrorLine(lineFragment: string): boolean {
	const trimmed = lineFragment.trimStart();
	if (trimmed.length === 0) return false;
	if (isQuotaExhaustedLine(trimmed)) return true;
	for (const prefix of SUSPICIOUS_LINE_PREFIXES) {
		if (prefix.startsWith(trimmed) || trimmed.startsWith(prefix)) {
			return trimmed.length < 240;
		}
	}
	return false;
}

/**
 * 流式行尾缓冲与 429 配额软错误拦截器。
 * 普通文本与 Markdown 代码块内部内容零延迟透传；仅当代码块外行首命中 `⚠` 告警前缀时暂扣至行尾判定。
 */
export class QuotaSoftErrorFilter {
	private heldLine = "";
	private detectedError?: string;
	private inFenceBlock = false;

	get quotaErrorMessage(): string | undefined {
		return this.detectedError;
	}

	push(chunk: string): { safeText: string; quotaError?: string } {
		if (this.detectedError) {
			return { safeText: "", quotaError: this.detectedError };
		}

		const combined = this.heldLine + chunk;
		this.heldLine = "";

		const lines = combined.split("\n");
		const safeParts: string[] = [];

		for (let i = 0; i < lines.length - 1; i++) {
			const fullLine = lines[i];
			if (/^\s*```/.test(fullLine)) {
				this.inFenceBlock = !this.inFenceBlock;
			}
			if (!this.inFenceBlock && isQuotaExhaustedLine(fullLine)) {
				this.detectedError = fullLine.replace(/^\s*⚠\s*/, "").trim();
				return {
					safeText: safeParts.join(""),
					quotaError: this.detectedError,
				};
			}
			safeParts.push(`${fullLine}\n`);
		}

		const tail = lines[lines.length - 1];
		if (tail.length > 0) {
			if (!this.inFenceBlock && isQuotaExhaustedLine(tail)) {
				this.detectedError = tail.replace(/^\s*⚠\s*/, "").trim();
				return {
					safeText: safeParts.join(""),
					quotaError: this.detectedError,
				};
			}
			if (!this.inFenceBlock && mightBecomeQuotaErrorLine(tail)) {
				this.heldLine = tail;
			} else {
				safeParts.push(tail);
			}
		}

		return { safeText: safeParts.join("") };
	}

	flush(): { safeText: string; quotaError?: string } {
		if (this.detectedError) {
			this.heldLine = "";
			return { safeText: "", quotaError: this.detectedError };
		}
		const rem = this.heldLine;
		this.heldLine = "";
		if (!rem) {
			return { safeText: "" };
		}
		if (!this.inFenceBlock && isQuotaExhaustedLine(rem)) {
			this.detectedError = rem.replace(/^\s*⚠\s*/, "").trim();
			return { safeText: "", quotaError: this.detectedError };
		}
		return { safeText: rem };
	}
}

export function createZeroUsage(): Usage {
	return {
		input: 0,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens: 0,
		cost: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			total: 0,
		},
	};
}

/** 将 ACP `PromptResponse.usage` 转换为 Pi 标准 `Usage` 结构 */
export function convertAcpUsageToPiUsage(acpUsage?: AcpPromptUsage): Usage {
	if (!acpUsage) {
		return createZeroUsage();
	}
	const input = Math.max(0, Math.floor(acpUsage.inputTokens ?? 0));
	const cacheRead = Math.max(0, Math.floor(acpUsage.cachedReadTokens ?? 0));
	const cacheWrite = Math.max(0, Math.floor(acpUsage.cachedWriteTokens ?? 0));
	const rawOutput = Math.max(0, Math.floor(acpUsage.outputTokens ?? 0));
	const thought =
		acpUsage.thoughtTokens !== undefined
			? Math.max(0, Math.floor(acpUsage.thoughtTokens))
			: undefined;

	// Pi 规范规定 Usage.output 包含 reasoning 子集；若上游汇报 thoughtTokens 大于 outputTokens 则安全对齐上限
	const output = thought !== undefined ? Math.max(rawOutput, thought) : rawOutput;
	const totalTokens = input + output + cacheRead + cacheWrite;

	return {
		input,
		output,
		cacheRead,
		cacheWrite,
		reasoning: thought,
		totalTokens,
		cost: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			total: 0,
		},
	};
}

interface PendingToolCallSlot {
	contentIndex: number;
	block: ToolCall;
	hintName: string;
}

/**
 * 单个 Pi 推理步（`AssistantMessageEventStream`）状态机写出器。
 * 严格保证 `start` 先于一切增量事件发出、块开启与闭合配对平衡，且最终仅发出一次 `done` 或 `error`。
 */
export class StepStreamWriter {
	readonly stream: AssistantMessageEventStream;
	readonly message: AssistantMessage;
	private started = false;
	private terminated = false;
	private openBlock?: { kind: "thinking" | "text"; index: number };
	private readonly pendingToolCallSlots: PendingToolCallSlot[] = [];
	private readonly messageFilter = new QuotaSoftErrorFilter();
	private rawEstimatedOutputTokens = 0;
	private rawEstimatedThoughtTokens = 0;

	constructor(
		model: Model<Api>,
		options: {
			responseId?: string;
			providerThinkingLevel?: string;
			thinkingLevel?: ModelThinkingLevel;
		} = {},
	) {
		this.stream = createAssistantMessageEventStream();
		this.message = {
			role: "assistant",
			content: [],
			api: model.api,
			provider: model.provider,
			model: model.id,
			responseModel: model.id,
			responseId: options.responseId,
			providerThinkingLevel: options.providerThinkingLevel,
			thinkingLevel: options.thinkingLevel,
			usage: createZeroUsage(),
			stopReason: "pending",
			timestamp: Date.now(),
		};
	}

	get isTerminated(): boolean {
		return this.terminated;
	}

	/** 当前单步流式产生的输出 Token 估算值（含思考、正文与工具调用参数） */
	get estimatedOutputTokens(): number {
		return Math.ceil(this.rawEstimatedOutputTokens);
	}

	/** 当前单步流式产生的思考 Token 估算值 */
	get estimatedThoughtTokens(): number {
		return Math.ceil(this.rawEstimatedThoughtTokens);
	}

	setResponseMetadata(meta: { responseId?: string; providerThinkingLevel?: string }): void {
		if (meta.responseId !== undefined) {
			this.message.responseId = meta.responseId;
		}
		if (meta.providerThinkingLevel !== undefined) {
			this.message.providerThinkingLevel = meta.providerThinkingLevel;
		}
	}

	ensureStarted(): void {
		if (this.started || this.terminated) return;
		this.started = true;
		this.stream.push({
			type: "start",
			partial: this.message,
		});
	}

	appendThinkingDelta(delta: string): void {
		if (this.terminated || delta.length === 0) return;
		this.ensureStarted();

		if (this.openBlock && this.openBlock.kind !== "thinking") {
			this.closeOpenBlock();
		}

		if (!this.openBlock) {
			const index = this.message.content.length;
			const block: ThinkingContent = { type: "thinking", thinking: "" };
			this.message.content.push(block);
			this.openBlock = { kind: "thinking", index };
			this.stream.push({
				type: "thinking_start",
				contentIndex: index,
				partial: this.message,
			});
		}

		const est = estimateTextTokens(delta);
		this.rawEstimatedThoughtTokens += est;
		this.rawEstimatedOutputTokens += est;

		const target = this.message.content[this.openBlock.index] as ThinkingContent;
		target.thinking += delta;
		this.stream.push({
			type: "thinking_delta",
			contentIndex: this.openBlock.index,
			delta,
			partial: this.message,
		});
	}

	/** 追加模型正文增量，返回值若包含 `quotaError` 表示捕获到 429 重试软错误 */
	appendTextDelta(rawDelta: string): { quotaError?: string } {
		if (this.terminated || rawDelta.length === 0) return {};

		const { safeText, quotaError } = this.messageFilter.push(rawDelta);
		if (safeText.length > 0) {
			this.emitRawText(safeText);
		}
		return { quotaError };
	}

	/**
	 * 在收到 ACP `tool_call` 前置宣告（模型刚开始生成 MCP 工具调用参数）时提前开启 `toolcall_start` 并推首字节 `"{"`，
	 * 使下游 TPS 追踪器在工具参数生成起点即获得首个流式时间采样点。
	 */
	beginPendingToolCall(placeholderId: string, hintName: string): void {
		if (this.terminated) return;
		this.flushHeldText();
		this.ensureStarted();
		this.closeOpenBlock();

		const index = this.message.content.length;
		const block: ToolCall = {
			type: "toolCall",
			id: placeholderId,
			name: hintName,
			arguments: {},
		};
		this.message.content.push(block);
		this.pendingToolCallSlots.push({
			contentIndex: index,
			block,
			hintName,
		});

		this.rawEstimatedOutputTokens += Math.max(1, estimateTextTokens(hintName) + 1);

		this.stream.push({
			type: "toolcall_start",
			contentIndex: index,
			partial: this.message,
		});
		this.stream.push({
			type: "toolcall_delta",
			contentIndex: index,
			delta: "{",
			partial: this.message,
		});
	}

	/**
	 * 完成先前已开启的挂起工具块（发出剩余 JSON `toolcall_delta` 与 `toolcall_end`），
	 * 若无匹配的前置挂起块则直接完整发出 `toolcall_start -> toolcall_delta -> toolcall_end`。
	 */
	appendToolCall(call: { id: string; name: string; arguments: JsonObject }): void {
		if (this.terminated) return;
		this.flushHeldText();
		this.ensureStarted();
		this.closeOpenBlock();

		const serializedArgs = JSON.stringify(call.arguments ?? {});
		const matchIdx = this.pendingToolCallSlots.findIndex(
			(s) =>
				s.hintName === call.name ||
				s.hintName.toLowerCase().includes(call.name.toLowerCase()) ||
				call.name.toLowerCase().includes(s.hintName.toLowerCase()),
		);
		const slot =
			matchIdx !== -1
				? this.pendingToolCallSlots.splice(matchIdx, 1)[0]
				: this.pendingToolCallSlots.shift();

		if (slot) {
			slot.block.id = call.id;
			slot.block.name = call.name;
			slot.block.arguments = call.arguments;
			const tailDelta = serializedArgs.startsWith("{")
				? serializedArgs.slice(1)
				: serializedArgs;
			if (tailDelta.length > 0) {
				this.rawEstimatedOutputTokens += estimateTextTokens(tailDelta);
				this.stream.push({
					type: "toolcall_delta",
					contentIndex: slot.contentIndex,
					delta: tailDelta,
					partial: this.message,
				});
			}
			this.stream.push({
				type: "toolcall_end",
				contentIndex: slot.contentIndex,
				toolCall: slot.block,
				partial: this.message,
			});
			return;
		}

		const index = this.message.content.length;
		const toolCallBlock: ToolCall = {
			type: "toolCall",
			id: call.id,
			name: call.name,
			arguments: call.arguments,
		};
		this.message.content.push(toolCallBlock);

		this.rawEstimatedOutputTokens += Math.max(
			1,
			estimateTextTokens(call.name) + estimateTextTokens(serializedArgs),
		);

		this.stream.push({
			type: "toolcall_start",
			contentIndex: index,
			partial: this.message,
		});
		this.stream.push({
			type: "toolcall_delta",
			contentIndex: index,
			delta: serializedArgs,
			partial: this.message,
		});
		this.stream.push({
			type: "toolcall_end",
			contentIndex: index,
			toolCall: toolCallBlock,
			partial: this.message,
		});
	}

	/** 清理尚未收到实际 MCP 调用即被取消的悬空前置工具块 */
	private finalizeUnmatchedPendingToolCalls(): void {
		while (this.pendingToolCallSlots.length > 0) {
			const slot = this.pendingToolCallSlots.shift()!;
			this.stream.push({
				type: "toolcall_delta",
				contentIndex: slot.contentIndex,
				delta: "}",
				partial: this.message,
			});
			this.stream.push({
				type: "toolcall_end",
				contentIndex: slot.contentIndex,
				toolCall: slot.block,
				partial: this.message,
			});
		}
	}

	/** 以 `toolUse` 结束当前切片步，并支持写入单步归因 Usage（供窗口进度条、缓存命中率与单步 TPS 实时计算） */
	finishWithToolUse(stepUsage?: Usage): void {
		if (this.terminated) return;
		this.flushHeldText();
		this.ensureStarted();
		this.closeOpenBlock();
		this.finalizeUnmatchedPendingToolCalls();

		if (stepUsage) {
			this.message.usage = stepUsage;
		}
		this.message.stopReason = "toolUse";
		this.terminated = true;
		this.stream.push({
			type: "done",
			reason: "toolUse",
			message: this.message,
		});
		this.stream.end(this.message);
	}

	/** 正常结束整个模型回答轮次并写入终态 Token 用量（支持直接传入校准后的 `Usage` 或原始 `AcpPromptUsage`） */
	finishTurn(
		acpStopReason: AcpPromptStopReason,
		usageOrRaw?: AcpPromptUsage | Usage,
	): { quotaError?: string } {
		if (this.terminated) return {};
		const flushed = this.flushHeldText();
		if (flushed.quotaError) {
			return { quotaError: flushed.quotaError };
		}

		this.ensureStarted();
		this.closeOpenBlock();
		this.finalizeUnmatchedPendingToolCalls();

		const reason = acpStopReason === "max_tokens" ? "length" : "stop";
		this.message.stopReason = reason;
		this.message.rawStopReason = acpStopReason;
		this.message.endTurn = reason === "stop";

		if (usageOrRaw && "cost" in usageOrRaw && "cacheRead" in usageOrRaw) {
			this.message.usage = usageOrRaw;
		} else {
			this.message.usage = convertAcpUsageToPiUsage(usageOrRaw as AcpPromptUsage | undefined);
		}

		this.terminated = true;
		this.stream.push({
			type: "done",
			reason,
			message: this.message,
		});
		this.stream.end(this.message);
		return {};
	}

	/** 以错误或用户中断结束当前流 */
	fail(reason: "aborted" | "error", errorMessage: string): void {
		if (this.terminated) return;
		this.closeOpenBlock();
		this.finalizeUnmatchedPendingToolCalls();
		this.message.stopReason = reason;
		this.message.errorMessage = errorMessage;

		this.terminated = true;
		this.stream.push({
			type: "error",
			reason,
			error: this.message,
		});
		this.stream.end(this.message);
	}

	private flushHeldText(): { quotaError?: string } {
		const { safeText, quotaError } = this.messageFilter.flush();
		if (safeText.length > 0) {
			this.emitRawText(safeText);
		}
		return { quotaError };
	}

	private emitRawText(text: string): void {
		this.ensureStarted();
		if (this.openBlock && this.openBlock.kind !== "text") {
			this.closeOpenBlock();
		}
		if (!this.openBlock) {
			const index = this.message.content.length;
			const block: TextContent = { type: "text", text: "" };
			this.message.content.push(block);
			this.openBlock = { kind: "text", index };
			this.stream.push({
				type: "text_start",
				contentIndex: index,
				partial: this.message,
			});
		}
		this.rawEstimatedOutputTokens += estimateTextTokens(text);
		const target = this.message.content[this.openBlock.index] as TextContent;
		target.text += text;
		this.stream.push({
			type: "text_delta",
			contentIndex: this.openBlock.index,
			delta: text,
			partial: this.message,
		});
	}

	private closeOpenBlock(): void {
		if (!this.openBlock) return;
		const { kind, index } = this.openBlock;
		this.openBlock = undefined;

		if (kind === "thinking") {
			const block = this.message.content[index] as ThinkingContent;
			this.stream.push({
				type: "thinking_end",
				contentIndex: index,
				content: block.thinking,
				partial: this.message,
			});
		} else {
			const block = this.message.content[index] as TextContent;
			this.stream.push({
				type: "text_end",
				contentIndex: index,
				content: block.text,
				partial: this.message,
			});
		}
	}
}
