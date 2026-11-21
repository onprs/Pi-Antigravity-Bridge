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

function mightBecomeFenceOrQuotaLine(lineStartFragment: string, inFenceBlock: boolean): boolean {
	const trimmed = lineStartFragment.trimStart();
	// 仅包含前导空白时，后续字符可能构成 ``` 围栏或 ⚠ 告警（限制最多暂扣 16 个前导空白）
	if (trimmed.length === 0) {
		return lineStartFragment.length <= 16;
	}
	// 跨 chunk 拆分的 ``` 围栏前缀（无论当前在代码块内外均需识别闭合/开启）
	if ("```".startsWith(trimmed) || trimmed.startsWith("```")) {
		return trimmed.length < 80;
	}
	if (inFenceBlock) {
		return false;
	}
	if (isQuotaExhaustedLine(trimmed)) {
		return true;
	}
	for (const prefix of SUSPICIOUS_LINE_PREFIXES) {
		if (prefix.startsWith(trimmed) || trimmed.startsWith(prefix)) {
			return trimmed.length < 240;
		}
	}
	return false;
}

/**
 * 流式行首状态跟踪与 429 配额软错误拦截器。
 * 严格维护 `atLineStart` 与代码块围栏状态：行中文本直接零延迟透传，不会因跨 chunk 切分把行内示例误判为行首 429 告警或漏计跨块围栏。
 */
export class QuotaSoftErrorFilter {
	private heldLine = "";
	private atLineStart = true;
	private detectedError?: string;
	private inFenceBlock = false;

	get quotaErrorMessage(): string | undefined {
		return this.detectedError;
	}

	push(chunk: string): { safeText: string; quotaError?: string } {
		if (this.detectedError) {
			return { safeText: "", quotaError: this.detectedError };
		}
		if (!chunk) {
			return { safeText: "" };
		}

		const combined = this.heldLine + chunk;
		this.heldLine = "";

		const segments = combined.split("\n");
		const safeParts: string[] = [];

		for (let i = 0; i < segments.length - 1; i++) {
			const seg = segments[i];
			if (this.atLineStart) {
				if (/^\s*```/.test(seg)) {
					this.inFenceBlock = !this.inFenceBlock;
				} else if (!this.inFenceBlock && isQuotaExhaustedLine(seg)) {
					this.detectedError = seg.replace(/^\s*⚠\s*/, "").trim();
					return {
						safeText: safeParts.join(""),
						quotaError: this.detectedError,
					};
				}
			}
			safeParts.push(`${seg}\n`);
			this.atLineStart = true;
		}

		const tail = segments[segments.length - 1];
		if (tail.length > 0) {
			if (!this.atLineStart) {
				safeParts.push(tail);
			} else if (!this.inFenceBlock && isQuotaExhaustedLine(tail)) {
				this.detectedError = tail.replace(/^\s*⚠\s*/, "").trim();
				return {
					safeText: safeParts.join(""),
					quotaError: this.detectedError,
				};
			} else if (mightBecomeFenceOrQuotaLine(tail, this.inFenceBlock)) {
				this.heldLine = tail;
			} else {
				safeParts.push(tail);
				this.atLineStart = false;
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
		if (this.atLineStart) {
			if (/^\s*```/.test(rem)) {
				this.inFenceBlock = !this.inFenceBlock;
			} else if (!this.inFenceBlock && isQuotaExhaustedLine(rem)) {
				this.detectedError = rem.replace(/^\s*⚠\s*/, "").trim();
				return { safeText: "", quotaError: this.detectedError };
			}
		}
		this.atLineStart = false;
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
	private readonly messageFilter = new QuotaSoftErrorFilter();
	private rawEstimatedOutputTokens = 0;
	private rawEstimatedThoughtTokens = 0;
	private emittedDeltaCount = 0;

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
		this.emittedDeltaCount++;

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

	/** 当前消息是否尚未发出过任何流式 delta（用于判断是否需分两帧发送首个工具 JSON 增量以供实时 TPS 采样） */
	get hasEmittedAnyDelta(): boolean {
		return this.emittedDeltaCount > 0;
	}

	/**
	 * 写入完整工具调用块。
	 * 严格保持 `toolCall.id` 与 `arguments` 从 `toolcall_start` 起恒定不变，避免 Pi TUI 因 ID 变更或空参数占位产生重复 `...` 卡片；
	 * 当 `splitDeltaWithDelayMs > 0` 时，在同一稳定 `id` 与完整 `arguments` 上分两帧发出 JSON 增量以提供跨时间窗口的流式 TPS 采样点。
	 */
	async appendToolCallStreaming(
		call: { id: string; name: string; arguments: JsonObject },
		splitDeltaWithDelayMs = 0,
	): Promise<void> {
		if (this.terminated) return;
		this.flushHeldText();
		this.ensureStarted();
		this.closeOpenBlock();

		const serializedArgs = JSON.stringify(call.arguments ?? {});
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

		if (splitDeltaWithDelayMs > 0 && serializedArgs.length >= 2) {
			const headDelta = serializedArgs.slice(0, 1);
			const tailDelta = serializedArgs.slice(1);
			this.emittedDeltaCount++;
			this.stream.push({
				type: "toolcall_delta",
				contentIndex: index,
				delta: headDelta,
				partial: this.message,
			});
			await new Promise((r) => setTimeout(r, splitDeltaWithDelayMs));
			if (this.terminated) return;
			this.emittedDeltaCount++;
			this.stream.push({
				type: "toolcall_delta",
				contentIndex: index,
				delta: tailDelta,
				partial: this.message,
			});
		} else {
			this.emittedDeltaCount++;
			this.stream.push({
				type: "toolcall_delta",
				contentIndex: index,
				delta: serializedArgs,
				partial: this.message,
			});
		}

		this.stream.push({
			type: "toolcall_end",
			contentIndex: index,
			toolCall: toolCallBlock,
			partial: this.message,
		});
	}

	appendToolCall(call: { id: string; name: string; arguments: JsonObject }): void {
		if (this.terminated) return;
		this.flushHeldText();
		this.ensureStarted();
		this.closeOpenBlock();

		const serializedArgs = JSON.stringify(call.arguments ?? {});
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
		this.emittedDeltaCount++;

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

	/** 以 `toolUse` 结束当前切片步，并支持写入单步归因 Usage（供窗口进度条、缓存命中率与单步 TPS 实时计算） */
	finishWithToolUse(stepUsage?: Usage): void {
		if (this.terminated) return;
		this.flushHeldText();
		this.ensureStarted();
		this.closeOpenBlock();

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
		this.emittedDeltaCount++;
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
