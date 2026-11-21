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

const QUOTA_ERROR_LINE_REGEX =
	/^(?:\s*⚠\s*)?(?:API error\s*\(attempt\s+\d+\):\s*)?RESOURCE_EXHAUSTED\s*\(code\s*429\):.*$/i;

const LEGACY_ATTEMPT_QUOTA_REGEX =
	/(?:⚠\s*)?API error\s*\(attempt\s+\d+\):.*(?:RESOURCE_EXHAUSTED|code\s*429|quota|rate[_\s-]?limit)/i;

const SUSPICIOUS_LINE_PREFIXES = [
	"⚠",
	"⚠ RESOURCE_EXHAUSTED",
	"RESOURCE_EXHAUSTED (",
	"⚠ API error",
	"API error (",
];

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
 * 普通文本零延迟透传；仅当行首命中 `⚠ API error (attempt` 可疑前缀时暂扣至行尾判定，
 * 一旦确认为 `RESOURCE_EXHAUSTED`（429），立即阻断脏文本进入聊天流并上报标准配额错误。
 */
export class QuotaSoftErrorFilter {
	private heldLine = "";
	private detectedError?: string;

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
			if (isQuotaExhaustedLine(fullLine)) {
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
			if (isQuotaExhaustedLine(tail)) {
				this.detectedError = tail.replace(/^\s*⚠\s*/, "").trim();
				return {
					safeText: safeParts.join(""),
					quotaError: this.detectedError,
				};
			}
			if (mightBecomeQuotaErrorLine(tail)) {
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
		if (isQuotaExhaustedLine(rem)) {
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

	appendToolCall(call: { id: string; name: string; arguments: JsonObject }): void {
		if (this.terminated) return;
		this.flushHeldText();
		this.ensureStarted();
		this.closeOpenBlock();

		const index = this.message.content.length;
		const toolCallBlock: ToolCall = {
			type: "toolCall",
			id: call.id,
			name: call.name,
			arguments: call.arguments,
		};
		this.message.content.push(toolCallBlock);

		this.stream.push({
			type: "toolcall_start",
			contentIndex: index,
			partial: this.message,
		});
		const serializedArgs = JSON.stringify(call.arguments);
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

	/** 以 `toolUse` 结束当前切片步（中间步保持账单 Usage 为 0，避免多步重复累加） */
	finishWithToolUse(): void {
		if (this.terminated) return;
		this.flushHeldText();
		this.ensureStarted();
		this.closeOpenBlock();

		this.message.stopReason = "toolUse";
		this.terminated = true;
		this.stream.push({
			type: "done",
			reason: "toolUse",
			message: this.message,
		});
		this.stream.end(this.message);
	}

	/** 正常结束整个模型回答轮次并写入终态 Token 用量 */
	finishTurn(acpStopReason: AcpPromptStopReason, acpUsage?: AcpPromptUsage): { quotaError?: string } {
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
		this.message.usage = convertAcpUsageToPiUsage(acpUsage);

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
