import * as crypto from "node:crypto";
import {
	getCurrentSystemPrompt,
	getCurrentTools,
	type AssistantMessage,
	type ImageContent,
	type Message,
	type SimpleStreamOptions,
	type TextContent,
	type ToolResultMessage,
	type TranscriptContext,
	type UserMessage,
	withoutInitialSystemMessage,
} from "@earendil-works/pi-ai";
import type { AcpPromptBlock } from "../acp/protocol.ts";

/** 提取去除一切 `system` 消息后的模型可见对话序列 */
export function extractVisibleMessages(
	messages: readonly Message[],
): Array<UserMessage | AssistantMessage | ToolResultMessage> {
	const stripped = withoutInitialSystemMessage([...messages]);
	return stripped.filter(
		(m): m is UserMessage | AssistantMessage | ToolResultMessage => m.role !== "system",
	);
}

/**
 * 提取对话结构身份特征用于会话连续性与分支锚点校验。
 * 对 `ToolResultMessage` 与历史图片块仅采集不可变结构标识（`toolCallId`、`toolName`、`isError` 与图片占位标记），
 * 确保 `pi-traffic-optimization`（工具输出归档投影）或 `pi-orca-images`（历史图片重压缩）等 `context` 阶段钩子改写载荷后，
 * 仍能 100% 精确命中活跃会话与历史锚点，绝不误拆健康热缓存会话。
 */
function canonicalizeMessage(msg: UserMessage | AssistantMessage | ToolResultMessage): unknown {
	if (msg.role === "user") {
		if (typeof msg.content === "string") {
			return { role: "user", text: msg.content };
		}
		return {
			role: "user",
			parts: msg.content.map((part) =>
				part.type === "image"
					? { type: "image", mimeType: part.mimeType }
					: { type: "text", text: part.text },
			),
		};
	}

	if (msg.role === "assistant") {
		return {
			role: "assistant",
			parts: msg.content.map((part) => {
				if (part.type === "text") {
					return { type: "text", text: part.text };
				}
				if (part.type === "thinking") {
					return { type: "thinking", thinking: part.thinking };
				}
				return {
					type: "toolCall",
					id: part.id,
					name: part.name,
					arguments: part.arguments,
				};
			}),
		};
	}

	return {
		role: "toolResult",
		toolCallId: msg.toolCallId,
		toolName: msg.toolName,
		isError: msg.isError,
	};
}

/**
 * 计算有序可见消息列表的确定性 SHA-256 摘要（忽略时间戳与统计噪音）。
 */
export function computeTranscriptDigest(
	visibleMessages: readonly (UserMessage | AssistantMessage | ToolResultMessage)[],
): string {
	const hash = crypto.createHash("sha256");
	for (const msg of visibleMessages) {
		hash.update(`${JSON.stringify(canonicalizeMessage(msg))}\n`);
	}
	return hash.digest("hex");
}

const SUMMARY_USER_HINT_PATTERN =
	/(?:<conversation>|summarize\s+(?:the|this)\s+(?:conversation|session|branch)|for\s+compaction|compaction\s+summary|context\s+summary|structured\s+checkpoint|update\s+the\s+previous\s+summary|create\s+a\s+(?:structured\s+)?checkpoint)/i;

const SUMMARY_SYSTEM_HINT_PATTERN =
	/(?:context\s+summarization\s+assistant|do\s+not\s+continue\s+the\s+conversation|summarize\s+the\s+conversation)/i;

/**
 * 判定当前请求是否为后台单轮无工具辅助请求（例如 `/compact` 生成摘要、`/tree` 分支总结或缓存预热）。
 * 辅助请求将分配独立临时 ACP Session，避免污染主会话状态；同时避免误伤用户在 `--no-tools` 下首轮普通的总结类提问。
 */
export function isAuxiliarySingleTurnRequest(
	context: TranscriptContext,
	options: SimpleStreamOptions | undefined,
	hasBusyPrimaryTurn: boolean,
): boolean {
	if (options?.disableContinuation === true) {
		return true;
	}
	if (options?.toolChoice === "none") {
		return true;
	}

	const tools = getCurrentTools(context.messages);
	const visible = extractVisibleMessages(context.messages);

	if (tools.length === 0 && visible.length <= 2) {
		const sysText = getCurrentSystemPrompt(context.messages);
		if (sysText && SUMMARY_SYSTEM_HINT_PATTERN.test(sysText)) {
			return true;
		}
		const last = visible[visible.length - 1];
		if (last?.role === "user") {
			const text =
				typeof last.content === "string"
					? last.content
					: last.content
							.filter((c): c is TextContent => c.type === "text")
							.map((c) => c.text)
							.join("\n");
			if (SUMMARY_USER_HINT_PATTERN.test(text)) {
				return true;
			}
		}
		if (hasBusyPrimaryTurn) {
			return true;
		}
	}

	return false;
}

/**
 * 为后台无状态辅助请求（如 `/compact`、`/tree` 分支摘要）提取宿主传入的任务说明文本，
 * 通过用户消息块传递给临时会话，确保摘要约束与输出格式生效且不修改任何官方系统提示词。
 */
export function extractAuxiliaryTaskInstructions(context: TranscriptContext): string | undefined {
	const raw = getCurrentSystemPrompt(context.messages)?.trim();
	if (!raw) return undefined;
	if (raw.length > 8_000) {
		return raw.slice(0, 8_000);
	}
	return raw;
}

/** 将单条或多条新增的 `UserMessage` 转换为 ACP `PromptBlock[]` */
export function convertUserMessagesToPromptBlocks(
	messages: readonly UserMessage[],
): AcpPromptBlock[] {
	const blocks: AcpPromptBlock[] = [];
	for (const msg of messages) {
		if (typeof msg.content === "string") {
			if (msg.content.length > 0) {
				blocks.push({ type: "text", text: msg.content });
			}
			continue;
		}
		for (const part of msg.content) {
			if (part.type === "text") {
				if (part.text.length > 0) {
					blocks.push({ type: "text", text: part.text });
				}
			} else if (part.type === "image") {
				blocks.push({
					type: "image",
					data: part.data,
					mimeType: part.mimeType,
				});
			}
		}
	}
	return blocks;
}

/**
 * 为超过 165 秒预取消剥离（Detached）的工具调用构造迟到结果补发 Prompt。
 * 保留文本与图片多模态内容，并指示模型继续后续步骤而无需重复调用同一工具。
 */
export function buildDetachedToolResultBlocks(
	results: readonly ToolResultMessage[],
	trailingUserMessages: readonly UserMessage[] = [],
): AcpPromptBlock[] {
	const blocks: AcpPromptBlock[] = [];
	const introLines = [
		"The previously initiated tool execution(s) finished after the MCP hold timeout.",
		"Below are the actual tool execution results. Treat these calls as completed and continue from here without invoking the same tool call again:",
		"",
	];
	blocks.push({ type: "text", text: introLines.join("\n") });

	for (const res of results) {
		const statusAttr = res.isError ? "error" : "ok";
		const textParts: string[] = [];
		const imageParts: ImageContent[] = [];

		for (const item of res.content) {
			if (item.type === "text") {
				textParts.push(item.text);
			} else if (item.type === "image") {
				imageParts.push(item);
			}
		}

		blocks.push({
			type: "text",
			text: `<tool_result id="${res.toolCallId}" tool="${res.toolName}" status="${statusAttr}">\n${textParts.join("\n")}\n</tool_result>`,
		});

		for (const img of imageParts) {
			blocks.push({
				type: "image",
				data: img.data,
				mimeType: img.mimeType,
			});
		}
	}

	if (trailingUserMessages.length > 0) {
		blocks.push(...convertUserMessagesToPromptBlocks(trailingUserMessages));
	}

	return blocks;
}

/** 历史检查点重放文本最大 UTF-8 字节预算（严防超出后端单次 Prompt 上限导致尾部最新用户提问被硬截断） */
export const MAX_FRESH_REPLAY_HISTORY_BYTES = 120_000;
/** 历史检查点重放最多保留的近期历史图片数（更早图片转为占位文本，防单行 JSON-RPC 体积失控） */
const MAX_FRESH_REPLAY_HISTORY_IMAGES = 6;
const MAX_SINGLE_REPLAY_TOOL_TEXT_BYTES = 4_000;
const MAX_SINGLE_REPLAY_MESSAGE_TEXT_BYTES = 16_000;

function truncateUtf8Middle(text: string, maxBytes: number, marker: string): string {
	const totalBytes = Buffer.byteLength(text, "utf8");
	if (totalBytes <= maxBytes) {
		return text;
	}
	const markerBytes = Buffer.byteLength(marker, "utf8");
	const budget = Math.max(128, maxBytes - markerBytes);
	const headBudget = Math.floor(budget * 0.65);
	const tailBudget = Math.max(64, budget - headBudget);
	const buf = Buffer.from(text, "utf8");
	// 使用 TextDecoder 自动处理多字节 UTF-8 边界，避免产生残缺代理对
	const decoder = new TextDecoder("utf-8", { fatal: false });
	const headStr = decoder.decode(buf.subarray(0, headBudget));
	const tailStr = decoder.decode(buf.subarray(buf.length - tailBudget));
	return `${headStr}${marker}${tailStr}`;
}

function serializeReplayHistoryEntry(
	msg: UserMessage | AssistantMessage | ToolResultMessage,
): { blocks: AcpPromptBlock[]; textBytes: number } {
	const entryBlocks: AcpPromptBlock[] = [];

	if (msg.role === "user") {
		if (typeof msg.content === "string") {
			const trimmed = truncateUtf8Middle(
				msg.content,
				MAX_SINGLE_REPLAY_MESSAGE_TEXT_BYTES,
				"\n...[earlier user text truncated in replay]...\n",
			);
			entryBlocks.push({ type: "text", text: `<user>\n${trimmed}\n</user>` });
		} else {
			entryBlocks.push({ type: "text", text: "<user>" });
			for (const part of msg.content) {
				if (part.type === "text") {
					const trimmed = truncateUtf8Middle(
						part.text,
						MAX_SINGLE_REPLAY_MESSAGE_TEXT_BYTES,
						"\n...[earlier user text truncated in replay]...\n",
					);
					entryBlocks.push({ type: "text", text: trimmed });
				} else if (part.type === "image") {
					entryBlocks.push({ type: "image", data: part.data, mimeType: part.mimeType });
				}
			}
			entryBlocks.push({ type: "text", text: "</user>" });
		}
	} else if (msg.role === "assistant") {
		const segs: string[] = [];
		for (const part of msg.content) {
			if (part.type === "text" && part.text.trim()) {
				segs.push(
					truncateUtf8Middle(
						part.text,
						MAX_SINGLE_REPLAY_MESSAGE_TEXT_BYTES,
						"\n...[assistant text truncated in replay]...\n",
					),
				);
			} else if (part.type === "toolCall") {
				const rawArgs = JSON.stringify(part.arguments);
				const safeArgs = truncateUtf8Middle(rawArgs, 2_000, "...[truncated]...");
				segs.push(`<tool_call id="${part.id}" name="${part.name}">${safeArgs}</tool_call>`);
			}
		}
		if (segs.length > 0) {
			entryBlocks.push({
				type: "text",
				text: `<assistant model="${msg.model}">\n${segs.join("\n")}\n</assistant>`,
			});
		}
	} else if (msg.role === "toolResult") {
		const status = msg.isError ? "error" : "ok";
		const rawTexts = msg.content
			.filter((c): c is TextContent => c.type === "text")
			.map((c) => c.text)
			.join("\n");
		const cappedTexts = truncateUtf8Middle(
			rawTexts,
			MAX_SINGLE_REPLAY_TOOL_TEXT_BYTES,
			"\n...[tool output truncated in replay]...\n",
		);
		entryBlocks.push({
			type: "text",
			text: `<tool_result id="${msg.toolCallId}" tool="${msg.toolName}" status="${status}">\n${cappedTexts}\n</tool_result>`,
		});
		for (const part of msg.content) {
			if (part.type === "image") {
				entryBlocks.push({ type: "image", data: part.data, mimeType: part.mimeType });
			}
		}
	}

	let textBytes = 0;
	for (const b of entryBlocks) {
		if (b.type === "text") {
			textBytes += Buffer.byteLength(b.text, "utf8") + 1;
		}
	}
	return { blocks: entryBlocks, textBytes };
}

/**
 * 构建完整结构化历史重放（`FreshReplay`）的 ACP Prompt 块序列。
 * 内置字节预算保护：从最近的历史消息向前逆序纳管，超出预算时优先裁切最早的旧历史与超长工具输出，
 * 100% 保证最近上下文与末尾最新用户提问永不被后端截断。
 */
export function buildFreshReplayPromptBlocks(
	visibleMessages: readonly (UserMessage | AssistantMessage | ToolResultMessage)[],
	maxHistoryBytes = MAX_FRESH_REPLAY_HISTORY_BYTES,
): AcpPromptBlock[] {
	if (visibleMessages.length === 0) {
		return [{ type: "text", text: "Continue." }];
	}

	// 若仅有单轮用户消息，直接原样发送无需包裹检查点标签
	const allUsers = visibleMessages.every((m) => m.role === "user");
	if (allUsers) {
		return convertUserMessagesToPromptBlocks(visibleMessages as UserMessage[]);
	}

	// 拆分前序历史上下文与末尾最新待响应的用户消息切片
	let splitIndex = visibleMessages.length;
	while (splitIndex > 0 && visibleMessages[splitIndex - 1].role === "user") {
		splitIndex--;
	}

	const historySlice =
		splitIndex > 0 ? visibleMessages.slice(0, splitIndex) : visibleMessages.slice(0, -1);
	const activeTail =
		splitIndex < visibleMessages.length
			? visibleMessages.slice(splitIndex)
			: [visibleMessages[visibleMessages.length - 1]];

	const blocks: AcpPromptBlock[] = [];

	if (historySlice.length > 0) {
		const selectedReversed: AcpPromptBlock[][] = [];
		let usedHistoryBytes = 0;
		let usedHistoryImages = 0;
		let omittedCount = 0;

		for (let i = historySlice.length - 1; i >= 0; i--) {
			const serialized = serializeReplayHistoryEntry(historySlice[i]);
			if (serialized.blocks.length === 0) {
				continue;
			}
			if (
				selectedReversed.length > 0 &&
				usedHistoryBytes + serialized.textBytes > maxHistoryBytes
			) {
				omittedCount = i + 1;
				break;
			}
			const cappedBlocks: AcpPromptBlock[] = serialized.blocks.map((block) => {
				if (block.type !== "image") return block;
				if (usedHistoryImages < MAX_FRESH_REPLAY_HISTORY_IMAGES) {
					usedHistoryImages++;
					return block;
				}
				return {
					type: "text",
					text: `[earlier ${block.mimeType} image omitted in replay]`,
				};
			});
			selectedReversed.push(cappedBlocks);
			usedHistoryBytes += serialized.textBytes;
		}

		blocks.push({
			type: "text",
			text: "<conversation-checkpoint>\nThe following is the prior conversation transcript from this session (preserved across model switch, compaction, or branch navigation):",
		});

		if (omittedCount > 0) {
			blocks.push({
				type: "text",
				text: `[Note: ${omittedCount} earlier historical messages were omitted to stay within the prompt replay size budget.]`,
			});
		}

		for (let i = selectedReversed.length - 1; i >= 0; i--) {
			blocks.push(...selectedReversed[i]);
		}

		blocks.push({
			type: "text",
			text: "</conversation-checkpoint>\n\nPlease respond to the latest message(s) below based on the checkpoint context above:",
		});
	}

	const tailUsers = activeTail.filter((m): m is UserMessage => m.role === "user");
	if (tailUsers.length > 0) {
		blocks.push(...convertUserMessagesToPromptBlocks(tailUsers));
	} else {
		const tailToolResults = activeTail.filter(
			(m): m is ToolResultMessage => m.role === "toolResult",
		);
		if (tailToolResults.length > 0) {
			blocks.push(...buildDetachedToolResultBlocks(tailToolResults));
		} else {
			blocks.push({ type: "text", text: "Please continue." });
		}
	}

	return blocks;
}
