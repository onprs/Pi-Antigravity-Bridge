import * as crypto from "node:crypto";
import {
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

function hashImageData(img: ImageContent): string {
	return crypto.createHash("sha256").update(`${img.mimeType}:${img.data}`).digest("hex").slice(0, 24);
}

function canonicalizeMessage(msg: UserMessage | AssistantMessage | ToolResultMessage): unknown {
	if (msg.role === "user") {
		if (typeof msg.content === "string") {
			return { role: "user", text: msg.content };
		}
		return {
			role: "user",
			parts: msg.content.map((part) =>
				part.type === "image"
					? { type: "image", hash: hashImageData(part) }
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
		parts: msg.content.map((part) =>
			part.type === "image"
				? { type: "image", hash: hashImageData(part) }
				: { type: "text", text: part.text },
		),
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

const SUMMARY_HINT_PATTERN =
	/(?:summarize|summary|compaction|checkpoint|context\s+summary|<conversation>)/i;

/**
 * 判定当前请求是否为后台单轮无工具辅助请求（例如 `/compact` 生成摘要、`/tree` 分支总结或缓存预热）。
 * 辅助请求将分配独立临时 ACP Session，避免污染主会话状态。
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
		const last = visible[visible.length - 1];
		if (last?.role === "user") {
			const text =
				typeof last.content === "string"
					? last.content
					: last.content
							.filter((c): c is TextContent => c.type === "text")
							.map((c) => c.text)
							.join("\n");
			if (SUMMARY_HINT_PATTERN.test(text)) {
				return true;
			}
		}
		if (hasBusyPrimaryTurn) {
			return true;
		}
	}

	return false;
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

/**
 * 构建完整结构化历史重放（`FreshReplay`）的 ACP Prompt 块序列。
 * 用于跨 Provider 切入、无本地快照的分支回退及上下文压缩后的新会话冷启动，完整保留历史图片块。
 */
export function buildFreshReplayPromptBlocks(
	visibleMessages: readonly (UserMessage | AssistantMessage | ToolResultMessage)[],
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
		blocks.push({
			type: "text",
			text: "<conversation-checkpoint>\nThe following is the prior conversation transcript from this session (preserved across model switch, compaction, or branch navigation):",
		});

		for (const msg of historySlice) {
			if (msg.role === "user") {
				if (typeof msg.content === "string") {
					blocks.push({ type: "text", text: `<user>\n${msg.content}\n</user>` });
				} else {
					blocks.push({ type: "text", text: "<user>" });
					for (const part of msg.content) {
						if (part.type === "text") {
							blocks.push({ type: "text", text: part.text });
						} else if (part.type === "image") {
							blocks.push({ type: "image", data: part.data, mimeType: part.mimeType });
						}
					}
					blocks.push({ type: "text", text: "</user>" });
				}
			} else if (msg.role === "assistant") {
				const segs: string[] = [];
				for (const part of msg.content) {
					if (part.type === "thinking" && part.thinking.trim()) {
						segs.push(`<thought>\n${part.thinking.trim()}\n</thought>`);
					} else if (part.type === "text" && part.text.trim()) {
						segs.push(part.text);
					} else if (part.type === "toolCall") {
						segs.push(
							`<tool_call id="${part.id}" name="${part.name}">${JSON.stringify(part.arguments)}</tool_call>`,
						);
					}
				}
				if (segs.length > 0) {
					blocks.push({
						type: "text",
						text: `<assistant model="${msg.model}">\n${segs.join("\n")}\n</assistant>`,
					});
				}
			} else if (msg.role === "toolResult") {
				const status = msg.isError ? "error" : "ok";
				const texts = msg.content
					.filter((c): c is TextContent => c.type === "text")
					.map((c) => c.text)
					.join("\n");
				blocks.push({
					type: "text",
					text: `<tool_result id="${msg.toolCallId}" tool="${msg.toolName}" status="${status}">\n${texts}\n</tool_result>`,
				});
				for (const part of msg.content) {
					if (part.type === "image") {
						blocks.push({ type: "image", data: part.data, mimeType: part.mimeType });
					}
				}
			}
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
