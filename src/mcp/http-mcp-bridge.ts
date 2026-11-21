import * as crypto from "node:crypto";
import * as http from "node:http";
import type { ImageContent, JsonObject, TextContent, Tool } from "@earendil-works/pi-ai";
import type { AcpMcpServerSpec } from "../acp/protocol.ts";
import { validateToolArguments } from "./schema-validator.ts";
import { buildInlineStdioShimCommand } from "./stdio-shim.ts";

export const MCP_SERVER_NAME = "pi_tools";

export interface McpToolResultPayload {
	content: Array<TextContent | ImageContent>;
	isError?: boolean;
}

export interface ParkedToolCall {
	/** Pi 侧使用的工具调用唯一 ID */
	callId: string;
	/** 还原后的 Pi 原始工具名 */
	toolName: string;
	/** 校验后的工具参数对象 */
	arguments: JsonObject;
	/** 所属 AGY 会话 ID（从 _meta 或活跃绑定提取） */
	agySessionId?: string;
	/** 结算 HTTP MCP 响应 */
	settle: (result: McpToolResultPayload) => void;
	/** 是否已因超过 165s 守护阈值而剥离为迟到补发模式 */
	readonly isDetached: boolean;
}

export interface SessionMcpBinding {
	agySessionId?: string;
	tools: Tool[];
	/** 收到合法的 MCP tools/call 且通过门控校验时触发，交由协调器写入 ToolCall 流 */
	onParkedCall: (call: ParkedToolCall) => void;
	/** 达到 mcpHoldLimitMs 仍未收到宿主工具执行结果时触发，先对 AGY 执行预取消 */
	onHoldTimeout: (call: ParkedToolCall) => Promise<void>;
	/** 当工具调用在未知工具、参数预校验或门控阶段提前被拒绝时回调，用于平衡前置宣告计数 */
	onRejectedCall?: (toolName: string) => void;
	/** 当上游 HTTP 客户端在工具结果结算前主动断连时回调，用于及时清理失效挂起句柄 */
	onClientDisconnect?: (callId: string) => void;
}

interface AnnouncedToolEntry {
	agySessionId: string;
	rawTitleOrName: string;
	timestamp: number;
}

interface PendingAnnouncementWaiter {
	toolName: string;
	agySessionId?: string;
	resolve: (matched: boolean) => void;
	timer: ReturnType<typeof setTimeout>;
}

/** JSON Schema 清洗：剥离 TypeBox Symbol 及可能触发上游严格解析器异常的非标准顶层元字段 */
export function sanitizeJsonSchemaForMcp(rawSchema: unknown): Record<string, unknown> {
	try {
		const cloned = JSON.parse(JSON.stringify(rawSchema ?? { type: "object", properties: {} })) as Record<
			string,
			unknown
		>;
		delete cloned.$schema;
		if (!cloned.type && cloned.properties) {
			cloned.type = "object";
		}
		return cloned;
	} catch {
		return { type: "object", properties: {} };
	}
}

function normalizeMatchToken(name: string): string {
	return name.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function doesAnnouncementMatchTool(announced: string, toolName: string): boolean {
	if (!announced) return true;
	const a = normalizeMatchToken(announced);
	const t = normalizeMatchToken(toolName);
	return a.includes(t) || t.includes(a);
}

function isSingleEditReplacement(value: unknown): value is { oldText: string; newText: string } {
	if (!value || typeof value !== "object" || Array.isArray(value)) {
		return false;
	}
	const rec = value as Record<string, unknown>;
	return typeof rec.oldText === "string" && typeof rec.newText === "string";
}

/**
 * 在 JSON Schema 预校验前对齐原生 Pi 工具的参数容错归一化（如 `edit` 工具对字符串化 `edits`、单对象 `edits` 或顶层 `oldText/newText` 的兼容转换）。
 */
function normalizeHostToolArguments(
	toolName: string,
	rawArgs: JsonObject,
	toolDef: Tool,
): JsonObject {
	let current: unknown = { ...rawArgs };
	const maybeWithPrepare = toolDef as Tool & {
		prepareArguments?: (input: unknown) => unknown;
	};
	if (typeof maybeWithPrepare.prepareArguments === "function") {
		try {
			current = maybeWithPrepare.prepareArguments(current);
		} catch {
			// 交由后续 JSON Schema 校验生成明确自愈反馈
		}
	}

	if (toolName === "edit" && current && typeof current === "object" && !Array.isArray(current)) {
		const args = { ...(current as Record<string, unknown>) };
		if (typeof args.edits === "string") {
			try {
				const parsed: unknown = JSON.parse(args.edits);
				if (Array.isArray(parsed)) {
					args.edits = parsed;
				} else if (isSingleEditReplacement(parsed)) {
					args.edits = [parsed];
				}
			} catch {
				// 保留原值交由校验器提示
			}
		} else if (isSingleEditReplacement(args.edits)) {
			args.edits = [args.edits];
		}

		if (typeof args.oldText === "string" && typeof args.newText === "string") {
			const mergedEdits = Array.isArray(args.edits) ? [...args.edits] : [];
			mergedEdits.push({ oldText: args.oldText, newText: args.newText });
			delete args.oldText;
			delete args.newText;
			args.edits = mergedEdits;
		}
		current = args;
	}

	if (current && typeof current === "object" && !Array.isArray(current)) {
		return current as JsonObject;
	}
	return rawArgs;
}

/**
 * 本地回环 Streamable HTTP MCP 服务端。
 * 支持多会话独立 Bearer Token 路由、JSON Schema 参数自愈校验、ACP 公告门控校验及 165s 超时预剥离。
 */
export class LocalHttpMcpBridge {
	private server?: http.Server;
	private port = 0;
	private readonly bindingsByToken = new Map<string, SessionMcpBinding>();
	private readonly sseClientsByToken = new Map<string, Set<http.ServerResponse>>();
	private readonly recentAnnouncements: AnnouncedToolEntry[] = [];
	private readonly announcementWaiters = new Set<PendingAnnouncementWaiter>();
	private readonly cancelledSessions = new Set<string>();
	private holdLimitMs: number;

	constructor(holdLimitMs = 165_000) {
		this.holdLimitMs = holdLimitMs;
	}

	setHoldLimitMs(ms: number): void {
		this.holdLimitMs = Math.max(5_000, Math.min(175_000, Math.floor(ms)));
	}

	async ensureListening(): Promise<number> {
		if (this.server && this.port > 0) {
			return this.port;
		}

		const srv = http.createServer((req, res) => {
			void this.handleHttpRequest(req, res);
		});

		await new Promise<void>((resolve, reject) => {
			srv.once("error", reject);
			srv.listen(0, "127.0.0.1", () => {
				srv.removeListener("error", reject);
				const addr = srv.address();
				if (addr && typeof addr === "object") {
					this.port = addr.port;
					this.server = srv;
					resolve();
				} else {
					reject(new Error("无法获取本地 HTTP MCP 服务监听端口"));
				}
			});
		});

		return this.port;
	}

	/** 注册一个新的会话级 MCP 绑定并返回一次性鉴权 Token */
	registerSessionBinding(binding: SessionMcpBinding): string {
		const token = crypto.randomBytes(24).toString("hex");
		this.bindingsByToken.set(token, binding);
		return token;
	}

	updateBindingSessionId(token: string, agySessionId: string): void {
		const binding = this.bindingsByToken.get(token);
		if (binding) {
			binding.agySessionId = agySessionId;
			this.cancelledSessions.delete(agySessionId);
		}
	}

	/** 更新会话绑定的工具表，并返回是否已通过活跃 SSE 连接向客户端实时推送 `tools/list_changed` */
	updateBindingTools(token: string, tools: Tool[]): boolean {
		const binding = this.bindingsByToken.get(token);
		if (!binding) return false;
		binding.tools = tools;
		return this.broadcastToolsListChanged(token);
	}

	unregisterSessionBinding(token: string): void {
		this.bindingsByToken.delete(token);
		const clients = this.sseClientsByToken.get(token);
		if (clients) {
			for (const res of clients) {
				try {
					res.end();
				} catch {
					// 忽略关闭异常
				}
			}
			this.sseClientsByToken.delete(token);
		}
	}

	/** 记录来自 ACP `session/update` 的 `tool_call` 宣告，供门控配对校验 */
	recordAcpToolAnnouncement(agySessionId: string, rawTitleOrName: string): void {
		if (this.cancelledSessions.has(agySessionId)) {
			return;
		}

		for (const waiter of Array.from(this.announcementWaiters)) {
			if (
				(!waiter.agySessionId || waiter.agySessionId === agySessionId) &&
				doesAnnouncementMatchTool(rawTitleOrName, waiter.toolName)
			) {
				clearTimeout(waiter.timer);
				this.announcementWaiters.delete(waiter);
				waiter.resolve(true);
				return;
			}
		}

		const now = Date.now();
		this.pruneExpiredAnnouncements(now);
		this.recentAnnouncements.push({
			agySessionId,
			rawTitleOrName,
			timestamp: now,
		});
	}

	/** 标记某会话当前轮次已取消，拦截迟到的残留 MCP `tools/call` */
	markSessionTurnCancelled(agySessionId: string): void {
		this.cancelledSessions.add(agySessionId);
		for (let i = this.recentAnnouncements.length - 1; i >= 0; i--) {
			if (this.recentAnnouncements[i].agySessionId === agySessionId) {
				this.recentAnnouncements.splice(i, 1);
			}
		}
		for (const waiter of Array.from(this.announcementWaiters)) {
			if (waiter.agySessionId === agySessionId) {
				clearTimeout(waiter.timer);
				this.announcementWaiters.delete(waiter);
				waiter.resolve(false);
			}
		}
	}

	clearSessionCancelled(agySessionId: string): void {
		this.cancelledSessions.delete(agySessionId);
	}

	/** 生成注入 `session/new` 或 `session/resume` 的 ACP `mcpServers` 配置项 */
	async buildAcpMcpServerSpec(
		token: string,
		useHttpTransport: boolean,
	): Promise<AcpMcpServerSpec> {
		const port = await this.ensureListening();
		const url = `http://127.0.0.1:${port}/mcp`;

		if (useHttpTransport) {
			return {
				type: "http",
				name: MCP_SERVER_NAME,
				url,
				headers: [{ name: "Authorization", value: `Bearer ${token}` }],
			};
		}

		const shim = buildInlineStdioShimCommand(url, token);
		return {
			type: "stdio",
			name: MCP_SERVER_NAME,
			command: shim.command,
			args: shim.args,
			env: shim.env,
		};
	}

	async dispose(): Promise<void> {
		for (const token of Array.from(this.bindingsByToken.keys())) {
			this.unregisterSessionBinding(token);
		}
		for (const waiter of this.announcementWaiters) {
			clearTimeout(waiter.timer);
			waiter.resolve(false);
		}
		this.announcementWaiters.clear();

		if (this.server) {
			const srv = this.server;
			this.server = undefined;
			this.port = 0;
			srv.closeIdleConnections?.();
			srv.closeAllConnections?.();
			await new Promise<void>((resolve) => srv.close(() => resolve()));
		}
	}

	private broadcastToolsListChanged(token: string): boolean {
		const clients = this.sseClientsByToken.get(token);
		if (!clients || clients.size === 0) return false;
		const payload = JSON.stringify({
			jsonrpc: "2.0",
			method: "notifications/tools/list_changed",
			params: {},
		});
		let delivered = false;
		for (const res of Array.from(clients)) {
			if (res.writableEnded || res.destroyed) {
				clients.delete(res);
				continue;
			}
			try {
				res.write(`event: message\ndata: ${payload}\n\n`);
				delivered = true;
			} catch {
				clients.delete(res);
			}
		}
		return delivered;
	}

	private pruneExpiredAnnouncements(now: number): void {
		while (
			this.recentAnnouncements.length > 0 &&
			now - this.recentAnnouncements[0].timestamp > 10_000
		) {
			this.recentAnnouncements.shift();
		}
	}

	private waitForAnnouncementPairing(
		toolName: string,
		agySessionId: string | undefined,
		windowMs = 2_000,
	): Promise<boolean> {
		if (agySessionId && this.cancelledSessions.has(agySessionId)) {
			return Promise.resolve(false);
		}

		const now = Date.now();
		this.pruneExpiredAnnouncements(now);
		const existingIdx = this.recentAnnouncements.findIndex(
			(entry) =>
				(!agySessionId || entry.agySessionId === agySessionId) &&
				doesAnnouncementMatchTool(entry.rawTitleOrName, toolName),
		);
		if (existingIdx !== -1) {
			this.recentAnnouncements.splice(existingIdx, 1);
			return Promise.resolve(true);
		}

		return new Promise<boolean>((resolve) => {
			const waiter: PendingAnnouncementWaiter = {
				toolName,
				agySessionId,
				resolve,
				timer: setTimeout(() => {
					this.announcementWaiters.delete(waiter);
					// 若窗口期满后会话已被取消则拦截；若会话仍正常活跃则放行，兼容不发送前置宣告的模型分支
					const cancelled = Boolean(agySessionId && this.cancelledSessions.has(agySessionId));
					resolve(!cancelled);
				}, windowMs),
			};
			this.announcementWaiters.add(waiter);
		});
	}

	private extractBearerToken(req: http.IncomingMessage): string | undefined {
		const header = req.headers.authorization;
		if (!header || typeof header !== "string") return undefined;
		const match = /^Bearer\s+(.+)$/i.exec(header.trim());
		return match?.[1]?.trim();
	}

	private async handleHttpRequest(
		req: http.IncomingMessage,
		res: http.ServerResponse,
	): Promise<void> {
		const urlPath = (req.url || "/").split("?")[0];

		// AGY MCP 客户端会探测 OAuth 元数据路径且不携带静态 Authorization 头，返回 404 表明无需 OAuth 握手
		if (urlPath.startsWith("/.well-known/")) {
			res.writeHead(404);
			res.end();
			return;
		}

		if (urlPath !== "/mcp") {
			res.writeHead(404);
			res.end();
			return;
		}

		const token = this.extractBearerToken(req);
		const binding = token ? this.bindingsByToken.get(token) : undefined;
		if (!token || !binding) {
			res.writeHead(401, { "Content-Type": "application/json" });
			res.end(JSON.stringify({ error: "Unauthorized" }));
			return;
		}

		// 处理会话关闭时的空体 DELETE 请求
		if (req.method === "DELETE") {
			res.writeHead(200);
			res.end();
			return;
		}

		// 处理 Streamable HTTP SSE 通知订阅请求
		if (req.method === "GET") {
			res.writeHead(200, {
				"Content-Type": "text/event-stream",
				"Cache-Control": "no-cache",
				Connection: "keep-alive",
			});
			let set = this.sseClientsByToken.get(token);
			if (!set) {
				set = new Set();
				this.sseClientsByToken.set(token, set);
			}
			set.add(res);
			req.on("close", () => {
				set?.delete(res);
			});
			return;
		}

		if (req.method !== "POST") {
			res.writeHead(405);
			res.end();
			return;
		}

		let rawBody = "";
		try {
			for await (const chunk of req) {
				rawBody += typeof chunk === "string" ? chunk : (chunk as Buffer).toString("utf8");
			}
		} catch {
			res.writeHead(400);
			res.end();
			return;
		}

		if (!rawBody.trim()) {
			res.writeHead(202);
			res.end();
			return;
		}

		let rpc: Record<string, unknown>;
		try {
			rpc = JSON.parse(rawBody) as Record<string, unknown>;
		} catch {
			res.writeHead(200, { "Content-Type": "application/json" });
			res.end(
				JSON.stringify({
					jsonrpc: "2.0",
					id: null,
					error: { code: -32700, message: "Parse error" },
				}),
			);
			return;
		}

		const id = rpc.id;
		const method = typeof rpc.method === "string" ? rpc.method : "";
		const params =
			rpc.params && typeof rpc.params === "object"
				? (rpc.params as Record<string, unknown>)
				: {};

		// 单向通知直接返回 202
		if (id === undefined) {
			res.writeHead(202);
			res.end();
			return;
		}

		res.setHeader("Content-Type", "application/json");
		res.setHeader("Mcp-Session-Id", token.slice(0, 16));

		if (method === "initialize") {
			const clientProto =
				typeof params.protocolVersion === "string" ? params.protocolVersion : "2025-11-25";
			res.writeHead(200);
			res.end(
				JSON.stringify({
					jsonrpc: "2.0",
					id,
					result: {
						protocolVersion: clientProto,
						capabilities: {
							tools: { listChanged: true },
						},
						serverInfo: {
							name: MCP_SERVER_NAME,
							version: "0.1.0",
						},
					},
				}),
			);
			return;
		}

		if (method === "ping") {
			res.writeHead(200);
			res.end(JSON.stringify({ jsonrpc: "2.0", id, result: {} }));
			return;
		}

		if (method === "tools/list") {
			const toolsList = binding.tools.map((tool) => ({
				name: tool.name,
				description: tool.description || tool.name,
				inputSchema: sanitizeJsonSchemaForMcp(tool.parameters),
			}));
			res.writeHead(200);
			res.end(
				JSON.stringify({
					jsonrpc: "2.0",
					id,
					result: { tools: toolsList },
				}),
			);
			return;
		}

		if (method === "tools/call") {
			await this.handleToolCallRpc(id, params, binding, res);
			return;
		}

		// 对 server/discover 等非标准探测方法返回标准 -32601，引导客户端回退标准 initialize
		res.writeHead(200);
		res.end(
			JSON.stringify({
				jsonrpc: "2.0",
				id,
				error: { code: -32601, message: `Method not found: ${method}` },
			}),
		);
	}

	private async handleToolCallRpc(
		rpcId: unknown,
		params: Record<string, unknown>,
		binding: SessionMcpBinding,
		res: http.ServerResponse,
	): Promise<void> {
		const toolName = typeof params.name === "string" ? params.name : "";
		const rawArgs =
			params.arguments && typeof params.arguments === "object" && !Array.isArray(params.arguments)
				? (params.arguments as JsonObject)
				: {};

		const meta =
			params._meta && typeof params._meta === "object"
				? (params._meta as Record<string, unknown>)
				: undefined;
		const rootConvId =
			(typeof meta?.["antigravity.google/root_conversation_id"] === "string"
				? (meta["antigravity.google/root_conversation_id"] as string)
				: undefined) ??
			(typeof meta?.["antigravity.google/conversation_id"] === "string"
				? (meta["antigravity.google/conversation_id"] as string)
				: undefined) ??
			binding.agySessionId;

		const toolDef = binding.tools.find((t) => t.name === toolName);
		if (!toolDef) {
			binding.onRejectedCall?.(toolName);
			res.writeHead(200);
			res.end(
				JSON.stringify({
					jsonrpc: "2.0",
					id: rpcId,
					result: {
						isError: true,
						content: [
							{
								type: "text",
								text: `Unknown tool "${toolName}". Available tools: ${binding.tools.map((t) => t.name).join(", ")}`,
							},
						],
					},
				}),
			);
			return;
		}

		// 1. 对齐宿主工具的参数预处理与 JSON Schema 参数预校验
		const normalizedArgs = normalizeHostToolArguments(toolName, rawArgs, toolDef);
		const validation = validateToolArguments(toolName, normalizedArgs, toolDef.parameters);
		if (!validation.valid) {
			binding.onRejectedCall?.(toolName);
			res.writeHead(200);
			res.end(
				JSON.stringify({
					jsonrpc: "2.0",
					id: rpcId,
					result: {
						isError: true,
						content: [{ type: "text", text: validation.formattedError ?? "Invalid tool arguments" }],
					},
				}),
			);
			return;
		}

		// 2. ACP 公告门控校验，防止已取消旧轮次迟到调用串扰
		const allowed = await this.waitForAnnouncementPairing(toolName, rootConvId, 450);
		if (!allowed) {
			binding.onRejectedCall?.(toolName);
			res.writeHead(200);
			res.end(
				JSON.stringify({
					jsonrpc: "2.0",
					id: rpcId,
					result: {
						isError: true,
						content: [{ type: "text", text: "Tool call cancelled." }],
					},
				}),
			);
			return;
		}

		// 3. Park-and-Yield 挂起 HTTP 响应，交由 Pi Agent Loop 执行真实工具
		const callId = `agy_mcp_${crypto.randomUUID().slice(0, 12)}`;
		let settled = false;
		let detached = false;

		const sendMcpResult = (payload: McpToolResultPayload) => {
			if (settled) return;
			settled = true;
			clearTimeout(holdTimer);
			const mcpContent = payload.content.map((block) => {
				if (block.type === "image") {
					return {
						type: "image",
						data: block.data,
						mimeType: block.mimeType,
					};
				}
				return {
					type: "text",
					text: block.text,
				};
			});
			if (mcpContent.length === 0) {
				mcpContent.push({ type: "text", text: "(empty output)" });
			}
			try {
				if (!res.writableEnded) {
					res.writeHead(200);
					res.end(
						JSON.stringify({
							jsonrpc: "2.0",
							id: rpcId,
							result: {
								isError: Boolean(payload.isError),
								content: mcpContent,
							},
						}),
					);
				}
			} catch {
				// 忽略客户端提前断连异常
			}
		};

		const parkedCall: ParkedToolCall = {
			callId,
			toolName,
			arguments: normalizedArgs,
			agySessionId: rootConvId,
			get isDetached() {
				return detached;
			},
			settle: (result) => {
				sendMcpResult(result);
			},
		};

		const holdTimer = setTimeout(() => {
			if (settled) return;
			detached = true;
			void binding
				.onHoldTimeout(parkedCall)
				.catch(() => {})
				.finally(() => {
					sendMcpResult({
						isError: true,
						content: [
							{
								type: "text",
								text: "Tool execution detached due to hold timeout; result will be delivered in the next prompt.",
							},
						],
					});
				});
		}, this.holdLimitMs);

		res.on("close", () => {
			if (!settled) {
				settled = true;
				clearTimeout(holdTimer);
				binding.onClientDisconnect?.(callId);
			}
		});

		binding.onParkedCall(parkedCall);
	}
}
