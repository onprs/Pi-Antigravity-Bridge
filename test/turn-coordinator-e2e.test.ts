import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";
import {
	normalizeContext,
	type AssistantMessageEvent,
	type Model,
	type ToolCall,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { AcpProcessManager } from "../src/acp/process-manager.ts";
import { DEFAULT_BRIDGE_CONFIG, type BridgeConfig } from "../src/config.ts";
import { TurnCoordinator } from "../src/session/turn-coordinator.ts";
import {
	AgyTurnMirrorBroker,
	createAgyMirrorToolDefinitions,
} from "../src/tools/agy-mirror-tools.ts";

/**
 * 生成一个纯 Node.js 标准库编写的跨平台模拟 `agy acp` 服务端脚本，
 * 真实走 stdio NDJSON 与本地 Streamable HTTP MCP 网络套接字进行端到端协议验证。
 */
function writeMockAgyExecutable(dir: string): string {
	const scriptPath = path.join(dir, "mock-agy.mjs");
	const scriptSource = `#!/usr/bin/env node
import * as readline from "node:readline";

const isModeA = !process.argv.some((arg) => arg.startsWith("pi-bridge"));
const sessions = new Map();

function send(obj) {
  process.stdout.write(JSON.stringify(obj) + "\\n");
}

let nextServerReqId = 9000;
const serverPending = new Map();

function requestClient(method, params) {
  const id = nextServerReqId++;
  return new Promise((resolve) => {
    serverPending.set(id, resolve);
    send({ jsonrpc: "2.0", id, method, params });
  });
}

const rl = readline.createInterface({ input: process.stdin });
rl.on("line", async (line) => {
  if (!line.trim()) return;
  const msg = JSON.parse(line);

  if (msg.id !== undefined && !msg.method && serverPending.has(msg.id)) {
    const fn = serverPending.get(msg.id);
    serverPending.delete(msg.id);
    fn(msg.result);
    return;
  }

  if (msg.method === "initialize") {
    send({
      jsonrpc: "2.0",
      id: msg.id,
      result: {
        protocolVersion: 1,
        agentCapabilities: {
          mcpCapabilities: { http: true },
          sessionCapabilities: { resume: {}, close: {}, list: {}, load: {}, additionalDirectories: {} },
          _meta: { antigravity: { tier: true, systemPrompt: { append: true } } }
        },
        agentInfo: { name: "Mock AGY", version: "1.5.13" }
      }
    });
    return;
  }

  if (msg.method === "session/new") {
    const sessionId = "mock-session-" + (sessions.size + 1);
    const mcpHttp = (msg.params?.mcpServers || []).find((s) => s.type === "http");
    sessions.set(sessionId, { mcpHttp });

    if (mcpHttp) {
      const authHeader = mcpHttp.headers?.[0]?.value || "";
      await fetch(mcpHttp.url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: authHeader },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-11-25" } })
      });
      await fetch(mcpHttp.url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: authHeader },
        body: JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} })
      });
    }

    send({
      jsonrpc: "2.0",
      id: msg.id,
      result: {
        sessionId,
        models: { availableModels: [{ modelId: "gemini-4-neon-high", name: "Gemini 4 Neon (High)" }] },
        configOptions: [
          { id: "model", name: "Model", type: "select", currentValue: "gemini-4-neon", options: [{ value: "gemini-4-neon", name: "Gemini 4 Neon" }] },
          { id: "thought_level", name: "Thinking", type: "select", currentValue: "high", options: [{ value: "high", name: "High" }] },
          { id: "mode", name: "Mode", type: "select", currentValue: "default", options: [{ value: "default", name: "Default" }] }
        ],
        _meta: { antigravity: { tier: { id: "ultra", name: "Google AI Ultra" } } }
      }
    });
    return;
  }

  if (msg.method === "session/cancel") {
    // 模拟后端繁忙时 session/cancel 迟迟不回包，验证前端 Esc 不受其阻塞
    return;
  }

  if (msg.method === "session/set_config_option" || msg.method === "session/close") {
    send({ jsonrpc: "2.0", id: msg.id, result: {} });
    return;
  }

  if (msg.method === "session/prompt") {
    const { sessionId, prompt } = msg.params;
    if (prompt?.[0]?.text === "HANG_FOREVER") {
      // 模拟长耗时推理中等待用户按 Esc
      return;
    }
    const sess = sessions.get(sessionId);

    if (!isModeA && sess?.mcpHttp) {
      // 模式 B：模拟中间步 usage_update + 思考 + 工具宣告 + HTTP MCP 挂起 + 最终步 usage_update 与累计账单返回
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "usage_update", used: 40000, size: 1048576 } }
      });
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "agent_thought_chunk", content: { type: "text", text: "Checking package.json..." } } }
      });
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "tool_call", toolCallId: "acp_tc_1", title: "mcp_pi_tools_read" } }
      });

      const authHeader = sess.mcpHttp.headers?.[0]?.value || "";
      const toolRes = await fetch(sess.mcpHttp.url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: authHeader },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 10,
          method: "tools/call",
          params: {
            name: "read",
            arguments: { path: "package.json" },
            _meta: { "antigravity.google/root_conversation_id": sessionId }
          }
        })
      }).then((r) => r.json());

      const readText = toolRes.result?.content?.[0]?.text || "";
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "usage_update", used: 50000, size: 1048576 } }
      });
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "agent_thought_chunk", content: { type: "text", text: "Got result." } } }
      });
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text: "File read OK: " + readText } } }
      });
      send({
        jsonrpc: "2.0",
        id: msg.id,
        result: {
          stopReason: "end_turn",
          usage: { inputTokens: 15000, cachedReadTokens: 75000, outputTokens: 80, thoughtTokens: 15 }
        }
      });
      return;
    }

    // 模式 A：原生工具切片 + 权限确认 + Diff 更新 + 续接正文
    send({
      jsonrpc: "2.0",
      method: "session/update",
      params: { sessionId, update: { sessionUpdate: "agent_thought_chunk", content: { type: "text", text: "Editing src/app.ts..." } } }
    });
    send({
      jsonrpc: "2.0",
      method: "session/update",
      params: {
        sessionId,
        update: {
          sessionUpdate: "tool_call",
          toolCallId: "native_edit_1",
          kind: "edit",
          title: "Edit src/app.ts",
          status: "pending",
          locations: [{ path: "src/app.ts", line: 1 }]
        }
      }
    });

    // 请求客户端授权确认
    const perm = await requestClient("session/request_permission", {
      sessionId,
      toolCall: { toolCallId: "native_edit_1", title: "Edit src/app.ts", kind: "edit" },
      options: [
        { optionId: "allow_once", name: "Allow once", kind: "allow_once" },
        { optionId: "reject_once", name: "Reject", kind: "reject_once" }
      ]
    });

    send({
      jsonrpc: "2.0",
      method: "session/update",
      params: {
        sessionId,
        update: {
          sessionUpdate: "tool_call_update",
          toolCallId: "native_edit_1",
          status: "completed",
          content: [
            { type: "diff", path: "src/app.ts", oldText: "const a = 1;", newText: "const a = 2;" },
            { type: "content", content: { type: "text", text: "Permission: " + perm?.outcome?.optionId } }
          ]
        }
      }
    });

    send({
      jsonrpc: "2.0",
      method: "session/update",
      params: { sessionId, update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text: "Native edit done!" } } }
    });
    send({
      jsonrpc: "2.0",
      id: msg.id,
      result: { stopReason: "end_turn", usage: { inputTokens: 200, outputTokens: 60 } }
    });
  }
});
`;

	fs.writeFileSync(scriptPath, scriptSource, { mode: 0o755 });
	if (process.platform === "win32") {
		const cmdWrapper = path.join(dir, "mock-agy.cmd");
		fs.writeFileSync(cmdWrapper, `@node "${scriptPath}" %*\r\n`);
		return cmdWrapper;
	}
	return scriptPath;
}

const TEST_MODEL: Model<"antigravity-acp"> = {
	id: "gemini-4-neon",
	name: "Gemini 4 Neon",
	api: "antigravity-acp",
	provider: "antigravity",
	baseUrl: "acp://antigravity-cli",
	reasoning: true,
	input: ["text", "image"],
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
	contextWindow: 512_000,
	maxTokens: 65_536,
};

function createStubPiApi(): ExtensionAPI {
	return {
		appendEntry() {},
		getSessionName() {
			return undefined;
		},
		setSessionName() {},
	} as unknown as ExtensionAPI;
}

describe("TurnCoordinator 双模式端到端协议闭环集成测试", () => {
	it("模式 B (pi-tools)：完成 HTTP MCP Park-and-Yield 挂起、宿主工具执行与同轮续接", async () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-e2e-b-"));
		const binPath = writeMockAgyExecutable(tmpRoot);
		const cfg: BridgeConfig = {
			...DEFAULT_BRIDGE_CONFIG,
			toolMode: "pi-tools",
			agyBin: binPath,
			geminiDir: path.join(tmpRoot, ".gemini"),
		};

		const pm = new AcpProcessManager();
		const broker = new AgyTurnMirrorBroker();
		const coord = new TurnCoordinator(createStubPiApi(), pm, broker, cfg);

		try {
			const tools = [
				{
					name: "read",
					description: "Read file",
					parameters: {
						type: "object",
						properties: { path: { type: "string" } },
						required: ["path"],
					},
				},
			];

			// 第 1 步：发送用户请求，预期收到思考块 + read 工具调用并以 toolUse 结束
			const ctx1 = normalizeContext({
				systemPrompt: "You are Pi.",
				tools,
				messages: [{ role: "user", content: "Inspect package.json", timestamp: 1 }],
			});

			const stream1 = coord.streamSimple(TEST_MODEL, ctx1, {
				sessionId: "pi-sess-b",
				reasoning: "high",
			});
			const eventsStep1: AssistantMessageEvent[] = [];
			for await (const ev of stream1) {
				eventsStep1.push(ev);
			}
			const msg1 = await stream1.result();

			assert.equal(msg1.stopReason, "toolUse");
			const toolCall = msg1.content.find((c): c is ToolCall => c.type === "toolCall");
			assert.ok(toolCall);
			assert.equal(toolCall.name, "read");
			assert.deepEqual(toolCall.arguments, { path: "package.json" });
			// 验证所有流式事件中只存在唯一稳定的 toolCall.id（无空参数占位幽灵卡片），且流式采样数 >= 2
			const seenToolIds = new Set<string>();
			let totalDeltas = 0;
			for (const ev of eventsStep1) {
				if (
					ev.type === "thinking_delta" ||
					ev.type === "text_delta" ||
					ev.type === "toolcall_delta"
				) {
					totalDeltas++;
				}
				if ("partial" in ev) {
					for (const b of ev.partial.content) {
						if (b.type === "toolCall") {
							seenToolIds.add(b.id);
							assert.deepEqual(b.arguments, { path: "package.json" });
						}
					}
				}
			}
			assert.equal(seenToolIds.size, 1);
			assert.ok(totalDeltas >= 2);
			assert.ok(msg1.usage.output > 0);
			assert.equal(msg1.usage.totalTokens, 40000);

			// 第 2 步：模拟 Pi Agent Loop 执行完 read 工具并返回结果，预期无缝恢复同一 ACP Turn
			const ctx2 = normalizeContext({
				systemPrompt: "You are Pi.",
				tools,
				messages: [
					{ role: "user", content: "Inspect package.json", timestamp: 1 },
					msg1,
					{
						role: "toolResult",
						toolCallId: toolCall.id,
						toolName: "read",
						content: [{ type: "text", text: '{"name":"demo"}' }],
						isError: false,
						timestamp: 2,
					},
				],
			});

			const stream2 = coord.streamSimple(TEST_MODEL, ctx2, {
				sessionId: "pi-sess-b",
				reasoning: "high",
			});
			const msg2 = await stream2.result();

			assert.equal(msg2.stopReason, "stop");
			const finalText = msg2.content
				.filter((c) => c.type === "text")
				.map((c) => c.text)
				.join("");
			assert.equal(finalText, 'File read OK: {"name":"demo"}');
			// 最终消息总窗口严格对齐当前真实窗口 50000（避免两步累加值 90080 虚高），同时保留缓存命中拆分
			assert.equal(msg2.usage.totalTokens, 50000);
			assert.ok(msg2.usage.cacheRead > 35000);
			assert.ok(msg2.usage.output > 0);
			assert.equal(TEST_MODEL.contextWindow, 1048576);
		} finally {
			await coord.dispose();
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});

	it("模式 A (agy-tools)：完成单 Turn 流分片、镜像工具 agy_edit、权限弹窗审批与 Diff 同步", async () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-e2e-a-"));
		const binPath = writeMockAgyExecutable(tmpRoot);
		const cfg: BridgeConfig = {
			...DEFAULT_BRIDGE_CONFIG,
			toolMode: "agy-tools",
			agyBin: binPath,
			geminiDir: path.join(tmpRoot, ".gemini"),
		};

		const pm = new AcpProcessManager();
		const broker = new AgyTurnMirrorBroker();
		const coord = new TurnCoordinator(createStubPiApi(), pm, broker, cfg);
		const mirrorTools = createAgyMirrorToolDefinitions(broker);

		let permissionPromptShown = false;
		const stubUiCtx = {
			hasUI: true,
			cwd: tmpRoot,
			ui: {
				async select(_title: string, options: string[]) {
					permissionPromptShown = true;
					return options[0]; // 选择 Allow once
				},
				setStatus() {},
			},
		} as unknown as ExtensionContext;
		coord.setActiveContext(stubUiCtx);

		try {
			// 第 1 步：启动流，预期在原生 tool_call 处自动分片，产出 agy_edit 工具调用
			const ctx1 = normalizeContext({
				messages: [{ role: "user", content: "Update src/app.ts", timestamp: 1 }],
			});
			const stream1 = coord.streamSimple(TEST_MODEL, ctx1, { sessionId: "pi-sess-a" });
			const events1: AssistantMessageEvent[] = [];
			for await (const ev of stream1) {
				events1.push(ev);
			}
			const msg1 = await stream1.result();

			assert.equal(msg1.stopReason, "toolUse");
			const mirrorCall = msg1.content.find((c): c is ToolCall => c.type === "toolCall");
			assert.ok(mirrorCall);
			assert.equal(mirrorCall.name, "agy_edit");
			assert.equal(mirrorCall.arguments.path, "src/app.ts");

			// 执行镜像工具：内部应消费权限请求弹窗并等待 tool_call_update(completed) 返回 Diff
			const editTool = mirrorTools.find((t) => t.name === "agy_edit")!;
			const mirrorResult = await editTool.execute(
				mirrorCall.id,
				mirrorCall.arguments,
				undefined,
				undefined,
				stubUiCtx as never,
			);

			assert.equal(permissionPromptShown, true);
			assert.equal(mirrorResult.details?.diffs?.length, 1);
			assert.equal(mirrorResult.details?.diffs?.[0].newText, "const a = 2;");

			// 第 2 步：自动续接同一后台 ACP Turn 的剩余正文输出
			const ctx2 = normalizeContext({
				messages: [
					{ role: "user", content: "Update src/app.ts", timestamp: 1 },
					msg1,
					{
						role: "toolResult",
						toolCallId: mirrorCall.id,
						toolName: "agy_edit",
						content: mirrorResult.content,
						isError: false,
						timestamp: 2,
					},
				],
			});
			const stream2 = coord.streamSimple(TEST_MODEL, ctx2, { sessionId: "pi-sess-a" });
			const msg2 = await stream2.result();

			assert.equal(msg2.stopReason, "stop");
			const text2 = msg2.content
				.filter((c) => c.type === "text")
				.map((c) => c.text)
				.join("");
			assert.equal(text2, "Native edit done!");
		} finally {
			await coord.dispose();
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});

	it("按下 Esc (signal.abort) 时即使后端 session/cancel 无响应也在 30ms 内瞬时结束流", async () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-e2e-esc-"));
		const binPath = writeMockAgyExecutable(tmpRoot);
		const cfg: BridgeConfig = {
			...DEFAULT_BRIDGE_CONFIG,
			toolMode: "agy-tools",
			agyBin: binPath,
			geminiDir: path.join(tmpRoot, ".gemini"),
		};

		const pm = new AcpProcessManager();
		const broker = new AgyTurnMirrorBroker();
		const coord = new TurnCoordinator(createStubPiApi(), pm, broker, cfg);

		try {
			const ac = new AbortController();
			const ctx = normalizeContext({
				messages: [{ role: "user", content: "HANG_FOREVER", timestamp: 1 }],
			});
			const stream = coord.streamSimple(TEST_MODEL, ctx, {
				sessionId: "pi-sess-esc",
				signal: ac.signal,
			});

			// 等待请求已发往 mock-agy 进入挂起态
			await new Promise((r) => setTimeout(r, 80));

			const t0 = Date.now();
			ac.abort();
			const abortedMsg = await stream.result();
			const elapsed = Date.now() - t0;

			assert.equal(abortedMsg.stopReason, "aborted");
			assert.ok(elapsed < 50, `Esc 中断耗时应小于 50ms，实际耗时 ${elapsed}ms`);
		} finally {
			await coord.dispose();
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});
});
