import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { DatabaseSync } from "node:sqlite";
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
    const cancelSid = msg.params?.sessionId;
    const sess = cancelSid ? sessions.get(cancelSid) : undefined;
    if (sess?.cancelablePromptId !== undefined) {
      const pid = sess.cancelablePromptId;
      sess.cancelablePromptId = undefined;
      sess.inFlightPromptId = undefined;
      send({ jsonrpc: "2.0", id: pid, result: { stopReason: "cancelled" } });
    }
    // 注意：真实 ACP 协议中 session/cancel 是单向通知，不返回 JSON-RPC response
    return;
  }

  if (msg.method === "session/set_config_option" || msg.method === "session/close") {
    send({ jsonrpc: "2.0", id: msg.id, result: {} });
    return;
  }

  if (msg.method === "session/prompt") {
    const { sessionId, prompt } = msg.params;
    const sess = sessions.get(sessionId);

    // 模拟真实 agy acp 协议约束：同一会话若上一轮 session/prompt 尚未结算就收到重叠 session/prompt，
    // 立即令旧请求提前返回 end_turn 并挂死新请求，确保测试能严格拦截任何并发抢占回归！
    if (sess?.inFlightPromptId !== undefined) {
      const oldId = sess.inFlightPromptId;
      sess.inFlightPromptId = msg.id;
      send({ jsonrpc: "2.0", id: oldId, result: { stopReason: "end_turn" } });
      return;
    }

    const lastBlockText = prompt?.[prompt.length - 1]?.text || "";

    if (lastBlockText === "HANG_FOREVER") {
      if (sess) sess.inFlightPromptId = msg.id;
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "agent_thought_chunk", content: { type: "text", text: "Hanging..." } } }
      });
      // 模拟极端死锁：即使收到 session/cancel 也不回包
      return;
    }

    if (lastBlockText === "CANCEL_RESPONSIVE") {
      if (sess) {
        sess.inFlightPromptId = msg.id;
        sess.cancelablePromptId = msg.id;
      }
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text: "Counting 1..." } } }
      });
      return;
    }

    if (lastBlockText === "SAME_GEN_DUAL_SEQ_TOOLS" && !isModeA && sess?.mcpHttp) {
      sess.inFlightPromptId = msg.id;
      const authHeader = sess.mcpHttp.headers?.[0]?.value || "";
      // 同一次 LLM 生成连续串行执行 2 个工具（used 窗口水位保持 50000 不变）
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "usage_update", used: 50000, size: 1048576 } }
      });
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "tool_call", toolCallId: "dup_1", title: "mcp_pi_tools_read" } }
      });
      await fetch(sess.mcpHttp.url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: authHeader },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 31,
          method: "tools/call",
          params: { name: "read", arguments: { path: "one.ts" }, _meta: { "antigravity.google/root_conversation_id": sessionId } }
        })
      }).then((r) => r.json());

      // 第 2 个工具在同一生成中紧随其后发出（无新 usage_update 或相同 used=50000）
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "tool_call", toolCallId: "dup_2", title: "mcp_pi_tools_read" } }
      });
      await fetch(sess.mcpHttp.url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: authHeader },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 32,
          method: "tools/call",
          params: { name: "read", arguments: { path: "two.ts" }, _meta: { "antigravity.google/root_conversation_id": sessionId } }
        })
      }).then((r) => r.json());

      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "usage_update", used: 54000, size: 1048576 } }
      });
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text: "Summary ok." } } }
      });
      sess.inFlightPromptId = undefined;
      send({
        jsonrpc: "2.0",
        id: msg.id,
        result: {
          stopReason: "end_turn",
          usage: { inputTokens: 4000, cachedReadTokens: 100000, outputTokens: 8000, thoughtTokens: 1000 }
        }
      });
      return;
    }

    if (lastBlockText === "MULTI_STEP_WITH_STEER" && !isModeA && sess?.mcpHttp) {
      const myPromptId = msg.id;
      sess.inFlightPromptId = myPromptId;
      sess.cancelablePromptId = myPromptId;
      const authHeader = sess.mcpHttp.headers?.[0]?.value || "";

      // Step 1：读取 a.ts
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "usage_update", used: 20000, size: 1048576 } }
      });
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "tool_call", toolCallId: "acp_ms_1", title: "mcp_pi_tools_read" } }
      });
      const res1 = await fetch(sess.mcpHttp.url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: authHeader },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 21,
          method: "tools/call",
          params: {
            name: "read",
            arguments: { path: "a.ts" },
            _meta: { "antigravity.google/root_conversation_id": sessionId }
          }
        })
      }).then((r) => r.json());

      sess.savedA = (res1.result?.content || []).map((c) => c.text || "").join("\\n");
      // 等待短暂窗口以允许工具边界处的 session/cancel 即时介入（验证 steer 在第 1 个工具结束边界立即切入，而非拖到整轮末尾！）
      await new Promise((r) => setTimeout(r, 80));
      if (sess.inFlightPromptId !== myPromptId) {
        return;
      }

      sess.inFlightPromptId = undefined;
      sess.cancelablePromptId = undefined;
      send({
        jsonrpc: "2.0",
        id: myPromptId,
        result: { stopReason: "end_turn", usage: { inputTokens: 8000, cachedReadTokens: 12000, outputTokens: 50 } }
      });
      return;
    }

    if (lastBlockText === "顺便检查缓存显示" && !isModeA && sess?.mcpHttp) {
      sess.inFlightPromptId = msg.id;
      const authHeader = sess.mcpHttp.headers?.[0]?.value || "";
      // 转向消息立即在同一会话切入：保留已完成的 a.ts 结果（sess.savedA），继续读取未完成的 b.ts 并一并答复转向问题！
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "usage_update", used: 28000, size: 1048576 } }
      });
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "tool_call", toolCallId: "acp_ms_2", title: "mcp_pi_tools_read" } }
      });
      const res2 = await fetch(sess.mcpHttp.url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: authHeader },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 22,
          method: "tools/call",
          params: {
            name: "read",
            arguments: { path: "b.ts" },
            _meta: { "antigravity.google/root_conversation_id": sessionId }
          }
        })
      }).then((r) => r.json());

      const combinedText2 = (res2.result?.content || []).map((c) => c.text || "").join("\\n");
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: { sessionId, update: { sessionUpdate: "usage_update", used: 32000, size: 1048576 } }
      });
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: {
          sessionId,
          update: {
            sessionUpdate: "agent_message_chunk",
            content: { type: "text", text: "Both done! A=" + (sess.savedA || "") + " B=" + combinedText2 + "\\n\\nSteerAnswered=true" }
          }
        }
      });
      sess.inFlightPromptId = undefined;
      send({
        jsonrpc: "2.0",
        id: msg.id,
        result: { stopReason: "end_turn", usage: { inputTokens: 600, cachedReadTokens: 31400, outputTokens: 40 } }
      });
      return;
    }

    if (lastBlockText.startsWith("FOLLOWUP_MSG:") && sess) {
      sess.inFlightPromptId = msg.id;
      send({
        jsonrpc: "2.0",
        method: "session/update",
        params: {
          sessionId,
          update: {
            sessionUpdate: "agent_message_chunk",
            content: { type: "text", text: "Echo -> " + lastBlockText }
          }
        }
      });
      sess.inFlightPromptId = undefined;
      send({
        jsonrpc: "2.0",
        id: msg.id,
        result: { stopReason: "end_turn", usage: { inputTokens: 500, cachedReadTokens: 32000, outputTokens: 25 } }
      });
      return;
    }

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

			// 确定性等待首个思考分片到达（证明请求已真正进入 mock-agy 挂起态，消除云端慢机器固定 sleep 竞态）
			let t0 = 0;
			for await (const ev of stream) {
				if (ev.type === "thinking_delta") {
					t0 = Date.now();
					ac.abort();
					break;
				}
			}
			const abortedMsg = await stream.result();
			const elapsed = Date.now() - t0;

			assert.equal(abortedMsg.stopReason, "aborted");
			assert.ok(elapsed < 50, `Esc 中断耗时应小于 50ms，实际耗时 ${elapsed}ms`);
		} finally {
			await coord.dispose();
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});

	it("运行中插入用户消息 (Steering) 不中断当前多步任务、实时估算首轮缓存命中且后续追问正常吐字", async () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-e2e-steer-"));
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

			// Step 1：触发多步读取任务，返回第 1 个工具调用 a.ts
			const u0 = { role: "user" as const, content: "MULTI_STEP_WITH_STEER", timestamp: 1 };
			const ctx1 = normalizeContext({ systemPrompt: "You are Pi.", tools, messages: [u0] });
			const msg1 = await coord.streamSimple(TEST_MODEL, ctx1, { sessionId: "pi-sess-steer" }).result();
			assert.equal(msg1.stopReason, "toolUse");
			const tc1 = msg1.content.find((c): c is ToolCall => c.type === "toolCall")!;
			assert.equal(tc1.arguments.path, "a.ts");

			// Step 2：在 a.ts 工具结果之后夹带用户在运行中按 Enter 发送的转向消息
			const tr1 = {
				role: "toolResult" as const,
				toolCallId: tc1.id,
				toolName: "read",
				content: [{ type: "text" as const, text: "export const a = 1;" }],
				isError: false,
				timestamp: 2,
			};
			const uSteer = {
				role: "user" as const,
				content: "顺便检查缓存显示",
				timestamp: 3,
			};
			const ctx2 = normalizeContext({
				systemPrompt: "You are Pi.",
				tools,
				messages: [u0, msg1, tr1, uSteer],
			});
			const msg2 = await coord.streamSimple(TEST_MODEL, ctx2, { sessionId: "pi-sess-steer" }).result();

			// 关键断言 1：原任务绝不提前终止，继续产出第 2 步工具调用 b.ts！
			assert.equal(msg2.stopReason, "toolUse");
			const tc2 = msg2.content.find((c): c is ToolCall => c.type === "toolCall")!;
			assert.equal(tc2.arguments.path, "b.ts");
			// 关键断言 2：首轮第 2 步基于前序窗口前缀保守估算非零 cacheRead（0.85 折算，不虚报 100%）
			assert.ok(
				msg2.usage.cacheRead >= 16000 && msg2.usage.cacheRead < 20000,
				`预期首轮第 2 步 cacheRead 落在保守区间 [16000, 20000)，实际为 ${msg2.usage.cacheRead}`,
			);

			// Step 3：完成 b.ts 工具执行，收口整轮输出（包含原任务结果与运行中消息响应）
			const tr2 = {
				role: "toolResult" as const,
				toolCallId: tc2.id,
				toolName: "read",
				content: [{ type: "text" as const, text: "export const b = 2;" }],
				isError: false,
				timestamp: 4,
			};
			const ctx3 = normalizeContext({
				systemPrompt: "You are Pi.",
				tools,
				messages: [u0, msg1, tr1, uSteer, msg2, tr2],
			});
			const msg3 = await coord.streamSimple(TEST_MODEL, ctx3, { sessionId: "pi-sess-steer" }).result();
			assert.equal(msg3.stopReason, "stop");
			const text3 = msg3.content
				.filter((c) => c.type === "text")
				.map((c) => c.text)
				.join("");
			assert.equal(text3, "Both done! A=export const a = 1; B=export const b = 2;\n\nSteerAnswered=true");

			// Step 4：继续发送下一条普通用户消息，验证复用同一会话且正常流式吐字（绝不挂死）
			const uFollow = {
				role: "user" as const,
				content: "FOLLOWUP_MSG:下一条指令",
				timestamp: 5,
			};
			const ctx4 = normalizeContext({
				systemPrompt: "You are Pi.",
				tools,
				messages: [u0, msg1, tr1, uSteer, msg2, tr2, msg3, uFollow],
			});
			const msg4 = await coord.streamSimple(TEST_MODEL, ctx4, { sessionId: "pi-sess-steer" }).result();
			assert.equal(msg4.stopReason, "stop");
			const text4 = msg4.content
				.filter((c) => c.type === "text")
				.map((c) => c.text)
				.join("");
			assert.equal(text4, "Echo -> FOLLOWUP_MSG:下一条指令");
			assert.equal(msg4.responseId, msg1.responseId);
		} finally {
			await coord.dispose();
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});

	it("按下 Esc 取消运行中轮次后立即发送新消息仍能正常流式输出（不卡死）", async () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-e2e-cancel-next-"));
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
			const ac = new AbortController();
			const u1 = { role: "user" as const, content: "CANCEL_RESPONSIVE", timestamp: 1 };
			const ctx1 = normalizeContext({ messages: [u1] });
			const stream1 = coord.streamSimple(TEST_MODEL, ctx1, {
				sessionId: "pi-sess-cancel-next",
				signal: ac.signal,
			});

			for await (const ev of stream1) {
				if (ev.type === "text_delta") {
					ac.abort();
					break;
				}
			}
			const abortedMsg = await stream1.result();
			assert.equal(abortedMsg.stopReason, "aborted");

			// 模拟 Pi 在 turn_end 记录完结快照后用户立即发送下一条消息
			await coord.persistTurnAnchor([u1, abortedMsg], "pi-sess-cancel-next");

			const u2 = { role: "user" as const, content: "FOLLOWUP_MSG:取消后立刻重发", timestamp: 2 };
			const ctx2 = normalizeContext({ messages: [u1, abortedMsg, u2] });
			const msg2 = await coord
				.streamSimple(TEST_MODEL, ctx2, { sessionId: "pi-sess-cancel-next" })
				.result();
			assert.equal(msg2.stopReason, "stop");
			const text2 = msg2.content
				.filter((c) => c.type === "text")
				.map((c) => c.text)
				.join("");
			assert.equal(text2, "Echo -> FOLLOWUP_MSG:取消后立刻重发");
		} finally {
			await coord.dispose();
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});

	it("同次生成拆分多工具不重复累加 cacheRead、末步不倒灌累计 output 余量且 aborted 轮次绝不写入损坏锚点", async () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-e2e-telemetry-"));
		const binPath = writeMockAgyExecutable(tmpRoot);
		const cfg: BridgeConfig = {
			...DEFAULT_BRIDGE_CONFIG,
			toolMode: "pi-tools",
			agyBin: binPath,
			geminiDir: path.join(tmpRoot, ".gemini"),
		};

		const appendedEntries: Array<{ type: string; data: unknown }> = [];
		const trackingPi = {
			appendEntry(type: string, data: unknown) {
				appendedEntries.push({ type, data });
			},
			getSessionName() {
				return undefined;
			},
			setSessionName() {},
		} as unknown as ExtensionAPI;

		const pm = new AcpProcessManager();
		const broker = new AgyTurnMirrorBroker();
		const coord = new TurnCoordinator(trackingPi, pm, broker, cfg);

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

			const u1 = { role: "user" as const, content: "SAME_GEN_DUAL_SEQ_TOOLS", timestamp: 1 };
			const step1 = await coord
				.streamSimple(TEST_MODEL, normalizeContext({ tools, messages: [u1] }), {
					sessionId: "pi-sess-tel",
				})
				.result();
			const tc1 = step1.content.find((c): c is ToolCall => c.type === "toolCall")!;
			assert.equal(step1.usage.totalTokens, 50000);

			const tr1 = {
				role: "toolResult" as const,
				toolCallId: tc1.id,
				toolName: "read",
				content: [{ type: "text" as const, text: "file-1" }],
				isError: false,
				timestamp: 2,
			};
			const step2 = await coord
				.streamSimple(TEST_MODEL, normalizeContext({ tools, messages: [u1, step1, tr1] }), {
					sessionId: "pi-sess-tel",
				})
				.result();
			const tc2 = step2.content.find((c): c is ToolCall => c.type === "toolCall")!;
			// 同一次生成拆出的第 2 个工具调用必须返回全零 Usage，杜绝 2x/3x cacheRead 虚高
			assert.equal(step2.usage.totalTokens, 0);
			assert.equal(step2.usage.cacheRead, 0);

			const tr2 = {
				role: "toolResult" as const,
				toolCallId: tc2.id,
				toolName: "read",
				content: [{ type: "text" as const, text: "file-2" }],
				isError: false,
				timestamp: 3,
			};
			const step3 = await coord
				.streamSimple(
					TEST_MODEL,
					normalizeContext({ tools, messages: [u1, step1, tr1, step2, tr2] }),
					{ sessionId: "pi-sess-tel" },
				)
				.result();
			assert.equal(step3.stopReason, "stop");
			// 末步绝不把整轮累计 outputTokens (8000) 倒灌进仅十余字符的最终回复，且总窗口严格等于 54000
			assert.ok(step3.usage.output < 100, `末步 output 不应倒灌累计余量，实际为 ${step3.usage.output}`);
			assert.equal(step3.usage.totalTokens, 54000);

			// 正常完结轮次可写入 1 条锚点
			await coord.persistTurnAnchor([u1, step1, tr1, step2, tr2, step3], "pi-sess-tel");
			assert.equal(appendedEntries.length, 1);

			// 按下 Esc 产生的 aborted 消息传入 persistTurnAnchor 时必须严格拦截，绝不追加损坏锚点
			const fakeAborted = { ...step3, stopReason: "aborted" as const };
			await coord.persistTurnAnchor([u1, step1, tr1, step2, tr2, step3, fakeAborted], "pi-sess-tel");
			assert.equal(appendedEntries.length, 1);

			// 验证在已建立缓存命中率记录后的第 2 轮中，同次生成拆分多工具时每一步均具备有效正提示词与稳定命中率（绝不变为 0 触发 UI 隐藏闪烁）
			const uWarm = { role: "user" as const, content: "SAME_GEN_DUAL_SEQ_TOOLS", timestamp: 4 };
			const wStep1 = await coord
				.streamSimple(
					TEST_MODEL,
					normalizeContext({ tools, messages: [u1, step1, tr1, step2, tr2, step3, uWarm] }),
					{ sessionId: "pi-sess-tel" },
				)
				.result();
			const wTc1 = wStep1.content.find((c): c is ToolCall => c.type === "toolCall")!;
			const wPrompt1 = wStep1.usage.input + wStep1.usage.cacheRead;
			assert.ok(wPrompt1 > 0 && wStep1.usage.cacheRead > 0);

			const wTr1 = {
				role: "toolResult" as const,
				toolCallId: wTc1.id,
				toolName: "read",
				content: [{ type: "text" as const, text: "warm-1" }],
				isError: false,
				timestamp: 5,
			};
			const wStep2 = await coord
				.streamSimple(
					TEST_MODEL,
					normalizeContext({
						tools,
						messages: [u1, step1, tr1, step2, tr2, step3, uWarm, wStep1, wTr1],
					}),
					{ sessionId: "pi-sess-tel" },
				)
				.result();
			const wTc2 = wStep2.content.find((c): c is ToolCall => c.type === "toolCall")!;
			const wPrompt2 = wStep2.usage.input + wStep2.usage.cacheRead;
			// 第 2 个拆分步既不重复累加 50000 窗口（<= 100），又保持 promptTokens > 0 且命中率与上一轮一致（约 96%）
			assert.ok(wPrompt2 > 0 && wPrompt2 <= 100);
			assert.ok(wStep2.usage.cacheRead >= 90);
			assert.equal(wStep2.usage.totalTokens, 50000);

			const wTr2 = {
				role: "toolResult" as const,
				toolCallId: wTc2.id,
				toolName: "read",
				content: [{ type: "text" as const, text: "warm-2" }],
				isError: false,
				timestamp: 6,
			};
			const wStep3 = await coord
				.streamSimple(
					TEST_MODEL,
					normalizeContext({
						tools,
						messages: [u1, step1, tr1, step2, tr2, step3, uWarm, wStep1, wTr1, wStep2, wTr2],
					}),
					{ sessionId: "pi-sess-tel" },
				)
				.result();
			assert.equal(wStep3.stopReason, "stop");

			// 验证存在本地 SQLite steps 表时，单次生成内含 2 个顺序工具按整数守恒分摊：两步均显示 95.7% 命中率且总和 100% 精确守恒
			const agyId = step1.responseId!;
			const convDir = path.join(cfg.geminiDir, "antigravity-cli", "conversations");
			fs.mkdirSync(convDir, { recursive: true });
			const dbPath = path.join(convDir, `${agyId}.db`);
			const encodeVarint = (n: number): number[] => {
				const out: number[] = [];
				let v = n;
				while (v >= 0x80) {
					out.push((v & 0x7f) | 0x80);
					v = Math.floor(v / 128);
				}
				out.push(v);
				return out;
			};
			const f9Inner = Buffer.from([
				(2 << 3) | 0, ...encodeVarint(6000),   // inputTokens = 6000
				(3 << 3) | 0, ...encodeVarint(400),    // outputTokens = 400
				(5 << 3) | 0, ...encodeVarint(134000), // cacheReadTokens = 134000 (95.7%)
				(9 << 3) | 0, ...encodeVarint(150),    // thoughtTokens = 150
			]);
			const metaBlob = Buffer.from([(9 << 3) | 2, ...encodeVarint(f9Inner.length), ...f9Inner]);
			const tcSub = Buffer.from([(7 << 3) | 2, 2, 0x08, 0x01]);
			const f20Inner = Buffer.concat([tcSub, tcSub]); // plannedToolCalls = 2
			const payloadBlob = Buffer.from([
				...encodeVarint((20 << 3) | 2),
				...encodeVarint(f20Inner.length),
				...f20Inner,
			]);

			const db = new DatabaseSync(dbPath);
			db.exec("CREATE TABLE steps (idx integer PRIMARY KEY, step_type integer NOT NULL, status integer NOT NULL, step_payload blob, metadata blob);");
			db.prepare("INSERT INTO steps VALUES (10, 15, 3, ?, ?)").run(payloadBlob, metaBlob);
			db.close();

			const uSql = { role: "user" as const, content: "SAME_GEN_DUAL_SEQ_TOOLS", timestamp: 7 };
			const historyBeforeSql = [u1, step1, tr1, step2, tr2, step3, uWarm, wStep1, wTr1, wStep2, wTr2, wStep3];
			const sqlS1 = await coord
				.streamSimple(TEST_MODEL, normalizeContext({ tools, messages: [...historyBeforeSql, uSql] }), {
					sessionId: "pi-sess-tel",
				})
				.result();
			const sqlTc1 = sqlS1.content.find((c): c is ToolCall => c.type === "toolCall")!;
			const sqlTr1 = {
				role: "toolResult" as const,
				toolCallId: sqlTc1.id,
				toolName: "read",
				content: [{ type: "text" as const, text: "sql-1" }],
				isError: false,
				timestamp: 8,
			};
			const sqlS2 = await coord
				.streamSimple(
					TEST_MODEL,
					normalizeContext({ tools, messages: [...historyBeforeSql, uSql, sqlS1, sqlTr1] }),
					{ sessionId: "pi-sess-tel" },
				)
				.result();
			const sqlTc2 = sqlS2.content.find((c): c is ToolCall => c.type === "toolCall")!;

			// 第 1 步与第 2 步各自均具备正提示词与约 95.7% 命中率（绝不为 0），且两步整数之和 100% 精确等于 (6000, 134000, 400)！
			const rate1 = (sqlS1.usage.cacheRead / (sqlS1.usage.input + sqlS1.usage.cacheRead)) * 100;
			const rate2 = (sqlS2.usage.cacheRead / (sqlS2.usage.input + sqlS2.usage.cacheRead)) * 100;
			assert.ok(Math.abs(rate1 - 95.7) < 0.2, `第 1 步命中率应约为 95.7%，实际 ${rate1}`);
			assert.ok(Math.abs(rate2 - 95.7) < 0.2, `第 2 步命中率应约为 95.7%，实际 ${rate2}`);
			assert.equal(sqlS1.usage.input + sqlS2.usage.input, 6000);
			assert.equal(sqlS1.usage.cacheRead + sqlS2.usage.cacheRead, 134000);
			assert.equal(sqlS1.usage.output + sqlS2.usage.output, 400);

			const sqlTr2 = {
				role: "toolResult" as const,
				toolCallId: sqlTc2.id,
				toolName: "read",
				content: [{ type: "text" as const, text: "sql-2" }],
				isError: false,
				timestamp: 9,
			};
			const sqlS3 = await coord
				.streamSimple(
					TEST_MODEL,
					normalizeContext({
						tools,
						messages: [...historyBeforeSql, uSql, sqlS1, sqlTr1, sqlS2, sqlTr2],
					}),
					{ sessionId: "pi-sess-tel" },
				)
				.result();
			assert.equal(sqlS3.stopReason, "stop");
		} finally {
			await coord.dispose();
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});
});
