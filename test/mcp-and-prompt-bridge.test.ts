import * as assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import type { SystemMessage, Tool } from "@earendil-works/pi-ai";
import { resolveAgyBinary } from "../src/acp/binary-resolver.ts";
import { DEFAULT_BRIDGE_CONFIG, type BridgeConfig } from "../src/config.ts";
import { LocalHttpMcpBridge, type ParkedToolCall } from "../src/mcp/http-mcp-bridge.ts";
import { filterToolsForHostMcpBridge } from "../src/mcp/pi-mcp-forwarder.ts";
import { validateToolArguments } from "../src/mcp/schema-validator.ts";
import { prepareCustomizationMount } from "../src/session/customization-mounter.ts";

describe("MCP 工具参数 JSON Schema 预校验", () => {
	const editSchema = {
		type: "object",
		properties: {
			path: { type: "string", minLength: 1 },
			line: { type: "integer", minimum: 1 },
			mode: { type: "string", enum: ["replace", "insert"] },
		},
		required: ["path"],
		additionalProperties: false,
	};

	it("通过合法参数并拦截缺失必填项、错误类型与多余字段", () => {
		const ok = validateToolArguments(
			"edit",
			{ path: "src/a.ts", line: 3, mode: "replace" },
			editSchema,
		);
		assert.equal(ok.valid, true);

		const bad = validateToolArguments(
			"edit",
			{ line: 0, mode: "invalid", extra: true },
			editSchema,
		);
		assert.equal(bad.valid, false);
		assert.ok(bad.formattedError?.includes("$.path"));
		assert.ok(bad.formattedError?.includes("$.line"));
		assert.ok(bad.formattedError?.includes("$.mode"));
		assert.ok(bad.formattedError?.includes("$.extra"));
	});
});

describe("本地 Streamable HTTP MCP 服务端", () => {
	it("完整处理鉴权、OAuth 元数据 404、空体 DELETE、参数自愈、挂起结算与超时剥离", async () => {
		const bridge = new LocalHttpMcpBridge(5_000);
		const tools: Tool[] = [
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

		const parkedList: ParkedToolCall[] = [];
		let holdTimedOut = false;

		const token = bridge.registerSessionBinding({
			agySessionId: "sess-1",
			tools,
			onParkedCall: (call) => parkedList.push(call),
			onHoldTimeout: async () => {
				holdTimedOut = true;
			},
		});

		try {
			const port = await bridge.ensureListening();
			const baseUrl = `http://127.0.0.1:${port}`;

			const wellKnown = await fetch(`${baseUrl}/.well-known/oauth-protected-resource/mcp`);
			assert.equal(wellKnown.status, 404);

			const unauth = await fetch(`${baseUrl}/mcp`, { method: "POST", body: "{}" });
			assert.equal(unauth.status, 401);

			const authHeaders = {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			};

			const delRes = await fetch(`${baseUrl}/mcp`, {
				method: "DELETE",
				headers: { Authorization: `Bearer ${token}` },
			});
			assert.equal(delRes.status, 200);

			const discoverRes = (await (
				await fetch(`${baseUrl}/mcp`, {
					method: "POST",
					headers: authHeaders,
					body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "server/discover" }),
				})
			).json()) as { error?: { code: number } };
			assert.equal(discoverRes.error?.code, -32601);

			const listRes = (await (
				await fetch(`${baseUrl}/mcp`, {
					method: "POST",
					headers: authHeaders,
					body: JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/list" }),
				})
			).json()) as { result: { tools: Array<{ name: string }> } };
			assert.equal(listRes.result.tools.length, 1);
			assert.equal(listRes.result.tools[0].name, "read");

			const invalidCallRes = (await (
				await fetch(`${baseUrl}/mcp`, {
					method: "POST",
					headers: authHeaders,
					body: JSON.stringify({
						jsonrpc: "2.0",
						id: 3,
						method: "tools/call",
						params: { name: "read", arguments: {} },
					}),
				})
			).json()) as { result: { isError: boolean; content: Array<{ text: string }> } };
			assert.equal(invalidCallRes.result.isError, true);
			assert.equal(parkedList.length, 0);

			bridge.recordAcpToolAnnouncement("sess-1", "mcp_pi_tools_read");
			const callPromise = fetch(`${baseUrl}/mcp`, {
				method: "POST",
				headers: authHeaders,
				body: JSON.stringify({
					jsonrpc: "2.0",
					id: 4,
					method: "tools/call",
					params: { name: "read", arguments: { path: "README.md" } },
				}),
			}).then((r) => r.json());

			await new Promise((r) => setTimeout(r, 40));
			assert.equal(parkedList.length, 1);
			parkedList[0].settle({
				content: [
					{ type: "text", text: "file content" },
					{ type: "image", data: "YWJj", mimeType: "image/png" },
				],
			});

			const settledJson = (await callPromise) as {
				result: { isError: boolean; content: Array<{ type: string }> };
			};
			assert.equal(settledJson.result.isError, false);
			assert.equal(settledJson.result.content.length, 2);
			assert.equal(settledJson.result.content[1].type, "image");
			assert.equal(holdTimedOut, false);
		} finally {
			await bridge.dispose();
		}

		// 验证超过守护阈值时自动触发预取消剥离并释放连接
		const shortHoldBridge = new LocalHttpMcpBridge(60);
		let detachedCall: ParkedToolCall | undefined;
		let timeoutCallbackCount = 0;
		const shortToken = shortHoldBridge.registerSessionBinding({
			agySessionId: "sess-2",
			tools,
			onParkedCall: (c) => {
				detachedCall = c;
			},
			onHoldTimeout: async () => {
				timeoutCallbackCount++;
			},
		});

		try {
			const p2 = await shortHoldBridge.ensureListening();
			shortHoldBridge.recordAcpToolAnnouncement("sess-2", "mcp_pi_tools_read");
			const detachJson = (await (
				await fetch(`http://127.0.0.1:${p2}/mcp`, {
					method: "POST",
					headers: {
						"Content-Type": "application/json",
						Authorization: `Bearer ${shortToken}`,
					},
					body: JSON.stringify({
						jsonrpc: "2.0",
						id: 99,
						method: "tools/call",
						params: { name: "read", arguments: { path: "slow.ts" } },
					}),
				})
			).json()) as { result: { isError: boolean } };

			assert.equal(detachJson.result.isError, true);
			assert.equal(timeoutCallbackCount, 1);
			assert.equal(detachedCall?.isDetached, true);
		} finally {
			await shortHoldBridge.dispose();
		}
	});
});

describe("官方原生 Plugin 生态挂载、纯净模式上下文与零系统提示词注入安全红线", () => {
	it("静态扫描全部源码确保不存在任何 _meta.antigravity.systemPrompt 注入逻辑", () => {
		const testDir = path.dirname(fileURLToPath(import.meta.url));
		const srcRoot = path.resolve(testDir, "..", "src");

		const collectTsFiles = (dir: string): string[] => {
			const out: string[] = [];
			for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
				const full = path.join(dir, entry.name);
				if (entry.isDirectory()) {
					out.push(...collectTsFiles(full));
				} else if (entry.isFile() && entry.name.endsWith(".ts")) {
					out.push(full);
				}
			}
			return out;
		};

		for (const file of collectTsFiles(srcRoot)) {
			const source = fs.readFileSync(file, "utf8");
			// 仅允许 protocol.ts 在只读响应类型中声明服务端通告的能力，禁止出现任何请求注入或 append 调用
			if (!file.endsWith(path.join("acp", "protocol.ts"))) {
				assert.equal(
					source.includes("systemPrompt"),
					false,
					`源码文件 ${file} 不得包含任何 systemPrompt 触碰或注入字段`,
				);
			}
		}
	});

	it("pi-tools 纯净模式下生成完整 <workspace_context> 用户上下文块，且兼容 edit 工具字符串化 edits 归一化", async () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-pure-ctx-"));
		try {
			const wsDir = path.join(tmpRoot, "repo");
			const extSkillDir = path.join(tmpRoot, "external-pi-skills", "calc-guard");
			fs.mkdirSync(path.join(wsDir, ".git"), { recursive: true });
			fs.mkdirSync(extSkillDir, { recursive: true });

			const wsAgentsMd = path.join(wsDir, "AGENTS.md");
			const globalPiAgentsMd = path.join(tmpRoot, "global-AGENTS.md");
			const extSkillMd = path.join(extSkillDir, "SKILL.md");

			fs.writeFileSync(wsAgentsMd, "# Workspace Rule\nKeep commits atomic.\n");
			fs.writeFileSync(globalPiAgentsMd, "# Global Pi Rule\nReport progress briefly.\n");
			fs.writeFileSync(
				extSkillMd,
				"---\nname: calc-guard\ndescription: Calc verification rules.\n---\n# Calc Guard\n",
			);

			const sysMsg: SystemMessage = {
				role: "system",
				content: "",
				toolsAdded: [
					{ name: "read", description: "r", parameters: {} },
					{ name: "edit", description: "e", parameters: {} },
					{ name: "bash", description: "b", parameters: {} },
				],
				sections: {
					preamble: "You are a coding assistant running inside pi...",
					project_context: [
						"<project_context>",
						`<project_instructions path="${wsAgentsMd}">workspace rule</project_instructions>`,
						`<project_instructions path="${globalPiAgentsMd}">global pi rule</project_instructions>`,
						"</project_context>",
					].join("\n"),
					skills: [
						"<skills>",
						`<available_skills><skill><name>calc-guard</name><location>${extSkillMd}</location></skill></available_skills>`,
						"</skills>",
					].join("\n"),
				},
				timestamp: 1,
			};

			const pureCfg: BridgeConfig = {
				...DEFAULT_BRIDGE_CONFIG,
				toolMode: "pi-tools",
				geminiDir: path.join(tmpRoot, ".gemini"),
			};

			const pureMount = prepareCustomizationMount([sysMsg], pureCfg, wsDir);
			assert.equal(pureMount.mountedPluginPaths.length, 0);
			assert.deepEqual(pureMount.additionalDirectories, [fs.realpathSync(extSkillDir)]);
			assert.ok(pureMount.workspaceContextBlock);
			// 绝不包含宿主系统提示词 preamble，同时完整保留工作区与全局 AGENTS.md 及外部技能索引
			assert.ok(!pureMount.workspaceContextBlock.includes("running inside pi"));
			assert.ok(pureMount.workspaceContextBlock.includes("Keep commits atomic."));
			assert.ok(pureMount.workspaceContextBlock.includes("Report progress briefly."));
			assert.ok(pureMount.workspaceContextBlock.includes("calc-guard"));
			assert.ok(pureMount.workspaceContextBlock.includes("<tool_guidelines>"));

			// 验证 LocalHttpMcpBridge 对 edit 工具字符串化 edits 参数的自动兼容归一化
			const bridge = new LocalHttpMcpBridge(5_000);
			const parked: ParkedToolCall[] = [];
			const editTool: Tool = {
				name: "edit",
				description: "Edit file",
				parameters: {
					type: "object",
					properties: {
						path: { type: "string" },
						edits: {
							type: "array",
							items: {
								type: "object",
								properties: {
									oldText: { type: "string" },
									newText: { type: "string" },
								},
								required: ["oldText", "newText"],
							},
						},
					},
					required: ["path", "edits"],
				},
			};
			const token = bridge.registerSessionBinding({
				agySessionId: "sess-edit",
				tools: [editTool],
				onParkedCall: (c) => parked.push(c),
				onHoldTimeout: async () => {},
			});
			try {
				const port = await bridge.ensureListening();
				bridge.recordAcpToolAnnouncement("sess-edit", "mcp_pi_tools_edit");
				const callFuture = fetch(`http://127.0.0.1:${port}/mcp`, {
					method: "POST",
					headers: {
						"Content-Type": "application/json",
						Authorization: `Bearer ${token}`,
					},
					body: JSON.stringify({
						jsonrpc: "2.0",
						id: 77,
						method: "tools/call",
						params: {
							name: "edit",
							arguments: {
								path: "calc.ts",
								edits: JSON.stringify([{ oldText: "a + b", newText: "a * b" }]),
							},
						},
					}),
				}).then((r) => r.json());

				await new Promise((r) => setTimeout(r, 30));
				assert.equal(parked.length, 1);
				assert.ok(Array.isArray(parked[0].arguments.edits));
				parked[0].settle({ content: [{ type: "text", text: "OK" }] });
				const out = (await callFuture) as { result: { isError: boolean } };
				assert.equal(out.result.isError, false);
			} finally {
				await bridge.dispose();
			}
		} finally {
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});

	it("生成符合 agy plugin validate --strict 规范的原生挂载包并自动去重工作区已有配置", () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-native-mount-"));
		try {
			const piAgentDir = path.join(tmpRoot, "pi-agent");
			const wsDir = path.join(tmpRoot, "repo");
			const extSkillDir = path.join(tmpRoot, "external-pi-skills", "my-skill");
			const nativeSkillDir = path.join(wsDir, ".agents", "skills", "shared-skill");
			fs.mkdirSync(path.join(wsDir, ".git"), { recursive: true });
			fs.mkdirSync(extSkillDir, { recursive: true });
			fs.mkdirSync(nativeSkillDir, { recursive: true });

			const wsAgentsMd = path.join(wsDir, "AGENTS.md");
			const globalPiAgentsMd = path.join(tmpRoot, "global-AGENTS.md");
			const extSkillMd = path.join(extSkillDir, "SKILL.md");
			const nativeSkillMd = path.join(nativeSkillDir, "SKILL.md");

			fs.writeFileSync(wsAgentsMd, "# Workspace Rule\nAlready discovered by AGY.\n");
			fs.writeFileSync(globalPiAgentsMd, "# Global Pi Rule\nUse strict checks.\n");
			fs.writeFileSync(
				extSkillMd,
				"---\nname: my-skill\ndescription: Use when testing mounted Pi skills.\n---\n# My Skill\n",
			);
			fs.writeFileSync(
				nativeSkillMd,
				"---\nname: shared-skill\ndescription: Already native.\n---\n# Shared\n",
			);

			const sysMsg: SystemMessage = {
				role: "system",
				content: "",
				sections: {
					preamble: "You are a coding assistant running inside pi...",
					project_context: [
						"<project_context>",
						`<project_instructions path="${wsAgentsMd}">workspace rule</project_instructions>`,
						`<project_instructions path="${globalPiAgentsMd}">global pi rule</project_instructions>`,
						"</project_context>",
					].join("\n"),
					skills: [
						"<skills>",
						`<skill><name>shared-skill</name><location>${nativeSkillMd}</location></skill>`,
						`<skill><name>my-skill</name><location>${extSkillMd}</location></skill>`,
						"</skills>",
					].join("\n"),
				},
				timestamp: 1,
			};

			const cfg: BridgeConfig = {
				...DEFAULT_BRIDGE_CONFIG,
				toolMode: "hybrid",
				bridgePiAgentsMd: true,
				bridgePiSkills: true,
				agyInheritCustomizations: true,
				geminiDir: path.join(tmpRoot, ".gemini"),
			};

			const mounted = prepareCustomizationMount([sysMsg], cfg, wsDir, {
				PI_CODING_AGENT_DIR: piAgentDir,
			});

			assert.equal(mounted.mountedPluginPaths.length, 1);
			assert.equal(mounted.additionalDirectories.length, 1);

			const pluginDir = mounted.mountedPluginPaths[0];
			const rulesMd = fs.readFileSync(path.join(pluginDir, "rules", "AGENTS.md"), "utf8");
			const skillsManifest = JSON.parse(
				fs.readFileSync(path.join(pluginDir, "skills.json"), "utf8"),
			) as { entries: Array<{ path: string; include_only: string[] }> };

			// 绝不包含宿主 preamble，且自动剔除已被 AGY 原生扫描的 wsAgentsMd 与 nativeSkillDir
			assert.ok(!rulesMd.includes("running inside pi"));
			assert.ok(!rulesMd.includes("Workspace Rule"));
			assert.ok(rulesMd.includes("Global Pi Rule"));
			assert.equal(skillsManifest.entries.length, 1);
			assert.deepEqual(skillsManifest.entries[0].include_only, ["my-skill"]);

			// 若本机已安装 agy，执行官方严格插件校验器确认 0 error / 0 warning
			const agyBin = resolveAgyBinary();
			if (agyBin) {
				const validateRes = spawnSync(
					agyBin.binPath,
					["plugin", "validate", pluginDir, "--strict", "--json"],
					{
						encoding: "utf8",
						shell: agyBin.isWindowsWrapperScript,
					},
				);
				assert.equal(validateRes.status, 0, `agy plugin validate failed: ${validateRes.stderr}`);
				const report = JSON.parse(validateRes.stdout) as {
					components: { rules: number; skills: number };
				};
				assert.equal(report.components.rules, 1);
				assert.equal(report.components.skills, 1);
			}

			// hybrid 工具过滤应剔除基础 read/edit/bash，保留第三方工具
			const allTools: Tool[] = [
				{ name: "read", description: "r", parameters: {} },
				{ name: "bash", description: "b", parameters: {} },
				{ name: "agy_edit", description: "m", parameters: {} },
				{ name: "custom_deploy", description: "d", parameters: {} },
			];
			const hostTools = filterToolsForHostMcpBridge(allTools, cfg);
			assert.deepEqual(
				hostTools.map((t) => t.name),
				["custom_deploy"],
			);
		} finally {
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});
});
