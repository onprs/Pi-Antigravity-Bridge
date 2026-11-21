import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";
import { resolveAgyBinary } from "../src/acp/binary-resolver.ts";
import type { AcpAvailableModel, AcpConfigOption } from "../src/acp/protocol.ts";
import {
	aggregateAcpModels,
	resolveSessionModelSelection,
} from "../src/catalog/model-discovery.ts";
import {
	loadBridgeConfig,
	resolveEffectiveFlags,
} from "../src/config.ts";

const SAMPLE_AVAILABLE_MODELS: AcpAvailableModel[] = [
	{ modelId: "gemini-4-neon-high", name: "Gemini 4 Neon (High)" },
	{ modelId: "gemini-4-neon-xhigh", name: "Gemini 4 Neon (XHigh)" },
	{ modelId: "gemini-4-neon-medium", name: "Gemini 4 Neon (Medium)" },
	{ modelId: "gemini-4-neon-low", name: "Gemini 4 Neon (Low)" },
	{ modelId: "gemini-4-carbon-medium", name: "Gemini 4 Carbon (Medium)" },
	{ modelId: "gemini-4-carbon-high", name: "Gemini 4 Carbon (High)" },
	{ modelId: "gemini-4-carbon-low", name: "Gemini 4 Carbon (Low)" },
	{ modelId: "gemini-pro-orchestrator", name: "Orchestrator (Carbon)" },
	{ modelId: "claude-opus-5-5-low", name: "Claude Opus 5.5 (Low)" },
	{ modelId: "claude-opus-5-5-medium", name: "Claude Opus 5.5 (Medium)" },
	{ modelId: "claude-opus-5-5-high", name: "Claude Opus 5.5 (High)" },
	{ modelId: "gpt-oss-120b-medium", name: "GPT-OSS 120B (Medium)" },
];

const SAMPLE_CONFIG_OPTIONS: AcpConfigOption[] = [
	{
		id: "model",
		name: "Model",
		type: "select",
		currentValue: "gemini-4-neon",
		options: [
			{ value: "gemini-4-neon", name: "Gemini 4 Neon" },
			{ value: "gemini-4-carbon", name: "Gemini 4 Carbon" },
			{ value: "gemini-pro-orchestrator", name: "Orchestrator (Carbon)" },
			{ value: "claude-opus-5-5", name: "Claude Opus 5.5" },
			{ value: "gpt-oss-120b-medium", name: "GPT-OSS 120B (Medium)" },
		],
	},
	{
		id: "thought_level",
		name: "Thinking Level",
		type: "select",
		currentValue: "high",
		options: [
			{ value: "low", name: "Low" },
			{ value: "medium", name: "Medium" },
			{ value: "high", name: "High" },
			{ value: "xhigh", name: "Extra" },
		],
	},
];

describe("配置加载与有效开关推导", () => {
	it("遵循环境变量 > 受信项目配置 > 全局配置 > 默认值优先级，并隔离未受信项目配置", () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-cfg-"));
		try {
			const agentDir = path.join(tmpRoot, "agent");
			const projectDir = path.join(tmpRoot, "workspace");
			fs.mkdirSync(agentDir, { recursive: true });
			fs.mkdirSync(path.join(projectDir, ".pi"), { recursive: true });

			fs.writeFileSync(
				path.join(agentDir, "antigravity.json"),
				JSON.stringify({
					toolMode: "pi-tools",
					bridgePiMcp: true,
					mcpHoldLimitMs: 150000,
				}),
			);
			fs.writeFileSync(
				path.join(projectDir, ".pi", "antigravity.json"),
				JSON.stringify({
					toolMode: "agy-tools",
					bridgePiMcp: false,
				}),
			);

			// 未受信时不加载项目级覆盖
			const untrusted = loadBridgeConfig({
				cwd: projectDir,
				isProjectTrusted: false,
				env: { PI_CODING_AGENT_DIR: agentDir },
			});
			assert.equal(untrusted.toolMode, "pi-tools");
			assert.equal(untrusted.bridgePiMcp, true);

			// 受信时加载项目级覆盖
			const trusted = loadBridgeConfig({
				cwd: projectDir,
				isProjectTrusted: true,
				env: { PI_CODING_AGENT_DIR: agentDir },
			});
			assert.equal(trusted.toolMode, "agy-tools");
			assert.equal(trusted.bridgePiMcp, false);

			// 环境变量最高优先级覆盖
			const envOverridden = loadBridgeConfig({
				cwd: projectDir,
				isProjectTrusted: true,
				env: {
					PI_CODING_AGENT_DIR: agentDir,
					PI_AGY_TOOL_MODE: "hybrid",
					PI_AGY_BRIDGE_MCP: "on",
				},
			});
			assert.equal(envOverridden.toolMode, "hybrid");
			assert.equal(envOverridden.bridgePiMcp, true);
		} finally {
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});

	it("开箱默认使用 pi-tools，且 auto 开关在 pi-tools 与 agy-tools 模式下自动切换合理默认值", () => {
		const tmpEmptyAgentDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-default-"));
		try {
			const defaultCfg = loadBridgeConfig({ env: { PI_CODING_AGENT_DIR: tmpEmptyAgentDir } });
			assert.equal(defaultCfg.toolMode, "pi-tools");
			const defaultFlags = resolveEffectiveFlags(defaultCfg);
			assert.equal(defaultFlags.bridgePiMcp, true);
			assert.equal(defaultFlags.bridgePiSkills, true);
			assert.equal(defaultFlags.bridgePiAgentsMd, true);
			assert.equal(defaultFlags.agyInheritCustomizations, false);
		} finally {
			fs.rmSync(tmpEmptyAgentDir, { recursive: true, force: true });
		}

		const agyToolsCfg = loadBridgeConfig({ env: { PI_AGY_TOOL_MODE: "agy-tools" } });
		const agyFlags = resolveEffectiveFlags(agyToolsCfg);
		assert.equal(agyFlags.bridgePiMcp, false);
		assert.equal(agyFlags.bridgePiSkills, false);
		assert.equal(agyFlags.bridgePiAgentsMd, false);
		assert.equal(agyFlags.agyInheritCustomizations, true);
	});
});

describe("跨平台可执行文件定位", () => {
	it("在 Windows 模拟环境下优先选择 .exe 并识别 .cmd 包装脚本", () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-bin-win-"));
		try {
			const binDir = path.join(tmpRoot, "Programs", "Antigravity", "bin");
			fs.mkdirSync(binDir, { recursive: true });
			const exePath = path.join(binDir, "agy.exe");
			const cmdPath = path.join(binDir, "agy.cmd");
			fs.writeFileSync(cmdPath, "@echo off\r\n");
			fs.writeFileSync(exePath, "MZ");

			const foundExe = resolveAgyBinary({
				platform: "win32",
				homeDir: tmpRoot,
				env: {
					LOCALAPPDATA: tmpRoot,
					PATH: "",
					PATHEXT: ".CMD;.EXE",
				},
			});
			assert.ok(foundExe);
			assert.equal(foundExe.binPath, exePath);
			assert.equal(foundExe.isWindowsWrapperScript, false);

			fs.unlinkSync(exePath);
			const foundCmd = resolveAgyBinary({
				platform: "win32",
				homeDir: tmpRoot,
				env: {
					LOCALAPPDATA: tmpRoot,
					PATH: binDir,
					PATHEXT: ".CMD;.EXE",
				},
			});
			assert.ok(foundCmd);
			assert.equal(foundCmd.binPath, cmdPath);
			assert.equal(foundCmd.isWindowsWrapperScript, true);
		} finally {
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});
});

describe("双轨模型家族聚合与 Thinking 档位解析", () => {
	it("将多档位变体聚合为单一模型条目，同时完整保留单档位模型原始 ID", () => {
		const { models, modelEfforts } = aggregateAcpModels(
			SAMPLE_AVAILABLE_MODELS,
			SAMPLE_CONFIG_OPTIONS,
		);

		const ids = models.map((m) => m.id);
		assert.deepEqual(ids, [
			"gemini-4-neon",
			"gemini-4-carbon",
			"gemini-pro-orchestrator",
			"claude-opus-5-5",
			"gpt-oss-120b-medium",
		]);

		assert.deepEqual(modelEfforts["gemini-4-neon"], ["low", "medium", "high", "xhigh"]);
		assert.deepEqual(modelEfforts["gemini-4-carbon"], ["low", "medium", "high"]);
		assert.deepEqual(modelEfforts["gemini-pro-orchestrator"], []);
		assert.deepEqual(modelEfforts["gpt-oss-120b-medium"], ["medium"]);

		const carbon = models.find((m) => m.id === "gemini-4-carbon")!;
		assert.equal(carbon.reasoning, true);
		assert.equal(carbon.thinkingLevelMap?.high, "high");
		assert.equal(carbon.thinkingLevelMap?.xhigh, null);
		assert.equal(carbon.thinkingLevelMap?.minimal, null);

		const orchestrator = models.find((m) => m.id === "gemini-pro-orchestrator")!;
		assert.equal(orchestrator.reasoning, false);
	});

	it("兼容双阶段裸模型下发与纯后缀模型 ID 回退", () => {
		// 1. 标准裸 ID + thought_level
		const neonSel = resolveSessionModelSelection(
			"gemini-4-neon",
			"xhigh",
			SAMPLE_CONFIG_OPTIONS,
			SAMPLE_AVAILABLE_MODELS,
		);
		assert.equal(neonSel.configModelValue, "gemini-4-neon");
		assert.equal(neonSel.configThoughtLevel, "xhigh");

		// 2. Carbon 请求不支持的 xhigh 时自动就近降级至 high
		const carbonClamped = resolveSessionModelSelection(
			"gemini-4-carbon",
			"xhigh",
			SAMPLE_CONFIG_OPTIONS,
			SAMPLE_AVAILABLE_MODELS,
		);
		assert.equal(carbonClamped.configModelValue, "gemini-4-carbon");
		assert.equal(carbonClamped.configThoughtLevel, "high");

		// 3. 服务端仅提供后缀 ID 时自动拼接后缀下发
		const suffixOnlyOpts: AcpConfigOption[] = [
			{
				id: "model",
				name: "Model",
				type: "select",
				currentValue: "gemini-4-neon-high",
				options: SAMPLE_AVAILABLE_MODELS.map((m) => ({ value: m.modelId, name: m.name })),
			},
		];
		const fallbackSel = resolveSessionModelSelection(
			"gemini-4-neon",
			"medium",
			suffixOnlyOpts,
			SAMPLE_AVAILABLE_MODELS,
		);
		assert.equal(fallbackSel.configModelValue, "gemini-4-neon-medium");
		assert.equal(fallbackSel.configThoughtLevel, undefined);
		assert.equal(fallbackSel.effectiveEffort, "medium");
	});
});
