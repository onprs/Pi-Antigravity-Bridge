import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import type { Tool } from "@earendil-works/pi-ai";
import type { AcpEnvVar, AcpHttpHeader, AcpMcpServerSpec } from "../acp/protocol.ts";
import {
	type BridgeConfig,
	resolveEffectiveFlags,
	resolvePiAgentDir,
} from "../config.ts";

/** 与 AGY 原生内置工具能力重叠的 Pi 基础工具集（在 hybrid 模式下剔除以避免双套工具歧义） */
const OVERLAPPING_BUILTIN_TOOLS = new Set<string>([
	"read",
	"edit",
	"write",
	"bash",
	"powershell",
	"grep",
	"find",
	"ls",
]);

const PI_MCP_RESOURCE_TOOLS = new Set<string>([
	"list_mcp_resources",
	"list_mcp_resource_templates",
	"read_mcp_resource",
]);

/** 判断某工具是否属于 Pi MCP 工具体系 */
export function isPiMcpRelatedTool(toolName: string): boolean {
	return toolName.startsWith("mcp__") || PI_MCP_RESOURCE_TOOLS.has(toolName);
}

/** 判断某工具是否为本扩展在模式 A 注册的镜像代理工具 */
export function isAgyMirrorToolName(toolName: string): boolean {
	return toolName.startsWith("agy_");
}

/**
 * 根据当前 `toolMode` 与 `bridgePiMcp` 开关过滤需要挂载到宿主本地 HTTP MCP Server 的工具集合。
 */
export function filterToolsForHostMcpBridge(
	declaredTools: readonly Tool[],
	config: BridgeConfig,
): Tool[] {
	const flags = resolveEffectiveFlags(config);

	if (flags.toolMode === "agy-tools") {
		if (!flags.bridgePiMcp || config.piMcpBridgeStrategy !== "via-host") {
			return [];
		}
		// agy-tools 开启 via-host MCP 桥接时，仅暴露 Pi MCP 相关工具（含 codemode / tool_search）
		const hasAnyMcpTool = declaredTools.some(
			(t) => isPiMcpRelatedTool(t.name) || t.name === "codemode" || t.name === "tool_search",
		);
		if (!hasAnyMcpTool) return [];
		return declaredTools.filter(
			(t) =>
				!isAgyMirrorToolName(t.name) &&
				(isPiMcpRelatedTool(t.name) || t.name === "codemode" || t.name === "tool_search"),
		);
	}

	if (flags.toolMode === "hybrid") {
		const filtered = declaredTools.filter((tool) => {
			if (isAgyMirrorToolName(tool.name)) return false;
			if (OVERLAPPING_BUILTIN_TOOLS.has(tool.name)) return false;
			if (!flags.bridgePiMcp || config.piMcpBridgeStrategy === "direct-config") {
				if (isPiMcpRelatedTool(tool.name) || tool.name === "codemode") {
					return false;
				}
			}
			return true;
		});
		return filtered;
	}

	// pi-tools（模式 B）
	const filtered = declaredTools.filter((tool) => {
		if (isAgyMirrorToolName(tool.name)) return false;
		if (!flags.bridgePiMcp) {
			if (isPiMcpRelatedTool(tool.name) || tool.name === "codemode") {
				return false;
			}
		}
		return true;
	});

	return filtered;
}

/** 计算工具声明集合的稳定内容指纹，用于检测动态工具变更（例如 tool_search 激活新工具后） */
export function computeToolsFingerprint(tools: readonly Tool[]): string {
	const normalized = tools.map((t) => ({
		name: t.name,
		description: t.description,
		parameters: t.parameters,
	}));
	return crypto.createHash("sha256").update(JSON.stringify(normalized)).digest("hex");
}

function interpolateEnvVars(raw: string, env: NodeJS.ProcessEnv): string {
	return raw.replace(/\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)/g, (_match, braced, bare) => {
		const key = (braced || bare) as string;
		return env[key] ?? "";
	});
}

interface RawPiMcpEntry {
	enabled?: boolean;
	disabled?: boolean;
	command?: string;
	args?: unknown[];
	env?: Record<string, unknown>;
	url?: string;
	headers?: Record<string, unknown>;
}

function parsePiMcpConfigFile(filePath: string): Record<string, RawPiMcpEntry> {
	try {
		if (!fs.existsSync(filePath)) return {};
		const raw = JSON.parse(fs.readFileSync(filePath, "utf8")) as Record<string, unknown>;
		const servers =
			raw.mcpServers && typeof raw.mcpServers === "object" && !Array.isArray(raw.mcpServers)
				? (raw.mcpServers as Record<string, RawPiMcpEntry>)
				: {};
		return servers;
	} catch {
		return {};
	}
}

/**
 * 在 `agy-tools` / `hybrid` 模式下采用 `direct-config` 策略时，
 * 读取 `~/.pi/agent/mcp.json` 与受信项目的 `.pi/mcp.json` 并转换为 ACP `mcpServers` 规格。
 */
export function loadDirectPiMcpServerSpecs(options: {
	cwd: string;
	isProjectTrusted: boolean;
	processPerSession: boolean;
	env?: NodeJS.ProcessEnv;
}): AcpMcpServerSpec[] {
	const env = options.env ?? process.env;
	const globalPath = path.join(resolvePiAgentDir(env), "mcp.json");
	const mergedEntries: Record<string, RawPiMcpEntry> = {
		...parsePiMcpConfigFile(globalPath),
	};

	if (options.isProjectTrusted) {
		const projectPath = path.join(options.cwd, ".pi", "mcp.json");
		Object.assign(mergedEntries, parsePiMcpConfigFile(projectPath));
	}

	const specs: AcpMcpServerSpec[] = [];
	for (const [name, entry] of Object.entries(mergedEntries)) {
		if (!entry || typeof entry !== "object") continue;
		if (entry.enabled === false || entry.disabled === true) continue;
		// 避免与宿主内置通道名冲突
		const safeName = name === "pi_tools" ? "pi_user_tools" : name;

		if (typeof entry.url === "string" && entry.url.trim().length > 0) {
			if (!options.processPerSession) {
				continue;
			}
			const headers: AcpHttpHeader[] = [];
			if (entry.headers && typeof entry.headers === "object") {
				for (const [hk, hv] of Object.entries(entry.headers)) {
					if (typeof hv === "string") {
						headers.push({ name: hk, value: interpolateEnvVars(hv, env) });
					}
				}
			}
			specs.push({
				type: "http",
				name: safeName,
				url: interpolateEnvVars(entry.url.trim(), env),
				headers: headers.length > 0 ? headers : undefined,
			});
			continue;
		}

		if (typeof entry.command === "string" && entry.command.trim().length > 0) {
			const args = Array.isArray(entry.args)
				? entry.args
						.filter((a): a is string => typeof a === "string")
						.map((a) => interpolateEnvVars(a, env))
				: undefined;
			const envList: AcpEnvVar[] = [];
			if (entry.env && typeof entry.env === "object") {
				for (const [ek, ev] of Object.entries(entry.env)) {
					if (typeof ev === "string") {
						envList.push({ name: ek, value: interpolateEnvVars(ev, env) });
					}
				}
			}
			specs.push({
				type: "stdio",
				name: safeName,
				command: interpolateEnvVars(entry.command.trim(), env),
				args,
				env: envList.length > 0 ? envList : undefined,
			});
		}
	}

	return specs;
}
