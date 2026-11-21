import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import {
	getCurrentSystemMessage,
	getCurrentTools,
	type Tool,
	type TranscriptMessages,
} from "@earendil-works/pi-ai";
import {
	type BridgeConfig,
	resolveEffectiveFlags,
	resolvePiAgentDir,
} from "../config.ts";

/** Antigravity 官方单规则文件上限为 24,000 字节 */
const MAX_RULE_FILE_BYTES = 24_000;

export interface PreparedCustomizationMount {
	/** 生成的原生 Antigravity 挂载 Plugin 目录路径（pi-tools 纯净模式或无挂载项时为空数组） */
	mountedPluginPaths: string[];
	/** 需通过 ACP `additionalDirectories` 授权可读的工作区外目录白名单 */
	additionalDirectories: string[];
	/** 在 `pi-tools` 纯净模式下供会话首条用户消息前置携带的结构化工作区规则与技能上下文块 */
	workspaceContextBlock?: string;
	/** 当前挂载状态的 SHA-256 指纹（用于检测桥接开关或规则变更） */
	mountDigest: string;
}

function resolveRealPathSafe(targetPath: string): string {
	try {
		return fs.realpathSync(targetPath);
	} catch {
		return path.resolve(targetPath);
	}
}

function findGitRepoRoot(startDir: string): string | undefined {
	let current = path.resolve(startDir);
	while (true) {
		if (fs.existsSync(path.join(current, ".git"))) {
			return current;
		}
		const parent = path.dirname(current);
		if (parent === current) {
			return undefined;
		}
		current = parent;
	}
}

/**
 * 收集 AGY 在开启 `inheritCustomizations: true` 时原生自动发现的规则文件集合，
 * 避免同一份 `AGENTS.md` / `GEMINI.md` 被原生发现与桥接插件重复挂载。
 */
function collectAgyAutoLoadedRuleFiles(cwd: string, geminiDir: string): Set<string> {
	const autoLoaded = new Set<string>();
	const repoRoot = findGitRepoRoot(cwd);
	let current = path.resolve(cwd);

	while (true) {
		for (const rel of [
			"AGENTS.md",
			"GEMINI.md",
			path.join(".agents", "AGENTS.md"),
			path.join(".agents", "GEMINI.md"),
		]) {
			const candidate = path.join(current, rel);
			if (fs.existsSync(candidate)) {
				autoLoaded.add(resolveRealPathSafe(candidate));
			}
		}
		if (repoRoot && current === repoRoot) break;
		const parent = path.dirname(current);
		if (parent === current) break;
		current = parent;
		if (!repoRoot) break;
	}

	for (const globalRel of [
		"AGENTS.md",
		"GEMINI.md",
		path.join("config", "AGENTS.md"),
		path.join("config", "GEMINI.md"),
	]) {
		const globalCandidate = path.join(geminiDir, globalRel);
		if (fs.existsSync(globalCandidate)) {
			autoLoaded.add(resolveRealPathSafe(globalCandidate));
		}
	}

	return autoLoaded;
}

/**
 * 收集 AGY 在开启 `inheritCustomizations: true` 时原生自动发现的 `SKILL.md` 真实路径集合。
 */
function collectAgyNativeSkillFiles(cwd: string, geminiDir: string): Set<string> {
	const result = new Set<string>();
	const roots: string[] = [
		path.join(geminiDir, "config", "skills"),
		path.join(geminiDir, "skills"),
		path.join(geminiDir, "antigravity-cli", "skills"),
	];

	const repoRoot = findGitRepoRoot(cwd);
	let current = path.resolve(cwd);
	while (true) {
		for (const folder of [".agents", ".agent", "_agents", "_agent"]) {
			roots.push(path.join(current, folder, "skills"));
		}
		if (repoRoot && current === repoRoot) break;
		const parent = path.dirname(current);
		if (parent === current) break;
		current = parent;
		if (!repoRoot) break;
	}

	for (const skillsRoot of roots) {
		try {
			if (!fs.existsSync(skillsRoot)) continue;
			for (const entry of fs.readdirSync(skillsRoot, { withFileTypes: true })) {
				if (!entry.isDirectory() && !entry.isSymbolicLink()) continue;
				const skillMd = path.join(skillsRoot, entry.name, "SKILL.md");
				if (fs.existsSync(skillMd)) {
					result.add(resolveRealPathSafe(skillMd));
				}
			}
		} catch {
			// 忽略不可读目录
		}
	}

	return result;
}

interface ExtractedRuleSource {
	realPath: string;
	content: string;
}

/**
 * 提取 Pi 已加载的项目与全局上下文规则文件（`AGENTS.md` / `CLAUDE.md`），
 * 仅提取用户规则文件本身，绝不读取或携带任何宿主系统提示词（preamble / tools / docs）。
 */
function extractPiRuleSources(
	projectContextSection: string | null | undefined,
	cwd: string,
	geminiDir: string,
	agyInheritCustomizations: boolean,
): ExtractedRuleSource[] {
	if (!projectContextSection) return [];

	const autoLoaded = agyInheritCustomizations
		? collectAgyAutoLoadedRuleFiles(cwd, geminiDir)
		: new Set<string>();

	const seenRealPaths = new Set<string>();
	const results: ExtractedRuleSource[] = [];
	const blockRegex = /<project_instructions\s+path="([^"]+)">([\s\S]*?)<\/project_instructions>/g;

	let match = blockRegex.exec(projectContextSection);
	while (match !== null) {
		const rawPath = match[1].trim();
		const inlineContent = match[2].trim();
		const real = resolveRealPathSafe(rawPath);

		if (!seenRealPaths.has(real) && !autoLoaded.has(real)) {
			seenRealPaths.add(real);
			let content = inlineContent;
			try {
				if (fs.existsSync(real) && fs.statSync(real).isFile()) {
					content = fs.readFileSync(real, "utf8").trim();
				}
			} catch {
				// 文件不可直读时使用已加载的规则内容
			}
			if (content.length > 0) {
				results.push({ realPath: real, content });
			}
		}
		match = blockRegex.exec(projectContextSection);
	}

	return results;
}

/**
 * 提取 Pi 已加载的 Skill 目录（包含真实存在的 `SKILL.md`），并过滤掉 AGY 已原生加载的重复项。
 */
function extractPiSkillDirectories(
	skillsSection: string | null | undefined,
	cwd: string,
	geminiDir: string,
	agyInheritCustomizations: boolean,
): { skillDirs: string[]; externalDirs: string[] } {
	if (!skillsSection) {
		return { skillDirs: [], externalDirs: [] };
	}

	const resolvedCwd = resolveRealPathSafe(cwd);
	const nativeSkillFiles = agyInheritCustomizations
		? collectAgyNativeSkillFiles(cwd, geminiDir)
		: new Set<string>();

	const seenSkillDirs = new Set<string>();
	const skillDirs: string[] = [];
	const externalDirs = new Set<string>();

	const locRegex = /<location>([^<]+)<\/location>/g;
	let match = locRegex.exec(skillsSection);
	while (match !== null) {
		const rawSkillFile = match[1].trim();
		const realSkillFile = resolveRealPathSafe(rawSkillFile);

		if (
			path.basename(realSkillFile) === "SKILL.md" &&
			fs.existsSync(realSkillFile) &&
			!nativeSkillFiles.has(realSkillFile)
		) {
			const skillDir = path.dirname(realSkillFile);
			if (!seenSkillDirs.has(skillDir)) {
				seenSkillDirs.add(skillDir);
				skillDirs.push(skillDir);

				const rel = path.relative(resolvedCwd, skillDir);
				if (rel.startsWith("..") || path.isAbsolute(rel)) {
					externalDirs.add(skillDir);
				}
			}
		}
		match = locRegex.exec(skillsSection);
	}

	return {
		skillDirs,
		externalDirs: Array.from(externalDirs),
	};
}

/**
 * 按 Antigravity 官方 `rules.md` 的 24,000 字节单文件预算上限安全截断行边界。
 */
function truncateRuleBytesAtLineBoundary(markdown: string, maxBytes = MAX_RULE_FILE_BYTES): string {
	if (Buffer.byteLength(markdown, "utf8") <= maxBytes) {
		return markdown;
	}
	const lines = markdown.split("\n");
	const kept: string[] = [];
	let usedBytes = 0;
	for (const line of lines) {
		const lineBytes = Buffer.byteLength(`${line}\n`, "utf8");
		if (usedBytes + lineBytes > maxBytes) {
			break;
		}
		kept.push(line);
		usedBytes += lineBytes;
	}
	return `${kept.join("\n")}\n`;
}

/**
 * 按父目录聚合待挂载的 Skill 目录，生成符合官方 `skills.json` Discovery Manifest 规范的 `entries`。
 */
function buildSkillsManifestEntries(
	skillDirs: readonly string[],
): Array<{ path: string; include_only: string[] }> {
	const grouped = new Map<string, string[]>();
	for (const dir of skillDirs) {
		const parent = path.dirname(dir);
		const folderName = path.basename(dir);
		const list = grouped.get(parent) ?? [];
		if (!list.includes(folderName)) {
			list.push(folderName);
		}
		grouped.set(parent, list);
	}

	return Array.from(grouped.entries()).map(([parentPath, folderNames]) => ({
		path: parentPath,
		include_only: folderNames.sort(),
	}));
}

function writeFileIfChanged(filePath: string, content: string): void {
	try {
		if (fs.existsSync(filePath) && fs.readFileSync(filePath, "utf8") === content) {
			return;
		}
	} catch {
		// 继续覆写
	}
	fs.mkdirSync(path.dirname(filePath), { recursive: true });
	fs.writeFileSync(filePath, content, "utf8");
}

/**
 * 为 `pi-tools` 纯净模式对齐原生 Pi 的极简工具使用准则。
 */
function buildPureModeToolGuidelines(declaredTools: readonly Tool[]): string[] {
	const names = new Set(declaredTools.map((t) => t.name));
	const guidelines: string[] = [];

	const hasBash = names.has("bash");
	const hasPowerShell = names.has("powershell");
	const hasSearchTools = names.has("grep") || names.has("find") || names.has("ls");

	if ((hasBash || hasPowerShell) && !hasSearchTools) {
		if (hasBash && hasPowerShell) {
			guidelines.push(
				"Use bash or PowerShell for file operations like listing, searching, and finding files.",
			);
		} else if (hasPowerShell) {
			guidelines.push(
				"Use PowerShell for file operations like listing, searching, and finding files.",
			);
		} else {
			guidelines.push("Use bash for file operations like ls, rg, find.");
		}
	}

	if (names.has("read")) {
		guidelines.push("Use read to examine files instead of cat or sed.");
	}
	if (names.has("edit")) {
		guidelines.push(
			"Use edit for targeted changes. Batch changes to one file in a single call; each edits[].oldText must exactly and uniquely match the original file.",
		);
	}

	guidelines.push("Be concise in your responses.");
	guidelines.push("Show file paths clearly when working with files without forcing file:// URLs.");
	return guidelines;
}

/**
 * 在 `pi-tools` 纯净模式（`excludeDefaultComponents: true`）下，构建供首条用户消息携带的 `<workspace_context>` 块。
 * 保持系统提示词零触碰，同时完整交付工作目录、工具简明准则、Pi 规则文件（`AGENTS.md`）及按需加载的 `<available_skills>` 索引。
 */
function buildPureModeWorkspaceContextBlock(params: {
	cwd: string;
	declaredTools: readonly Tool[];
	rules: readonly ExtractedRuleSource[];
	rawSkillsSection: string | null | undefined;
	bridgeSkills: boolean;
}): string {
	const blocks: string[] = [
		"<workspace_context>",
		`<cwd>${params.cwd.replace(/\\/g, "/")}</cwd>`,
	];

	const guidelines = buildPureModeToolGuidelines(params.declaredTools);
	if (guidelines.length > 0) {
		blocks.push(
			"<tool_guidelines>",
			...guidelines.map((line) => `- ${line}`),
			"</tool_guidelines>",
		);
	}

	if (params.rules.length > 0) {
		const ruleBlocks = params.rules.map(
			(r) => `<project_instructions path="${r.realPath}">\n${r.content}\n</project_instructions>`,
		);
		blocks.push(
			"<project_context>",
			"Project-specific instructions and guidelines:",
			...ruleBlocks,
			"</project_context>",
		);
	}

	if (params.bridgeSkills && params.rawSkillsSection) {
		const trimmedSkills = params.rawSkillsSection.trim();
		if (trimmedSkills.length > 0) {
			const wrappedSkills = trimmedSkills.startsWith("<skills>")
				? trimmedSkills
				: `<skills>\n${trimmedSkills}\n</skills>`;
			blocks.push(wrappedSkills);
		}
	}

	blocks.push("</workspace_context>", "");
	return blocks.join("\n");
}

function pruneOldMountCacheDirs(mountsRoot: string, keepDirName: string, maxEntries = 16): void {
	try {
		if (!fs.existsSync(mountsRoot)) return;
		const entries = fs
			.readdirSync(mountsRoot, { withFileTypes: true })
			.filter((e) => e.isDirectory() && e.name.startsWith("pi-mount-") && e.name !== keepDirName)
			.map((e) => {
				const full = path.join(mountsRoot, e.name);
				let mtimeMs = 0;
				try {
					mtimeMs = fs.statSync(full).mtimeMs;
				} catch {
					// 忽略
				}
				return { full, mtimeMs };
			})
			.sort((a, b) => b.mtimeMs - a.mtimeMs);

		for (let i = maxEntries - 1; i < entries.length; i++) {
			if (i < 0) continue;
			try {
				fs.rmSync(entries[i].full, { recursive: true, force: true });
			} catch {
				// 忽略并发清理异常
			}
		}
	} catch {
		// 忽略
	}
}

/**
 * 构建会话级规则与技能挂载状态：
 * - `pi-tools` 纯净模式：生成结构化 `workspaceContextBlock` 供首条用户消息携带，并授权外部技能目录可读。
 * - `agy-tools` / `hybrid` 模式：构建符合官方 Plugin 规范（可通过 `agy plugin validate --strict`）的原生挂载包。
 */
export function prepareCustomizationMount(
	messages: TranscriptMessages,
	config: BridgeConfig,
	cwd: string,
	env: NodeJS.ProcessEnv = process.env,
): PreparedCustomizationMount {
	const flags = resolveEffectiveFlags(config);
	const sysMsg = getCurrentSystemMessage(messages);
	const sections = sysMsg?.sections ?? {};

	// 分支 A：pi-tools 纯净模式（excludeDefaultComponents: true，AGY 默认渲染组件已关闭）
	if (flags.toolMode === "pi-tools") {
		const declaredTools = getCurrentTools(messages);
		const allPiRules = flags.bridgePiAgentsMd
			? extractPiRuleSources(sections.project_context, cwd, config.geminiDir, false)
			: [];
		const { externalDirs } = flags.bridgePiSkills
			? extractPiSkillDirectories(sections.skills, cwd, config.geminiDir, false)
			: { externalDirs: [] };

		const workspaceContextBlock = buildPureModeWorkspaceContextBlock({
			cwd,
			declaredTools,
			rules: allPiRules,
			rawSkillsSection: sections.skills,
			bridgeSkills: flags.bridgePiSkills,
		});

		const mountDigest = crypto
			.createHash("sha256")
			.update(
				JSON.stringify({
					toolMode: flags.toolMode,
					workspaceContextBlock,
					externalDirs,
				}),
			)
			.digest("hex");

		return {
			mountedPluginPaths: [],
			additionalDirectories: externalDirs,
			workspaceContextBlock,
			mountDigest,
		};
	}

	// 分支 B：agy-tools / hybrid 模式（原生 Plugin 挂载并自动去重工作区已有配置）
	const rulesToMount = flags.bridgePiAgentsMd
		? extractPiRuleSources(
				sections.project_context,
				cwd,
				config.geminiDir,
				flags.agyInheritCustomizations,
			)
		: [];

	const { skillDirs, externalDirs } = flags.bridgePiSkills
		? extractPiSkillDirectories(
				sections.skills,
				cwd,
				config.geminiDir,
				flags.agyInheritCustomizations,
			)
		: { skillDirs: [], externalDirs: [] };

	const skillEntries = buildSkillsManifestEntries(skillDirs);
	const consolidatedRulesText =
		rulesToMount.length > 0
			? truncateRuleBytesAtLineBoundary(
					rulesToMount.map((r) => r.content).join("\n\n---\n\n"),
				)
			: "";

	const mountPayload = {
		toolMode: flags.toolMode,
		agyInheritCustomizations: flags.agyInheritCustomizations,
		agyInheritMcp: flags.agyInheritMcp,
		skillEntries,
		consolidatedRulesText,
	};

	const mountDigest = crypto
		.createHash("sha256")
		.update(JSON.stringify(mountPayload))
		.digest("hex");

	if (skillEntries.length === 0 && consolidatedRulesText.length === 0) {
		return {
			mountedPluginPaths: [],
			additionalDirectories: [],
			mountDigest,
		};
	}

	const shortHash = mountDigest.slice(0, 12);
	const mountsRoot = path.join(resolvePiAgentDir(env), "cache", "antigravity-mounts");
	const pluginDirName = `pi-mount-${shortHash}`;
	const pluginDir = path.join(mountsRoot, pluginDirName);

	const manifestJson = `${JSON.stringify(
		{
			name: pluginDirName,
			version: "0.1.1",
		},
		null,
		2,
	)}\n`;
	writeFileIfChanged(path.join(pluginDir, "plugin.json"), manifestJson);
	pruneOldMountCacheDirs(mountsRoot, pluginDirName);

	const skillsJsonPath = path.join(pluginDir, "skills.json");
	if (skillEntries.length > 0) {
		writeFileIfChanged(
			skillsJsonPath,
			`${JSON.stringify({ entries: skillEntries }, null, 2)}\n`,
		);
	} else if (fs.existsSync(skillsJsonPath)) {
		try {
			fs.unlinkSync(skillsJsonPath);
		} catch {
			// 忽略
		}
	}

	const rulesAgentsMdPath = path.join(pluginDir, "rules", "AGENTS.md");
	if (consolidatedRulesText.length > 0) {
		writeFileIfChanged(rulesAgentsMdPath, consolidatedRulesText);
	} else if (fs.existsSync(rulesAgentsMdPath)) {
		try {
			fs.unlinkSync(rulesAgentsMdPath);
		} catch {
			// 忽略
		}
	}

	return {
		mountedPluginPaths: [pluginDir],
		additionalDirectories: externalDirs,
		mountDigest,
	};
}

/**
 * 当 `shareAgySkillsToPi: true` 时，扫描 AGY 全局与工作区 Skills 目录供 Pi `resources_discover` 挂载。
 */
export function discoverAgySkillDirectoriesForPi(config: BridgeConfig, cwd: string): string[] {
	if (!config.shareAgySkillsToPi) {
		return [];
	}

	const candidates = [
		path.join(config.geminiDir, "config", "skills"),
		path.join(config.geminiDir, "skills"),
		path.join(cwd, ".agents", "skills"),
		path.join(cwd, ".agent", "skills"),
	];

	return candidates.filter((dir) => {
		try {
			return fs.existsSync(dir) && fs.statSync(dir).isDirectory();
		} catch {
			return false;
		}
	});
}
