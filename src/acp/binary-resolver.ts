import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

export interface ResolvedBinary {
	/** 可执行文件绝对路径 */
	binPath: string;
	/** 是否为 Windows 批处理包装脚本（.cmd / .bat） */
	isWindowsWrapperScript: boolean;
	/** 解析来源说明 */
	source: "config" | "path" | "standard-location";
}

function isExecutableFile(candidate: string, platform: NodeJS.Platform): boolean {
	try {
		const stat = fs.statSync(candidate);
		if (!stat.isFile()) {
			return false;
		}
		if (platform !== "win32") {
			fs.accessSync(candidate, fs.constants.X_OK);
		}
		return true;
	} catch {
		return false;
	}
}

function getWindowsExtensions(env: NodeJS.ProcessEnv): string[] {
	const raw = env.PATHEXT || ".EXE;.CMD;.BAT;.COM";
	const parsed = raw
		.split(";")
		.map((ext) => ext.trim().toLowerCase())
		.filter((ext) => ext.length > 0);
	// 优先定位原生 .exe，避免不必要的 cmd.exe 包装层
	const ordered = [".exe", ...parsed.filter((ext) => ext !== ".exe")];
	return Array.from(new Set(ordered));
}

function getStandardLocations(
	platform: NodeJS.Platform,
	homeDir: string,
	geminiDir: string,
	env: NodeJS.ProcessEnv,
): string[] {
	if (platform === "win32") {
		const localAppData = env.LOCALAPPDATA || path.join(homeDir, "AppData", "Local");
		const userProfile = env.USERPROFILE || homeDir;
		return [
			path.join(localAppData, "Programs", "Antigravity", "bin", "agy.exe"),
			path.join(localAppData, "antigravity-cli", "bin", "agy.exe"),
			path.join(userProfile, ".local", "bin", "agy.exe"),
			path.join(geminiDir, "antigravity-cli", "bin", "agy.exe"),
			path.join(localAppData, "Programs", "Antigravity", "bin", "agy.cmd"),
		];
	}

	const candidates = [
		path.join(homeDir, ".local", "bin", "agy"),
		path.join(geminiDir, "antigravity-cli", "bin", "agy"),
		"/usr/local/bin/agy",
		"/usr/bin/agy",
	];
	if (platform === "darwin") {
		candidates.push("/opt/homebrew/bin/agy");
		candidates.push(path.join(homeDir, "Applications", "Antigravity.app", "Contents", "Resources", "app", "bin", "agy"));
	}
	return candidates;
}

export interface ResolveBinaryOptions {
	configuredBin?: string;
	geminiDir?: string;
	env?: NodeJS.ProcessEnv;
	platform?: NodeJS.Platform;
	homeDir?: string;
}

/**
 * 跨平台定位 `agy` 可执行文件。
 * 查找顺序：配置/环境变量显式路径 -> 系统 PATH -> 平台标准安装位置。
 */
export function resolveAgyBinary(options: ResolveBinaryOptions = {}): ResolvedBinary | undefined {
	const env = options.env ?? process.env;
	const platform = options.platform ?? process.platform;
	const homeDir = options.homeDir ?? os.homedir();
	const geminiDir = options.geminiDir ?? path.join(homeDir, ".gemini");
	const winExts = platform === "win32" ? getWindowsExtensions(env) : [""];

	const checkCandidate = (
		rawCandidate: string,
		source: ResolvedBinary["source"],
	): ResolvedBinary | undefined => {
		const resolved = path.resolve(rawCandidate);
		if (platform === "win32" && path.extname(resolved) === "") {
			for (const ext of winExts) {
				const withExt = `${resolved}${ext}`;
				if (isExecutableFile(withExt, platform)) {
					const lower = ext.toLowerCase();
					return {
						binPath: withExt,
						isWindowsWrapperScript: lower === ".cmd" || lower === ".bat",
						source,
					};
				}
			}
		}
		if (isExecutableFile(resolved, platform)) {
			const ext = path.extname(resolved).toLowerCase();
			return {
				binPath: resolved,
				isWindowsWrapperScript: platform === "win32" && (ext === ".cmd" || ext === ".bat"),
				source,
			};
		}
		return undefined;
	};

	// 1. 显式配置或环境变量
	const explicit = options.configuredBin?.trim() || env.PI_AGY_BIN?.trim();
	if (explicit) {
		const hit = checkCandidate(explicit, "config");
		if (hit) return hit;
	}

	// 2. 系统 PATH 检索（兼容 Windows 大小写环境变量 Path / PATH）
	const pathValue = env.PATH ?? env.Path ?? "";
	for (const dir of pathValue.split(path.delimiter)) {
		const trimmed = dir.trim().replace(/^"(.*)"$/, "$1");
		if (!trimmed) continue;
		const hit = checkCandidate(path.join(trimmed, "agy"), "path");
		if (hit) return hit;
	}

	// 3. 平台标准安装路径探测
	for (const stdPath of getStandardLocations(platform, homeDir, geminiDir, env)) {
		const hit = checkCandidate(stdPath, "standard-location");
		if (hit) return hit;
	}

	return undefined;
}

/** 读取本机 `agy --version` 版本字符串 */
export function probeAgyVersion(binary: ResolvedBinary, timeoutMs = 4_000): string | undefined {
	try {
		const result = spawnSync(binary.binPath, ["--version"], {
			encoding: "utf8",
			timeout: timeoutMs,
			shell: binary.isWindowsWrapperScript,
			windowsHide: true,
		});
		if (result.status === 0 && result.stdout) {
			const line = result.stdout
				.split(/\r?\n/)
				.map((s) => s.trim())
				.find((s) => s.length > 0);
			return line;
		}
	} catch {
		// 忽略探测异常
	}
	return undefined;
}
