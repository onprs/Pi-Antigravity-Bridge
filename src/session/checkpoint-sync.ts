import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import type { AssistantMessage, ToolResultMessage, UserMessage } from "@earendil-works/pi-ai";
import type { CustomEntry, SessionEntry } from "@earendil-works/pi-coding-agent";
import {
	type BridgeConfig,
	type ToolMode,
	resolveAgyConversationsDir,
} from "../config.ts";
import { computeTranscriptDigest } from "./reconcile.ts";

export const AGY_SESSION_ANCHOR_CUSTOM_TYPE = "antigravity-acp-state";

export interface SqliteDbCheckpoint {
	steps: number;
	generations: number;
	executions: number;
	fingerprint: string;
}

export interface AgySessionAnchor {
	version: 1;
	/** 关联的 AGY ACP 会话 UUID */
	agySessionId: string;
	/** 创建该锚点时的工具主控模式 */
	toolMode: ToolMode;
	/** 模型可见对话消息序列的 SHA-256 摘要 */
	transcriptDigest: string;
	/** 模型可见消息总数 */
	messageCount: number;
	/** 原生挂载插件（AGENTS.md / Skills）状态 SHA-256 摘要 */
	mountDigest: string;
	/** 暴露给宿主的 MCP 工具表指纹 */
	toolsDigest: string;
	/** 可选的本地 SQLite 会话数据库检查点水位 */
	dbCheckpoint?: SqliteDbCheckpoint;
}

export type AnchorMatchResult =
	| {
			kind: "exact";
			anchor: AgySessionAnchor;
	  }
	| {
			kind: "forkable";
			anchor: AgySessionAnchor;
	  }
	| {
			kind: "none";
	  };

export function hasLocalConversationDb(config: BridgeConfig, agySessionId: string): boolean {
	if (!agySessionId) return false;
	const dbPath = path.join(resolveAgyConversationsDir(config), `${agySessionId}.db`);
	return fs.existsSync(dbPath);
}

/**
 * 沿当前会话树分支（`sessionManager.getBranch()`）回溯匹配最优历史锚点。
 *
 * `priorMessages` 为本轮新增用户输入之前的历史可见消息序列：
 * - 若某个锚点的 `messageCount === priorMessages.length` 且摘要、工具模式与系统提示词一致，可直接走 `session/resume`；
 * - 若锚点命中更早的 `priorMessages.slice(0, k)` 前缀且具备 `dbCheckpoint`，可尝试只读 SQLite 快照分叉恢复。
 */
export function findBranchAnchorForHistory(options: {
	branchEntries: readonly SessionEntry[];
	priorMessages: readonly (UserMessage | AssistantMessage | ToolResultMessage)[];
	toolMode: ToolMode;
	mountDigest: string;
	config: BridgeConfig;
}): AnchorMatchResult {
	const { branchEntries, priorMessages, toolMode, mountDigest, config } = options;
	if (branchEntries.length === 0) {
		return { kind: "none" };
	}

	const fullPriorDigest = computeTranscriptDigest(priorMessages);
	let bestForkCandidate: AgySessionAnchor | undefined;

	// 从分支叶子向根节点逆序查找最近的合法锚点
	for (let i = branchEntries.length - 1; i >= 0; i--) {
		const entry = branchEntries[i];
		if (entry.type !== "custom") continue;
		const custom = entry as CustomEntry<AgySessionAnchor>;
		if (custom.customType !== AGY_SESSION_ANCHOR_CUSTOM_TYPE || !custom.data) continue;

		const anchor = custom.data;
		if (anchor.version !== 1 || !anchor.agySessionId) continue;
		if (anchor.toolMode !== toolMode || anchor.mountDigest !== mountDigest) continue;
		if (!hasLocalConversationDb(config, anchor.agySessionId)) continue;

		if (
			anchor.messageCount === priorMessages.length &&
			anchor.transcriptDigest === fullPriorDigest
		) {
			return { kind: "exact", anchor };
		}

		if (
			!bestForkCandidate &&
			config.enableSqliteFork &&
			anchor.dbCheckpoint &&
			anchor.messageCount > 0 &&
			anchor.messageCount < priorMessages.length
		) {
			const sliceDigest = computeTranscriptDigest(priorMessages.slice(0, anchor.messageCount));
			if (sliceDigest === anchor.transcriptDigest) {
				bestForkCandidate = anchor;
			}
		}
	}

	if (bestForkCandidate) {
		return { kind: "forkable", anchor: bestForkCandidate };
	}

	return { kind: "none" };
}

type NodeSqliteModule = typeof import("node:sqlite");
let cachedSqliteMod: NodeSqliteModule | null | undefined;

async function loadNodeSqlite(): Promise<NodeSqliteModule | undefined> {
	if (cachedSqliteMod !== undefined) {
		return cachedSqliteMod ?? undefined;
	}
	try {
		cachedSqliteMod = await import("node:sqlite");
		return cachedSqliteMod;
	} catch {
		cachedSqliteMod = null;
		return undefined;
	}
}

const REQUIRED_TABLES = [
	"trajectory_meta",
	"steps",
	"gen_metadata",
	"executor_metadata",
	"parent_references",
] as const;

function computeStepsPrefixFingerprint(
	db: import("node:sqlite").DatabaseSync,
	limitSteps: number,
): string {
	const rows = db
		.prepare(
			"SELECT idx, step_type, status, COALESCE(length(step_payload), 0) AS plen FROM steps WHERE idx < ? ORDER BY idx ASC",
		)
		.all(limitSteps) as Array<{ idx: number; step_type: number; status: number; plen: number }>;

	const hash = crypto.createHash("sha256");
	for (const row of rows) {
		hash.update(`${row.idx}:${row.step_type}:${row.status}:${row.plen};`);
	}
	return hash.digest("hex").slice(0, 32);
}

function verifyRequiredTables(db: import("node:sqlite").DatabaseSync): boolean {
	const rows = db
		.prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
		.all() as Array<{ name: string }>;
	const names = new Set(rows.map((r) => r.name));
	return REQUIRED_TABLES.every((req) => names.has(req));
}

/**
 * 以只读模式采集 AGY 会话 SQLite 数据库的当前高水位检查点。
 * 遇到文件不存在、WAL 锁冲突或跨版本表结构差异时静默返回 `undefined`。
 */
export async function captureSqliteCheckpoint(
	config: BridgeConfig,
	agySessionId: string,
): Promise<SqliteDbCheckpoint | undefined> {
	if (!config.enableSqliteFork || !agySessionId) {
		return undefined;
	}
	const dbPath = path.join(resolveAgyConversationsDir(config), `${agySessionId}.db`);
	if (!fs.existsSync(dbPath)) {
		return undefined;
	}

	const sqlite = await loadNodeSqlite();
	if (!sqlite) return undefined;

	let db: import("node:sqlite").DatabaseSync | undefined;
	try {
		db = new sqlite.DatabaseSync(dbPath, { readOnly: true });
		if (!verifyRequiredTables(db)) {
			return undefined;
		}

		const stepsRow = db
			.prepare("SELECT COALESCE(MAX(idx) + 1, 0) AS n FROM steps")
			.get() as { n: number };
		const genRow = db
			.prepare("SELECT COALESCE(MAX(idx) + 1, 0) AS n FROM gen_metadata")
			.get() as { n: number };
		const execRow = db
			.prepare("SELECT COALESCE(MAX(idx) + 1, 0) AS n FROM executor_metadata")
			.get() as { n: number };

		const steps = Number(stepsRow?.n ?? 0);
		if (steps <= 0) {
			return undefined;
		}

		const fingerprint = computeStepsPrefixFingerprint(db, steps);
		return {
			steps,
			generations: Number(genRow?.n ?? 0),
			executions: Number(execRow?.n ?? 0),
			fingerprint,
		};
	} catch {
		return undefined;
	} finally {
		try {
			db?.close();
		} catch {
			// 忽略关闭异常
		}
	}
}

/**
 * 基于 `VACUUM INTO` 只读副本导出与水位截断，从历史锚点分叉出全新的 AGY 会话数据库文件。
 * 严格遵循只读原库原则；若校验不通过或出现任何文件锁异常，立即清理临时副本并返回 `undefined` 以便调用方无感降级至 `FreshReplay`。
 */
export async function forkSqliteConversationAtCheckpoint(
	config: BridgeConfig,
	sourceSessionId: string,
	checkpoint: SqliteDbCheckpoint,
): Promise<string | undefined> {
	if (!config.enableSqliteFork) {
		return undefined;
	}
	const convDir = resolveAgyConversationsDir(config);
	const sourceDbPath = path.join(convDir, `${sourceSessionId}.db`);
	if (!fs.existsSync(sourceDbPath)) {
		return undefined;
	}

	const sqlite = await loadNodeSqlite();
	if (!sqlite) return undefined;

	const forkedSessionId = crypto.randomUUID();
	const targetDbPath = path.join(convDir, `${forkedSessionId}.db`);
	const escapedTarget = targetDbPath.replace(/'/g, "''");

	let srcDb: import("node:sqlite").DatabaseSync | undefined;
	try {
		srcDb = new sqlite.DatabaseSync(sourceDbPath, { readOnly: true });
		if (!verifyRequiredTables(srcDb)) {
			return undefined;
		}
		const currentStepsRow = srcDb
			.prepare("SELECT COALESCE(MAX(idx) + 1, 0) AS n FROM steps")
			.get() as { n: number };
		if (Number(currentStepsRow?.n ?? 0) < checkpoint.steps) {
			return undefined;
		}
		const actualPrefixFp = computeStepsPrefixFingerprint(srcDb, checkpoint.steps);
		if (actualPrefixFp !== checkpoint.fingerprint) {
			return undefined;
		}

		srcDb.exec(`VACUUM INTO '${escapedTarget}'`);
	} catch {
		cleanupDbFilesSafe(targetDbPath);
		return undefined;
	} finally {
		try {
			srcDb?.close();
		} catch {
			// 忽略
		}
	}

	let dstDb: import("node:sqlite").DatabaseSync | undefined;
	try {
		dstDb = new sqlite.DatabaseSync(targetDbPath);
		dstDb.exec("BEGIN IMMEDIATE");
		dstDb.prepare("DELETE FROM steps WHERE idx >= ?").run(checkpoint.steps);
		dstDb.prepare("DELETE FROM parent_references WHERE idx >= ?").run(checkpoint.steps);
		const hasBattleTable = dstDb
			.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = 'battle_mode_infos'")
			.get();
		if (hasBattleTable) {
			dstDb.prepare("DELETE FROM battle_mode_infos WHERE idx >= ?").run(checkpoint.steps);
		}
		dstDb.prepare("DELETE FROM gen_metadata WHERE idx >= ?").run(checkpoint.generations);
		dstDb.prepare("DELETE FROM executor_metadata WHERE idx >= ?").run(checkpoint.executions);
		dstDb
			.prepare("UPDATE trajectory_meta SET trajectory_id = ?, cascade_id = ?")
			.run(forkedSessionId, forkedSessionId);
		dstDb.exec("COMMIT");
		return forkedSessionId;
	} catch {
		try {
			dstDb?.exec("ROLLBACK");
		} catch {
			// 忽略
		}
		cleanupDbFilesSafe(targetDbPath);
		return undefined;
	} finally {
		try {
			dstDb?.close();
		} catch {
			// 忽略
		}
	}
}

function cleanupDbFilesSafe(baseDbPath: string): void {
	for (const ext of ["", "-wal", "-shm", "-journal"]) {
		try {
			const candidate = `${baseDbPath}${ext}`;
			if (fs.existsSync(candidate)) {
				fs.unlinkSync(candidate);
			}
		} catch {
			// 忽略
		}
	}
}
