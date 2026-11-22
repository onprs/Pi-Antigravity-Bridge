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
import { computeTranscriptDigest, type PreloadedSqliteTurn } from "./reconcile.ts";

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
	/** 是否为通过 `/agy sessions` 刚导入尚未经历首轮请求对齐的原始会话锚点 */
	importedRaw?: boolean;
	/** 可选：该 AGY 会话实际承载的 `pi-traffic-optimization` 归档投影指纹 */
	projectionDigest?: string;
	/** 可选：该 AGY 会话实际承载的 `pi-traffic-optimization` 归档占位符总数 */
	projectedCount?: number;
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
		if (anchor.toolMode !== toolMode) continue;
		if (!anchor.importedRaw && anchor.mountDigest !== mountDigest) continue;
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

/** 打开 SQLite 连接并设置 `busy_timeout`，缓解 Windows WAL 并发读写下的瞬时 `SQLITE_BUSY` 锁冲突 */
function openSqliteWithTimeout(
	sqlite: NodeSqliteModule,
	dbPath: string,
	readOnly: boolean,
	busyTimeoutMs = 1_500,
): import("node:sqlite").DatabaseSync {
	const db = readOnly
		? new sqlite.DatabaseSync(dbPath, { readOnly: true })
		: new sqlite.DatabaseSync(dbPath);
	try {
		db.exec(`PRAGMA busy_timeout = ${Math.max(100, Math.floor(busyTimeoutMs))}`);
	} catch {
		// 忽略只读连接或特殊环境下 PRAGMA 不可用异常
	}
	return db;
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

export interface SqliteGenerationUsage {
	stepIdx: number;
	inputTokens: number;
	outputTokens: number;
	cacheWriteTokens: number;
	cacheReadTokens: number;
	thoughtTokens?: number;
	/** 该次 `PLANNER_RESPONSE` 内含的工具调用总数（用于多工具分步执行时的精确守恒分摊） */
	plannedToolCalls?: number;
}

function readProtobufVarint(buf: Uint8Array, offset: number): [number, number] | undefined {
	let result = 0;
	let shift = 0;
	let pos = offset;
	while (pos < buf.length) {
		const byte = buf[pos++];
		result += (byte & 0x7f) * 2 ** shift;
		if ((byte & 0x80) === 0) {
			return [result, pos];
		}
		shift += 7;
		if (shift > 53) return undefined;
	}
	return undefined;
}

/**
 * 解析 AGY `steps.metadata`（`step_type = 15` 即 `PLANNER_RESPONSE`）字段 9 中的上游 API 真实单步 Token 明细。
 * 体积仅约 390 字节，在 Gemini 与 Claude 模型上均统一编码：
 * - field 2: uncached inputTokens
 * - field 3: total outputTokens（含思考，等于 field 9 + field 10）
 * - field 4: cachedWriteTokens（可选）
 * - field 5: cachedReadTokens（可选）
 * - field 9: thoughtTokens（思考 Token，可选）
 * - field 10: responseTokens（正文与工具调用 Token，可选）
 */
export function parseStepMetadataGenerationUsage(
	metadata: Uint8Array,
	stepIdx: number,
): SqliteGenerationUsage | undefined {
	let pos = 0;
	while (pos < metadata.length) {
		const tagRes = readProtobufVarint(metadata, pos);
		if (!tagRes) return undefined;
		const [tag, nextPos] = tagRes;
		pos = nextPos;
		const fieldNo = Math.floor(tag / 8);
		const wireType = tag & 7;
		if (fieldNo <= 0) return undefined;

		if (wireType === 0) {
			const valRes = readProtobufVarint(metadata, pos);
			if (!valRes) return undefined;
			pos = valRes[1];
		} else if (wireType === 2) {
			const lenRes = readProtobufVarint(metadata, pos);
			if (!lenRes) return undefined;
			const [len, subStart] = lenRes;
			const subEnd = subStart + len;
			if (subEnd > metadata.length) return undefined;
			if (fieldNo === 9) {
				const sub = metadata.subarray(subStart, subEnd);
				let k = 0;
				const fields = new Map<number, number>();
				while (k < sub.length) {
					const innerTag = readProtobufVarint(sub, k);
					if (!innerTag) break;
					k = innerTag[1];
					const innerFn = Math.floor(innerTag[0] / 8);
					const innerWt = innerTag[0] & 7;
					if (innerWt === 0) {
						const v = readProtobufVarint(sub, k);
						if (!v) break;
						k = v[1];
						fields.set(innerFn, v[0]);
					} else if (innerWt === 2) {
						const l = readProtobufVarint(sub, k);
						if (!l || l[1] + l[0] > sub.length) break;
						k = l[1] + l[0];
					} else if (innerWt === 5) {
						k += 4;
					} else if (innerWt === 1) {
						k += 8;
					} else {
						break;
					}
				}
				const inputTokens = fields.get(2) ?? 0;
				const rawOutput = fields.get(3) ?? 0;
				const cacheWriteTokens = fields.get(4) ?? 0;
				const cacheReadTokens = fields.get(5) ?? 0;
				const thoughtTokens = fields.get(9);
				if (inputTokens + rawOutput + cacheWriteTokens + cacheReadTokens > 0) {
					return {
						stepIdx,
						inputTokens,
						outputTokens:
							thoughtTokens !== undefined ? Math.max(rawOutput, thoughtTokens) : rawOutput,
						cacheWriteTokens,
						cacheReadTokens,
						thoughtTokens,
					};
				}
			}
			pos = subEnd;
		} else if (wireType === 5) {
			pos += 4;
		} else if (wireType === 1) {
			pos += 8;
		} else {
			return undefined;
		}
	}
	return undefined;
}

/**
 * 解析 `steps.step_payload`（`step_type = 15`）字段 20（`PlannerResponse`）中子字段 7（`repeated ToolCall`）的数量。
 * 在 Gemini 与 Claude 模型上均统一编码，用于在单次生成拆出多个顺序工具步时按整数守恒分摊 Token，避免后续子步出现 0 提示词导致 UI 缓存命中率消失。
 */
export function parseStepPayloadPlannedToolCount(stepPayload: Uint8Array): number {
	let pos = 0;
	while (pos < stepPayload.length) {
		const tagRes = readProtobufVarint(stepPayload, pos);
		if (!tagRes) return 0;
		pos = tagRes[1];
		const fieldNo = Math.floor(tagRes[0] / 8);
		const wireType = tagRes[0] & 7;
		if (fieldNo <= 0) return 0;

		if (wireType === 0) {
			const v = readProtobufVarint(stepPayload, pos);
			if (!v) return 0;
			pos = v[1];
		} else if (wireType === 2) {
			const lenRes = readProtobufVarint(stepPayload, pos);
			if (!lenRes) return 0;
			const [len, subStart] = lenRes;
			const subEnd = subStart + len;
			if (subEnd > stepPayload.length) return 0;
			if (fieldNo === 20) {
				const sub = stepPayload.subarray(subStart, subEnd);
				let k = 0;
				let count = 0;
				while (k < sub.length) {
					const innerTag = readProtobufVarint(sub, k);
					if (!innerTag) break;
					k = innerTag[1];
					const innerFn = Math.floor(innerTag[0] / 8);
					const innerWt = innerTag[0] & 7;
					if (innerWt === 0) {
						const v = readProtobufVarint(sub, k);
						if (!v) break;
						k = v[1];
					} else if (innerWt === 2) {
						const l = readProtobufVarint(sub, k);
						if (!l || l[1] + l[0] > sub.length) break;
						if (innerFn === 7) {
							count++;
						}
						k = l[1] + l[0];
					} else if (innerWt === 5) {
						k += 4;
					} else if (innerWt === 1) {
						k += 8;
					} else {
						break;
					}
				}
				return count;
			}
			pos = subEnd;
		} else if (wireType === 5) {
			pos += 4;
		} else if (wireType === 1) {
			pos += 8;
		} else {
			return 0;
		}
	}
	return 0;
}

/** 读取指定会话当前 `steps` 表的最大 `idx`（新建会话尚无记录时返回 `-1`），用于在 `session/resume` 时定位起始水位 */
export async function readCurrentMaxSqliteStepIdx(
	config: BridgeConfig,
	agySessionId: string,
): Promise<number> {
	if (!agySessionId) return -1;
	const dbPath = path.join(resolveAgyConversationsDir(config), `${agySessionId}.db`);
	if (!fs.existsSync(dbPath)) return -1;
	const sqlite = await loadNodeSqlite();
	if (!sqlite) return -1;

	let db: import("node:sqlite").DatabaseSync | undefined;
	try {
		db = openSqliteWithTimeout(sqlite, dbPath, true);
		const row = db
			.prepare("SELECT COALESCE(MAX(idx), -1) AS maxIdx FROM steps")
			.get() as { maxIdx: number } | undefined;
		return typeof row?.maxIdx === "number" ? row.maxIdx : -1;
	} catch {
		return -1;
	} finally {
		try {
			db?.close();
		} catch {
			// 忽略
		}
	}
}

/**
 * 以只读方式读取指定水位之后新产生的全部 `PLANNER_RESPONSE`（`step_type = 15`）逐次生成统计。
 * 若本地库不存在或跨版本字段变化则返回空数组，由调用方平滑回退至 ACP 窗口统计。
 */
export async function readSqliteGenerationUsagesSince(
	config: BridgeConfig,
	agySessionId: string,
	afterStepIdx: number,
): Promise<SqliteGenerationUsage[]> {
	if (!agySessionId) return [];
	const dbPath = path.join(resolveAgyConversationsDir(config), `${agySessionId}.db`);
	if (!fs.existsSync(dbPath)) return [];
	const sqlite = await loadNodeSqlite();
	if (!sqlite) return [];

	let db: import("node:sqlite").DatabaseSync | undefined;
	try {
		db = openSqliteWithTimeout(sqlite, dbPath, true);
		const rows = db
			.prepare(
				"SELECT idx, metadata, step_payload FROM steps WHERE step_type = 15 AND idx > ? AND metadata IS NOT NULL ORDER BY idx ASC",
			)
			.all(afterStepIdx) as Array<{
			idx: number;
			metadata: Uint8Array;
			step_payload?: Uint8Array | null;
		}>;

		const parsed: SqliteGenerationUsage[] = [];
		for (const row of rows) {
			if (row.metadata && row.metadata.byteLength > 0) {
				const u = parseStepMetadataGenerationUsage(row.metadata, row.idx);
				if (u) {
					if (row.step_payload && row.step_payload.byteLength > 0) {
						const toolCount = parseStepPayloadPlannedToolCount(row.step_payload);
						if (toolCount > 0) {
							u.plannedToolCalls = toolCount;
						}
					}
					parsed.push(u);
				}
			}
		}
		return parsed;
	} catch {
		return [];
	} finally {
		try {
			db?.close();
		} catch {
			// 忽略
		}
	}
}

/**
 * 在末步 `prompt_done` 到达时带短退避重试读取最新 SQLite `PLANNER_RESPONSE` 统计，
 * 兜底应对 `agy` WAL 异步落盘略晚于 JSON-RPC 回包的边界竞态。
 */
export async function readSqliteGenerationUsagesSinceWithRetry(
	config: BridgeConfig,
	agySessionId: string,
	afterStepIdx: number,
	retryDelaysMs: readonly number[] = [25, 50],
): Promise<SqliteGenerationUsage[]> {
	const first = await readSqliteGenerationUsagesSince(config, agySessionId, afterStepIdx);
	if (first.length > 0 || !hasLocalConversationDb(config, agySessionId)) {
		return first;
	}
	for (const delay of retryDelaysMs) {
		await new Promise((r) => setTimeout(r, delay));
		const next = await readSqliteGenerationUsagesSince(config, agySessionId, afterStepIdx);
		if (next.length > 0) {
			return next;
		}
	}
	return [];
}

function sleepWithAbort(ms: number, signal?: AbortSignal): Promise<void> {
	if (signal?.aborted) return Promise.resolve();
	return new Promise<void>((resolve) => {
		const timer = setTimeout(() => {
			signal?.removeEventListener("abort", onAbort);
			resolve();
		}, ms);
		const onAbort = () => {
			clearTimeout(timer);
			resolve();
		};
		signal?.addEventListener("abort", onAbort, { once: true });
	});
}

/**
 * 在工具边界切入用户转向消息（`steer`）前，等待刚结算的 loopback HTTP 工具结果落盘为非进行中状态（`status != 1`），
 * 确保随后的 `session/cancel` 不会误冲掉已完成工具的持久化输出。
 */
export async function waitForSqliteToolStepsSettled(
	config: BridgeConfig,
	agySessionId: string,
	maxWaitMs = 160,
	signal?: AbortSignal,
): Promise<void> {
	if (signal?.aborted) return;
	const dbPath = agySessionId
		? path.join(resolveAgyConversationsDir(config), `${agySessionId}.db`)
		: "";
	if (!dbPath || !fs.existsSync(dbPath)) {
		await sleepWithAbort(25, signal);
		return;
	}
	const sqlite = await loadNodeSqlite();
	if (!sqlite) {
		await sleepWithAbort(25, signal);
		return;
	}

	await sleepWithAbort(20, signal);
	if (signal?.aborted) return;
	const deadline = Date.now() + Math.max(20, maxWaitMs - 20);
	while (Date.now() < deadline && !signal?.aborted) {
		let hasRunningTool = false;
		let db: import("node:sqlite").DatabaseSync | undefined;
		try {
			db = openSqliteWithTimeout(sqlite, dbPath, true, 200);
			const busy = db
				.prepare(
					"SELECT 1 AS busy FROM steps WHERE step_type != 15 AND step_type != 14 AND status = 1 LIMIT 1",
				)
				.get() as { busy?: number } | undefined;
			hasRunningTool = busy?.busy === 1;
		} catch {
			hasRunningTool = false;
		} finally {
			try {
				db?.close();
			} catch {
				// 忽略
			}
		}
		if (!hasRunningTool) {
			return;
		}
		await sleepWithAbort(15, signal);
	}
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
		db = openSqliteWithTimeout(sqlite, dbPath, true);
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
		srcDb = openSqliteWithTimeout(sqlite, sourceDbPath, true);
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
		dstDb = openSqliteWithTimeout(sqlite, targetDbPath, false);
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

const CORTEX_STEP_TYPE_USER_INPUT = 14;
const CORTEX_STEP_TYPE_PLANNER_RESPONSE = 15;
const CORTEX_STEP_STATUS_DONE = 3;
const CORTEX_TRAJECTORY_TYPE_INTERACTIVE_CASCADE = 4;
const CORTEX_TRAJECTORY_SOURCE_CLI = 17;

function encodeProtobufVarint(value: number | bigint): Buffer {
	let v = typeof value === "bigint" ? value : BigInt(value);
	const out: number[] = [];
	while (v >= 0x80n) {
		out.push(Number((v & 0x7fn) | 0x80n));
		v >>= 7n;
	}
	out.push(Number(v));
	return Buffer.from(out);
}

function encodeProtobufVarintField(fieldNo: number, value: number | bigint): Buffer {
	return Buffer.concat([encodeProtobufVarint((fieldNo << 3) | 0), encodeProtobufVarint(value)]);
}

function encodeProtobufBytesField(fieldNo: number, payload: Buffer | string): Buffer {
	const buf = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, "utf8");
	return Buffer.concat([
		encodeProtobufVarint((fieldNo << 3) | 2),
		encodeProtobufVarint(buf.length),
		buf,
	]);
}

/** 构造 AGY `CortexStep`（`USER_INPUT`）最小标准 Protobuf 载荷 */
export function encodeUserInputStepPayload(text: string): Buffer {
	// CortexStepUserInput: field 2 = user_response, field 3 = items (TextOrScopeItem { field 1 = text })
	const textItem = encodeProtobufBytesField(1, text);
	const userInput = Buffer.concat([
		encodeProtobufBytesField(2, text),
		encodeProtobufBytesField(3, textItem),
	]);
	return Buffer.concat([
		encodeProtobufVarintField(1, CORTEX_STEP_TYPE_USER_INPUT),
		encodeProtobufVarintField(4, CORTEX_STEP_STATUS_DONE),
		encodeProtobufBytesField(19, userInput),
	]);
}

/** 构造 AGY `CortexStep`（`PLANNER_RESPONSE`）最小标准 Protobuf 载荷 */
export function encodePlannerResponseStepPayload(text: string): Buffer {
	// CortexStepPlannerResponse: field 1 = response, field 8 = modified_response
	const plannerResponse = Buffer.concat([
		encodeProtobufBytesField(1, text),
		encodeProtobufBytesField(8, text),
	]);
	return Buffer.concat([
		encodeProtobufVarintField(1, CORTEX_STEP_TYPE_PLANNER_RESPONSE),
		encodeProtobufVarintField(4, CORTEX_STEP_STATUS_DONE),
		encodeProtobufBytesField(20, plannerResponse),
	]);
}

/**
 * 新建一个独立 UUID 的 AGY 本地 SQLite 会话数据库，并将超长历史的多个安全分片以
 * `(USER_INPUT, PLANNER_RESPONSE)` 交替步预注入 `steps` 表，供随后通过 `session/resume`
 * 零额外 LLM 调用直接恢复，规避 `agy` 单条 `USER_INPUT` 64,000 token 截断上限。
 *
 * 严格仅新建独立 UUID 文件，永不改动任何已有会话库；任何异常均立即清理临时文件并返回 `undefined`。
 */
export async function createPreloadedSqliteConversation(
	config: BridgeConfig,
	turns: readonly PreloadedSqliteTurn[],
): Promise<string | undefined> {
	if (!config.enableSqliteFork || turns.length === 0) {
		return undefined;
	}

	const sqlite = await loadNodeSqlite();
	if (!sqlite) return undefined;

	const convDir = resolveAgyConversationsDir(config);
	try {
		fs.mkdirSync(convDir, { recursive: true });
	} catch {
		return undefined;
	}

	const sessionId = crypto.randomUUID();
	const dbPath = path.join(convDir, `${sessionId}.db`);

	let db: import("node:sqlite").DatabaseSync | undefined;
	try {
		db = openSqliteWithTimeout(sqlite, dbPath, false);
		db.exec(`
			PRAGMA user_version = 1;
			CREATE TABLE trajectory_meta (
				trajectory_id text,
				cascade_id text,
				trajectory_type integer,
				source integer,
				PRIMARY KEY (trajectory_id)
			);
			CREATE TABLE steps (
				idx integer,
				step_type integer NOT NULL DEFAULT 0,
				status integer NOT NULL DEFAULT 0,
				has_subtrajectory numeric NOT NULL DEFAULT false,
				metadata blob,
				error_details blob,
				permissions blob,
				task_details blob,
				render_info blob,
				step_payload blob,
				step_format integer NOT NULL DEFAULT 0,
				PRIMARY KEY (idx)
			);
			CREATE INDEX idx_steps_status ON steps(status);
			CREATE INDEX idx_steps_step_type ON steps(step_type);
			CREATE TABLE gen_metadata (
				idx integer,
				data blob,
				size integer NOT NULL DEFAULT 0,
				PRIMARY KEY (idx)
			);
			CREATE TABLE executor_metadata (
				idx integer,
				data blob,
				PRIMARY KEY (idx)
			);
			CREATE TABLE parent_references (
				idx integer,
				data blob,
				PRIMARY KEY (idx)
			);
			CREATE TABLE trajectory_metadata_blob (
				id text DEFAULT "main",
				data blob,
				PRIMARY KEY (id)
			);
			CREATE TABLE battle_mode_infos (
				idx integer,
				data blob,
				PRIMARY KEY (idx)
			);
		`);

		db.exec("BEGIN IMMEDIATE");
		db.prepare(
			"INSERT INTO trajectory_meta (trajectory_id, cascade_id, trajectory_type, source) VALUES (?, ?, ?, ?)",
		).run(
			sessionId,
			sessionId,
			CORTEX_TRAJECTORY_TYPE_INTERACTIVE_CASCADE,
			CORTEX_TRAJECTORY_SOURCE_CLI,
		);

		const insertStep = db.prepare(
			"INSERT INTO steps (idx, step_type, status, has_subtrajectory, step_payload, step_format) VALUES (?, ?, ?, 0, ?, 0)",
		);
		let idx = 0;
		for (const turn of turns) {
			insertStep.run(
				idx++,
				CORTEX_STEP_TYPE_USER_INPUT,
				CORTEX_STEP_STATUS_DONE,
				encodeUserInputStepPayload(turn.userText),
			);
			insertStep.run(
				idx++,
				CORTEX_STEP_TYPE_PLANNER_RESPONSE,
				CORTEX_STEP_STATUS_DONE,
				encodePlannerResponseStepPayload(turn.assistantAckText),
			);
		}
		db.exec("COMMIT");
		return sessionId;
	} catch {
		try {
			db?.exec("ROLLBACK");
		} catch {
			// 忽略
		}
		cleanupDbFilesSafe(dbPath);
		return undefined;
	} finally {
		try {
			db?.close();
		} catch {
			// 忽略
		}
	}
}

/**
 * 向刚通过 `forkSqliteConversationAtCheckpoint` 克隆出的新分叉 `.db` 末尾追加预注入分片步。
 * 仅用于尚未被 `agy` 打开的新分叉副本，严禁对正在运行的原库直接调用。
 */
export async function appendPreloadedTurnsToSqliteConversation(
	config: BridgeConfig,
	forkedSessionId: string,
	turns: readonly PreloadedSqliteTurn[],
): Promise<boolean> {
	if (!config.enableSqliteFork || !forkedSessionId || turns.length === 0) {
		return false;
	}
	const dbPath = path.join(resolveAgyConversationsDir(config), `${forkedSessionId}.db`);
	if (!fs.existsSync(dbPath)) {
		return false;
	}
	const sqlite = await loadNodeSqlite();
	if (!sqlite) return false;

	let db: import("node:sqlite").DatabaseSync | undefined;
	try {
		db = openSqliteWithTimeout(sqlite, dbPath, false);
		if (!verifyRequiredTables(db)) {
			return false;
		}
		const lastRow = db
			.prepare("SELECT idx, step_type FROM steps ORDER BY idx DESC LIMIT 1")
			.get() as { idx?: number; step_type?: number } | undefined;
		let nextIdx = typeof lastRow?.idx === "number" ? lastRow.idx + 1 : 0;

		db.exec("BEGIN IMMEDIATE");
		const insertStep = db.prepare(
			"INSERT INTO steps (idx, step_type, status, has_subtrajectory, step_payload, step_format) VALUES (?, ?, ?, 0, ?, 0)",
		);
		// 若原分叉尾部停在 USER_INPUT，先补一条中性 PLANNER_RESPONSE 保持严格交替角色合法性
		if (lastRow?.step_type === CORTEX_STEP_TYPE_USER_INPUT) {
			insertStep.run(
				nextIdx++,
				CORTEX_STEP_TYPE_PLANNER_RESPONSE,
				CORTEX_STEP_STATUS_DONE,
				encodePlannerResponseStepPayload("Continuing session context."),
			);
		}
		for (const turn of turns) {
			insertStep.run(
				nextIdx++,
				CORTEX_STEP_TYPE_USER_INPUT,
				CORTEX_STEP_STATUS_DONE,
				encodeUserInputStepPayload(turn.userText),
			);
			insertStep.run(
				nextIdx++,
				CORTEX_STEP_TYPE_PLANNER_RESPONSE,
				CORTEX_STEP_STATUS_DONE,
				encodePlannerResponseStepPayload(turn.assistantAckText),
			);
		}
		db.exec("COMMIT");
		return true;
	} catch {
		try {
			db?.exec("ROLLBACK");
		} catch {
			// 忽略
		}
		return false;
	} finally {
		try {
			db?.close();
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
