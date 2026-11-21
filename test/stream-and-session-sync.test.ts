import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { describe, it } from "node:test";
import {
	normalizeContext,
	type AssistantMessage,
	type AssistantMessageEvent,
	type UserMessage,
} from "@earendil-works/pi-ai";
import type { SessionEntry } from "@earendil-works/pi-coding-agent";
import { DEFAULT_BRIDGE_CONFIG, type BridgeConfig } from "../src/config.ts";
import {
	AGY_SESSION_ANCHOR_CUSTOM_TYPE,
	captureSqliteCheckpoint,
	findBranchAnchorForHistory,
	forkSqliteConversationAtCheckpoint,
	parseStepMetadataGenerationUsage,
	parseStepPayloadPlannedToolCount,
	readSqliteGenerationUsagesSince,
} from "../src/session/checkpoint-sync.ts";
import {
	buildFreshReplayPromptBlocks,
	computeTranscriptDigest,
	extractAuxiliaryTaskInstructions,
	inspectTrafficOptProjectionState,
	isAuxiliarySingleTurnRequest,
} from "../src/session/reconcile.ts";
import {
	convertAcpUsageToPiUsage,
	QuotaSoftErrorFilter,
	StepStreamWriter,
} from "../src/stream/event-mapper.ts";

describe("流式行尾缓冲、429 软错误拦截与 Usage 转换", () => {
	it("零延迟放行正常文本并精确拦截跨块分片的 429 RESOURCE_EXHAUSTED 行", () => {
		const filter = new QuotaSoftErrorFilter();

		const r1 = filter.push("Hello world!\nNormal line ");
		assert.equal(r1.safeText, "Hello world!\nNormal line ");
		assert.equal(r1.quotaError, undefined);

		const r2 = filter.push("\n⚠ API error (attempt 1");
		assert.equal(r2.safeText, "\n");
		assert.equal(r2.quotaError, undefined);

		const r3 = filter.push("): RESOURCE_EXHAUSTED (code 429) try again\nIgnored tail");
		assert.equal(r3.safeText, "");
		assert.ok(r3.quotaError?.includes("RESOURCE_EXHAUSTED"));

		// 验证 agy 1.5.13 直接无 attempt 前缀的真实 429 行格式
		const directFilter = new QuotaSoftErrorFilter();
		const d1 = directFilter.push("\n⚠ RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n");
		assert.equal(d1.safeText, "\n");
		assert.ok(d1.quotaError?.includes("RESOURCE_EXHAUSTED (code 429)"));

		// 验证模型正文中正常引用或代码块讨论 429 报错样本时不会误判拦截
		const quoteFilter = new QuotaSoftErrorFilter();
		const q1 = quoteFilter.push("常见报错如下：\n```\n⚠ RESOURCE_EXHAUSTED (code 429): sample\n```\nRESOURCE_EXHAUSTED (code 429): discussed inline\n");
		assert.equal(q1.quotaError, undefined);
		assert.ok(q1.safeText.includes("discussed inline"));

		// 验证跨 chunk 切分的行内 ⚠ 示例及跨 chunk 切分的 ``` 代码块围栏均不会误判
		const splitFilter = new QuotaSoftErrorFilter();
		const s1 = splitFilter.push("Inline example: ");
		const s2 = splitFilter.push("⚠ RESOURCE_EXHAUSTED (code 429): not at line start\n``");
		const s3 = splitFilter.push("`text\n⚠ RESOURCE_EXHAUSTED (code 429): inside split fence\n``");
		const s4 = splitFilter.push("`\nStill alive!\n");
		assert.equal(s1.quotaError, undefined);
		assert.equal(s2.quotaError, undefined);
		assert.equal(s3.quotaError, undefined);
		assert.equal(s4.quotaError, undefined);
		assert.ok((s1.safeText + s2.safeText + s3.safeText + s4.safeText).includes("Still alive!"));
	});

	it("StepStreamWriter 支持两阶段工具调用流式采样、代码块平衡与分步 Usage 写入", async () => {
		const mockModel = {
			id: "gemini-4-neon",
			name: "Gemini 4 Neon",
			api: "antigravity-acp",
			provider: "antigravity",
			baseUrl: "acp://antigravity-cli",
			reasoning: true,
			input: ["text", "image"] as ("text" | "image")[],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 512000,
			maxTokens: 65536,
		};

		const writer = new StepStreamWriter(mockModel, {
			responseId: "sess-abc",
			providerThinkingLevel: "high",
		});

		writer.appendThinkingDelta("Let me check ");
		writer.appendThinkingDelta("the file.");
		writer.appendTextDelta("Reading now.");
		await writer.appendToolCallStreaming(
			{ id: "call_1", name: "read", arguments: { path: "a.ts" } },
			3,
		);
		writer.finishWithToolUse();

		const events: AssistantMessageEvent[] = [];
		for await (const ev of writer.stream) {
			if ("partial" in ev) {
				for (const block of ev.partial.content) {
					if (block.type === "toolCall") {
						assert.equal(block.id, "call_1");
						assert.deepEqual(block.arguments, { path: "a.ts" });
					}
				}
			}
			events.push(ev);
		}

		assert.deepEqual(
			events.map((e) => e.type),
			[
				"start",
				"thinking_start",
				"thinking_delta",
				"thinking_delta",
				"thinking_end",
				"text_start",
				"text_delta",
				"text_end",
				"toolcall_start",
				"toolcall_delta",
				"toolcall_delta",
				"toolcall_end",
				"done",
			],
		);
		const toolDeltas = events
			.filter((e): e is Extract<AssistantMessageEvent, { type: "toolcall_delta" }> => e.type === "toolcall_delta")
			.map((e) => e.delta)
			.join("");
		assert.equal(toolDeltas, JSON.stringify({ path: "a.ts" }));
		assert.ok(writer.estimatedOutputTokens > 0);
		assert.equal(writer.message.stopReason, "toolUse");
		assert.equal(writer.message.usage.totalTokens, 0);

		const finalUsage = convertAcpUsageToPiUsage({
			inputTokens: 1200,
			outputTokens: 350,
			cachedReadTokens: 800,
			thoughtTokens: 150,
		});
		assert.equal(finalUsage.input, 1200);
		assert.equal(finalUsage.output, 350);
		assert.equal(finalUsage.cacheRead, 800);
		assert.equal(finalUsage.reasoning, 150);
		assert.equal(finalUsage.totalTokens, 2350);

		// 验证带真实定价的模型在 finishWithToolUse 与 finishTurn 时自动计算 usage.cost
		const pricedModel = {
			...mockModel,
			cost: { input: 3, output: 15, cacheRead: 0.15, cacheWrite: 0 },
		};
		const pricedWriter = new StepStreamWriter(pricedModel);
		pricedWriter.appendTextDelta("Costed reply");
		pricedWriter.finishTurn("end_turn", {
			inputTokens: 1_000_000,
			outputTokens: 100_000,
			cachedReadTokens: 2_000_000,
		});
		assert.ok(Math.abs(pricedWriter.message.usage.cost.input - 3) < 1e-6);
		assert.ok(Math.abs(pricedWriter.message.usage.cost.output - 1.5) < 1e-6);
		assert.ok(Math.abs(pricedWriter.message.usage.cost.cacheRead - 0.3) < 1e-6);
		assert.ok(Math.abs(pricedWriter.message.usage.cost.total - 4.8) < 1e-6);
	});
});

describe("会话摘要、历史重放与 SQLite 只读快照分叉", () => {
	it("识别辅助摘要请求并在 FreshReplay 中保留历史多模态图片块", () => {
		const auxCtx = normalizeContext({
			systemPrompt: "You are a context summarization assistant. Do not continue the conversation.",
			messages: [
				{
					role: "user",
					content: "Please summarize the conversation above for compaction.",
					timestamp: 1,
				},
			],
			tools: [],
		});
		assert.equal(isAuxiliarySingleTurnRequest(auxCtx, undefined, false), true);
		assert.ok(extractAuxiliaryTaskInstructions(auxCtx)?.includes("summarization assistant"));

		// 用户以 --no-tools 首轮正常提问“总结 README”绝不能误判为一次性辅助请求
		const normalNoToolsCtx = normalizeContext({
			messages: [
				{
					role: "user",
					content: "Please summarize this README file and list its features.",
					timestamp: 1,
				},
			],
			tools: [],
		});
		assert.equal(isAuxiliarySingleTurnRequest(normalNoToolsCtx, undefined, false), false);

		const u1: UserMessage = {
			role: "user",
			content: [
				{ type: "text", text: "Look at this screenshot" },
				{ type: "image", data: "aW1n", mimeType: "image/png" },
			],
			timestamp: 100,
		};
		const a1: AssistantMessage = {
			role: "assistant",
			content: [{ type: "text", text: "I see a login button." }],
			api: "openai-responses",
			provider: "openai",
			model: "gpt-5",
			usage: convertAcpUsageToPiUsage(),
			stopReason: "stop",
			timestamp: 200,
		};
		const u2: UserMessage = {
			role: "user",
			content: "Now change its color.",
			timestamp: 300,
		};

		// 时间戳改变、context 阶段归档工具输出或重压缩历史图片均不影响会话结构摘要一致性
		const trRaw = {
			role: "toolResult" as const,
			toolCallId: "call_big_1",
			toolName: "read",
			content: [{ type: "text" as const, text: "x".repeat(50_000) }],
			isError: false,
			timestamp: 250,
		};
		const trArchived = {
			...trRaw,
			content: [{ type: "text" as const, text: "[已归档 ctx://tool/call_big_1]" }],
		};
		const u1Recompressed: UserMessage = {
			...u1,
			content: [
				{ type: "text", text: "Look at this screenshot" },
				{ type: "image", data: "cmVjb21wcmVzc2Vk", mimeType: "image/png" },
			],
		};
		const d1 = computeTranscriptDigest([u1, a1, trRaw]);
		const d2 = computeTranscriptDigest([
			{ ...u1Recompressed, timestamp: 9999 },
			{ ...a1, timestamp: 8888 },
			trArchived,
		]);
		assert.equal(d1, d2);
		assert.notEqual(
			d1,
			computeTranscriptDigest([u1, a1, { ...trRaw, isError: true }]),
		);

		const replayBlocks = buildFreshReplayPromptBlocks([u1, a1, u2]);
		assert.ok(replayBlocks.some((b) => b.type === "image" && b.data === "aW1n"));
		assert.ok(
			replayBlocks.some((b) => b.type === "text" && b.text.includes("<conversation-checkpoint>")),
		);

		// 验证 FreshReplay 字节预算保护：裁剪早期超长历史，但 100% 完整保留最近消息与末尾最新用户提问
		const hugeHistory: (UserMessage | AssistantMessage)[] = [];
		for (let i = 0; i < 30; i++) {
			hugeHistory.push({
				role: "user",
				content: `Turn ${i}: ${"H".repeat(2_000)}`,
				timestamp: i * 2 + 1,
			});
			hugeHistory.push({
				...a1,
				content: [{ type: "text", text: `Reply ${i}: ${"R".repeat(2_000)}` }],
				timestamp: i * 2 + 2,
			});
		}
		const finalPrompt: UserMessage = {
			role: "user",
			content: "CRITICAL_FINAL_USER_QUESTION_MUST_NEVER_BE_TRUNCATED",
			timestamp: 99999,
		};
		const budgetedBlocks = buildFreshReplayPromptBlocks([...hugeHistory, finalPrompt], 18_000);
		const joinedBudgetText = budgetedBlocks
			.filter((b): b is { type: "text"; text: string } => b.type === "text")
			.map((b) => b.text)
			.join("\n");
		assert.ok(joinedBudgetText.includes("earlier historical messages were omitted"));
		assert.ok(!joinedBudgetText.includes("Turn 0:"));
		assert.ok(joinedBudgetText.includes("Reply 29:"));
		const tailBlock = budgetedBlocks[budgetedBlocks.length - 1];
		assert.equal(tailBlock.type, "text");
		if (tailBlock.type === "text") {
			assert.equal(tailBlock.text, "CRITICAL_FINAL_USER_QUESTION_MUST_NEVER_BE_TRUNCATED");
		}
	});

	it("利用 node:sqlite VACUUM INTO 安全分叉截断会话数据库且原库保持只读不变", async () => {
		const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agy-sqlite-"));
		try {
			const convDir = path.join(tmpRoot, ".gemini", "antigravity-cli", "conversations");
			fs.mkdirSync(convDir, { recursive: true });

			const cfg: BridgeConfig = {
				...DEFAULT_BRIDGE_CONFIG,
				geminiDir: path.join(tmpRoot, ".gemini"),
				appDataDirName: "antigravity-cli",
				enableSqliteFork: true,
			};

			const origSessionId = "11111111-2222-3333-4444-555555555555";
			const origDbPath = path.join(convDir, `${origSessionId}.db`);

			// 构建模拟真实 AGY 表结构的会话库（先写入第 1 轮共 2 个 step）
			const initDb = new DatabaseSync(origDbPath);
			initDb.exec(`
				CREATE TABLE trajectory_meta (trajectory_id text, cascade_id text, trajectory_type integer, source integer, PRIMARY KEY (trajectory_id));
				CREATE TABLE steps (idx integer, step_type integer NOT NULL DEFAULT 0, status integer NOT NULL DEFAULT 0, step_payload blob, metadata blob, PRIMARY KEY (idx));
				CREATE TABLE gen_metadata (idx integer, data blob, size integer NOT NULL DEFAULT 0, PRIMARY KEY (idx));
				CREATE TABLE executor_metadata (idx integer, data blob, PRIMARY KEY (idx));
				CREATE TABLE parent_references (idx integer, data blob, PRIMARY KEY (idx));
				CREATE TABLE battle_mode_infos (idx integer, data blob, PRIMARY KEY (idx));
			`);
			initDb
				.prepare("INSERT INTO trajectory_meta VALUES (?, ?, 1, 1)")
				.run(origSessionId, origSessionId);
			initDb
				.prepare("INSERT INTO steps (idx, step_type, status, step_payload) VALUES (0, 1, 2, X'0102'), (1, 2, 2, X'030405')")
				.run();
			initDb.prepare("INSERT INTO gen_metadata VALUES (0, X'AA', 2)").run();
			initDb.prepare("INSERT INTO executor_metadata VALUES (0, X'BB')").run();
			initDb.prepare("INSERT INTO parent_references VALUES (0, X'CC'), (1, X'DD')").run();
			initDb.close();

			// 在第 1 轮结束点采集检查点
			const cpTurn1 = await captureSqliteCheckpoint(cfg, origSessionId);
			assert.ok(cpTurn1);
			assert.equal(cpTurn1.steps, 2);

			// 模拟会话继续推进到了第 2 轮（新增 idx=2, 3，并在 idx=3 写入真实编码的 step_type=15 metadata）
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
			const innerUsageBytes = Buffer.from([
				(2 << 3) | 0, ...encodeVarint(3353), // field 2: inputTokens = 3353
				(3 << 3) | 0, ...encodeVarint(445),  // field 3: outputTokens = 445
				(5 << 3) | 0, ...encodeVarint(10441), // field 5: cacheReadTokens = 10441
				(9 << 3) | 0, ...encodeVarint(255),  // field 9: thoughtTokens = 255
				(10 << 3) | 0, ...encodeVarint(190), // field 10: responseTokens = 190
			]);
			const stepMetaBlob = Buffer.from([
				(9 << 3) | 2, ...encodeVarint(innerUsageBytes.length), ...innerUsageBytes,
			]);
			const directParsed = parseStepMetadataGenerationUsage(stepMetaBlob, 3);
			assert.deepEqual(directParsed, {
				stepIdx: 3,
				inputTokens: 3353,
				outputTokens: 445,
				cacheWriteTokens: 0,
				cacheReadTokens: 10441,
				thoughtTokens: 255,
			});

			const appendDb = new DatabaseSync(origDbPath);
			// 构造包含 3 个 ToolCall (field 20 -> 3x field 7) 的 PlannerResponse payload
			const toolEntry = Buffer.from([(7 << 3) | 2, 2, 0x08, 0x01]);
			const plannerInner = Buffer.concat([toolEntry, toolEntry, toolEntry]);
			const stepPayload3Tools = Buffer.from([
				...encodeVarint((20 << 3) | 2),
				...encodeVarint(plannerInner.length),
				...plannerInner,
			]);
			assert.equal(parseStepPayloadPlannedToolCount(stepPayload3Tools), 3);

			appendDb
				.prepare("INSERT INTO steps (idx, step_type, status, step_payload, metadata) VALUES (2, 14, 2, X'06', NULL), (3, 15, 2, ?, ?)")
				.run(stepPayload3Tools, stepMetaBlob);
			appendDb.prepare("INSERT INTO gen_metadata VALUES (1, X'EE', 2)").run();
			appendDb.prepare("INSERT INTO parent_references VALUES (2, X'FF'), (3, X'11')").run();
			appendDb.close();

			const gensSince1 = await readSqliteGenerationUsagesSince(cfg, origSessionId, 1);
			assert.equal(gensSince1.length, 1);
			assert.equal(gensSince1[0].inputTokens, 3353);
			assert.equal(gensSince1[0].cacheReadTokens, 10441);
			assert.equal(gensSince1[0].outputTokens, 445);
			assert.equal(gensSince1[0].thoughtTokens, 255);
			assert.equal(gensSince1[0].plannedToolCalls, 3);

			// 从第 1 轮检查点执行快照分叉
			const forkedId = await forkSqliteConversationAtCheckpoint(cfg, origSessionId, cpTurn1);
			assert.ok(forkedId);
			assert.notEqual(forkedId, origSessionId);

			// 验证原库仍完整保留 4 条 step，新分叉库精确截断为 2 条 step 且更新了 trajectory_id
			const checkOrig = new DatabaseSync(origDbPath, { readOnly: true });
			const origCount = (checkOrig.prepare("SELECT COUNT(*) AS c FROM steps").get() as { c: number }).c;
			checkOrig.close();
			assert.equal(origCount, 4);

			const forkedDbPath = path.join(convDir, `${forkedId}.db`);
			const checkFork = new DatabaseSync(forkedDbPath, { readOnly: true });
			const forkCount = (checkFork.prepare("SELECT COUNT(*) AS c FROM steps").get() as { c: number }).c;
			const forkMeta = checkFork
				.prepare("SELECT trajectory_id FROM trajectory_meta")
				.get() as { trajectory_id: string };
			checkFork.close();
			assert.equal(forkCount, 2);
			assert.equal(forkMeta.trajectory_id, forkedId);

			// 验证分支锚点查找逻辑
			const m1: UserMessage = { role: "user", content: "turn1", timestamp: 1 };
			const m2: UserMessage = { role: "user", content: "turn2-edited", timestamp: 2 };
			const entries: SessionEntry[] = [
				{
					type: "custom",
					id: "e1",
					parentId: null,
					timestamp: "2026-11-21T00:00:00Z",
					customType: AGY_SESSION_ANCHOR_CUSTOM_TYPE,
					data: {
						version: 1,
						agySessionId: origSessionId,
						toolMode: "pi-tools",
						transcriptDigest: computeTranscriptDigest([m1]),
						messageCount: 1,
						mountDigest: "mount-1",
						toolsDigest: "tools-1",
						dbCheckpoint: cpTurn1,
					},
				},
			];

			const exactHit = findBranchAnchorForHistory({
				branchEntries: entries,
				priorMessages: [m1],
				toolMode: "pi-tools",
				mountDigest: "mount-1",
				config: cfg,
			});
			assert.equal(exactHit.kind, "exact");

			const forkHit = findBranchAnchorForHistory({
				branchEntries: entries,
				priorMessages: [m1, m2],
				toolMode: "pi-tools",
				mountDigest: "mount-1",
				config: cfg,
			});
			assert.equal(forkHit.kind, "forkable");

			// 验证 /agy sessions 刚导入的原始锚点（importedRaw: true）可跨空 mountDigest 直接命中 exact 恢复
			const importedEntries: SessionEntry[] = [
				{
					type: "custom",
					id: "e2",
					parentId: null,
					timestamp: "2026-11-21T00:00:00Z",
					customType: AGY_SESSION_ANCHOR_CUSTOM_TYPE,
					data: {
						version: 1,
						agySessionId: origSessionId,
						toolMode: "pi-tools",
						transcriptDigest: computeTranscriptDigest([m1]),
						messageCount: 1,
						mountDigest: "",
						toolsDigest: "",
						importedRaw: true,
					},
				},
			];
			const importedHit = findBranchAnchorForHistory({
				branchEntries: importedEntries,
				priorMessages: [m1],
				toolMode: "pi-tools",
				mountDigest: "new-live-mount-digest",
				config: cfg,
			});
			assert.equal(importedHit.kind, "exact");
		} finally {
			fs.rmSync(tmpRoot, { recursive: true, force: true });
		}
	});

	it("适配 pi-traffic-optimization：图片压缩与归档占位符保持结构摘要恒定、精准统计投影指纹且重放不裁切 ctx:// 句柄", () => {
		const hash64 = "a".repeat(64);
		const imgHash64 = "b".repeat(64);

		const rawUserPng: UserMessage = {
			role: "user",
			content: [
				{ type: "text", text: "看看这张截图" },
				{ type: "image", data: "RAW_PNG_DATA", mimeType: "image/png" },
			],
			timestamp: 1,
		};
		const compressedUserWebp: UserMessage = {
			role: "user",
			content: [
				{ type: "text", text: "看看这张截图" },
				{ type: "image", data: "SMALL_WEBP_DATA", mimeType: "image/webp" },
			],
			timestamp: 1,
		};
		const projectedUserImg: UserMessage = {
			role: "user",
			content: [
				{ type: "text", text: "看看这张截图" },
				{
					type: "text",
					text: `[已归档图片 ctx://image/${imgHash64}] 1920x1080 image/webp optimized_bytes=12345 source=user；context_recall({id:"ctx://image/${imgHash64}"}) 可重新查看。`,
				},
			],
			timestamp: 1,
		};

		// 原始 PNG、WebP 压缩图与归档占位符在结构摘要上必须 100% 恒定，确保 turn_end 锚点跨重启与开关切换精准命中
		const digestRaw = computeTranscriptDigest([rawUserPng]);
		assert.equal(computeTranscriptDigest([compressedUserWebp]), digestRaw);
		assert.equal(computeTranscriptDigest([projectedUserImg]), digestRaw);

		const longPlaceholder = `[已归档 ctx://tool/${hash64}]\n${JSON.stringify({
			tool: "read",
			call: "call-1",
			preview: "x".repeat(5000),
		})}\n这是历史结果。精确取回：context_recall({id:"ctx://tool/${hash64}"})；query/offset/limit 可局部取回。`;

		const assistantStep: AssistantMessage = {
			role: "assistant",
			content: [{ type: "toolCall", id: "call-1", name: "read", arguments: { path: "big.ts" } }],
			api: "antigravity-acp",
			provider: "antigravity",
			model: "gemini-4-neon",
			usage: {
				input: 100,
				output: 20,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 120,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "toolUse",
			timestamp: 2,
		};
		const projectedToolRes = {
			role: "toolResult" as const,
			toolCallId: "call-1",
			toolName: "read",
			content: [{ type: "text" as const, text: longPlaceholder }],
			isError: false,
			timestamp: 3,
		};
		const followUser: UserMessage = { role: "user", content: "继续分析", timestamp: 4 };

		const projState = inspectTrafficOptProjectionState([
			projectedUserImg,
			assistantStep,
			projectedToolRes,
			followUser,
		]);
		assert.equal(projState.archivedCount, 2);
		assert.ok(projState.projectionDigest.length === 64);

		// FreshReplay 必须完整保留超长 ctx:// 归档占位符末尾的 context_recall 指引，绝不截断破坏句柄
		const replayBlocks = buildFreshReplayPromptBlocks([
			projectedUserImg,
			assistantStep,
			projectedToolRes,
			followUser,
		]);
		const mergedReplayText = replayBlocks
			.filter((b): b is { type: "text"; text: string } => b.type === "text")
			.map((b) => b.text)
			.join("\n");
		assert.ok(mergedReplayText.includes(`context_recall({id:"ctx://tool/${hash64}"})`));
		assert.ok(!mergedReplayText.includes("[tool output truncated in replay]"));
	});
});
