import * as path from "node:path";
import { StringDecoder } from "node:string_decoder";
import { fileURLToPath } from "node:url";

/**
 * 纯 Node.js 标准库实现的跨平台 Stdio-to-HTTP MCP 转发桥。
 * 当用户显式设置 `processPerSession: false` 时，作为零 `nc` 依赖的跨平台 Stdio MCP 入口回连宿主本地 HTTP MCP 服务。
 * 支持多路并发 JSON-RPC 请求同时在途，避免单个 Park-and-Yield 工具挂起阻塞并发工具调用或心跳。
 */
export async function runStdioToHttpMcpShim(targetUrl: string, bearerToken: string): Promise<void> {
	const decoder = new StringDecoder("utf8");
	let buffer = "";
	const inflight = new Set<Promise<void>>();

	const forwardLine = async (line: string): Promise<void> => {
		let parsed: Record<string, unknown>;
		try {
			parsed = JSON.parse(line) as Record<string, unknown>;
		} catch {
			return;
		}

		const hasId = "id" in parsed && parsed.id !== undefined;
		try {
			const response = await fetch(targetUrl, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${bearerToken}`,
				},
				body: line,
			});

			const responseText = await response.text();
			if (!response.ok) {
				if (hasId) {
					const errPayload = {
						jsonrpc: "2.0",
						id: parsed.id,
						error: {
							code: -32000,
							message: `MCP HTTP ${response.status}: ${responseText.trim().slice(0, 240) || response.statusText}`,
						},
					};
					process.stdout.write(`${JSON.stringify(errPayload)}\n`);
				}
				return;
			}
			if (hasId && responseText.trim().length > 0) {
				process.stdout.write(`${responseText.trim()}\n`);
			}
		} catch (error) {
			if (hasId) {
				const errPayload = {
					jsonrpc: "2.0",
					id: parsed.id,
					error: {
						code: -32603,
						message: error instanceof Error ? error.message : String(error),
					},
				};
				process.stdout.write(`${JSON.stringify(errPayload)}\n`);
			}
		}
	};

	const dispatchLine = (line: string): void => {
		const task = forwardLine(line).finally(() => {
			inflight.delete(task);
		});
		inflight.add(task);
	};

	const sseAbort = new AbortController();
	const startSseNotificationPump = async (): Promise<void> => {
		try {
			const res = await fetch(targetUrl, {
				method: "GET",
				headers: {
					Accept: "text/event-stream",
					Authorization: `Bearer ${bearerToken}`,
				},
				signal: sseAbort.signal,
			});
			if (!res.ok || !res.body) return;
			const sseDecoder = new StringDecoder("utf8");
			let sseBuf = "";
			for await (const chunk of res.body) {
				sseBuf += sseDecoder.write(Buffer.from(chunk));
				let nl = sseBuf.indexOf("\n");
				while (nl !== -1) {
					const rawLine = sseBuf.slice(0, nl).trim();
					sseBuf = sseBuf.slice(nl + 1);
					if (rawLine.startsWith("data:")) {
						const payload = rawLine.slice(5).trim();
						if (payload.length > 0) {
							process.stdout.write(`${payload}\n`);
						}
					}
					nl = sseBuf.indexOf("\n");
				}
			}
		} catch {
			// stdin 结束主动中断或宿主关闭连接时静默退出
		}
	};
	void startSseNotificationPump();

	process.stdin.on("data", (chunk: Buffer) => {
		buffer += decoder.write(chunk);
		let idx = buffer.indexOf("\n");
		while (idx !== -1) {
			const line = buffer.slice(0, idx).trim();
			buffer = buffer.slice(idx + 1);
			if (line.length > 0) {
				dispatchLine(line);
			}
			idx = buffer.indexOf("\n");
		}
	});

	process.stdin.on("end", () => {
		const rest = (buffer + decoder.end()).trim();
		if (rest.length > 0) {
			dispatchLine(rest);
		}
		void Promise.allSettled(Array.from(inflight)).finally(() => {
			sseAbort.abort();
		});
	});
}

/** 生成一段内联 Node.js 启动脚本参数，使宿主无需依赖额外编译产物即可在任意平台启动并发 Stdio 转发桥（含 SSE 工具变更通知订阅） */
export function buildInlineStdioShimCommand(
	targetUrl: string,
	bearerToken: string,
): { command: string; args: string[]; env: Array<{ name: string; value: string }> } {
	const inlineScript = [
		'const url = process.env.PI_AGY_MCP_URL;',
		'const token = process.env.PI_AGY_MCP_TOKEN;',
		'let buf = "";',
		'const inflight = new Set();',
		'const sseAc = new AbortController();',
		'(async () => {',
		'  try {',
		'    const r = await fetch(url, { method: "GET", headers: { "Accept": "text/event-stream", "Authorization": "Bearer " + token }, signal: sseAc.signal });',
		'    if (!r.ok || !r.body) return;',
		'    let sb = ""; const td = new TextDecoder();',
		'    for await (const ch of r.body) {',
		'      sb += td.decode(ch, { stream: true });',
		'      let k;',
		'      while ((k = sb.indexOf("\\n")) !== -1) {',
		'        const ln = sb.slice(0, k).trim(); sb = sb.slice(k + 1);',
		'        if (ln.startsWith("data:")) { const d = ln.slice(5).trim(); if (d) process.stdout.write(d + "\\n"); }',
		'      }',
		'    }',
		'  } catch {}',
		'})();',
		'async function send(line) {',
		'  let msg; try { msg = JSON.parse(line); } catch { return; }',
		'  const hasId = msg && msg.id !== undefined;',
		'  try {',
		'    const res = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token }, body: line });',
		'    const txt = (await res.text()).trim();',
		'    if (!res.ok) { if (hasId) process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: msg.id, error: { code: -32000, message: "MCP HTTP " + res.status + (txt ? ": " + txt.slice(0, 240) : "") } }) + "\\n"); return; }',
		'    if (hasId && txt) process.stdout.write(txt + "\\n");',
		'  } catch (e) {',
		'    if (hasId) process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: msg.id, error: { code: -32603, message: String(e) } }) + "\\n");',
		'  }',
		'}',
		'function dispatch(l) { const p = send(l).finally(() => inflight.delete(p)); inflight.add(p); }',
		'function drain(flushAll) {',
		'  let i;',
		'  while ((i = buf.indexOf("\\n")) !== -1) {',
		'    const l = buf.slice(0, i).trim(); buf = buf.slice(i + 1);',
		'    if (l) dispatch(l);',
		'  }',
		'  if (flushAll) { const r = buf.trim(); buf = ""; if (r) dispatch(r); Promise.allSettled([...inflight]).finally(() => sseAc.abort()); }',
		'}',
		'process.stdin.setEncoding("utf8");',
		'process.stdin.on("data", (c) => { buf += c; drain(false); });',
		'process.stdin.on("end", () => drain(true));',
	].join(" ");

	return {
		command: process.execPath,
		args: ["-e", inlineScript],
		env: [
			{ name: "PI_AGY_MCP_URL", value: targetUrl },
			{ name: "PI_AGY_MCP_TOKEN", value: bearerToken },
			{ name: "NO_PROXY", value: "127.0.0.1,localhost,::1" },
			{ name: "no_proxy", value: "127.0.0.1,localhost,::1" },
		],
	};
}

// 支持作为独立 CLI 脚本直接执行
const isDirectExecution = (() => {
	try {
		if (!process.argv[1]) return false;
		return path.resolve(fileURLToPath(import.meta.url)) === path.resolve(process.argv[1]);
	} catch {
		return false;
	}
})();

if (isDirectExecution) {
	const urlArgIndex = process.argv.indexOf("--url");
	const tokenArgIndex = process.argv.indexOf("--token");
	const url =
		(urlArgIndex !== -1 ? process.argv[urlArgIndex + 1] : undefined) ||
		process.env.PI_AGY_MCP_URL ||
		"";
	const token =
		(tokenArgIndex !== -1 ? process.argv[tokenArgIndex + 1] : undefined) ||
		process.env.PI_AGY_MCP_TOKEN ||
		"";
	if (url) {
		void runStdioToHttpMcpShim(url, token);
	}
}
