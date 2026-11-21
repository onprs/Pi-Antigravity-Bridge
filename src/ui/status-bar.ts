import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AcpTierInfo } from "../acp/protocol.ts";
import type { BridgeConfig } from "../config.ts";
import { ANTIGRAVITY_PROVIDER_ID } from "../catalog/model-discovery.ts";

export const STATUS_BAR_KEY = "antigravity";

export interface RuntimeTelemetrySnapshot {
	agySessionId?: string;
	tier?: AcpTierInfo;
	usedTokens?: number;
	windowSize?: number;
	backendCompactedRecently?: boolean;
}

function formatCompactTokens(tokens: number): string {
	if (tokens >= 1_000_000) {
		return `${(tokens / 1_000_000).toFixed(1)}M`;
	}
	if (tokens >= 1_000) {
		return `${Math.round(tokens / 1_000)}k`;
	}
	return String(tokens);
}

/**
 * 构建面向最终用户的简洁状态栏标识。
 * 仅在当前选中模型属于 `antigravity` Provider 时展示；切换到其他 Provider 时自动清除。
 */
export function updateAntigravityStatusBar(
	ctx: ExtensionContext | undefined,
	config: BridgeConfig,
	telemetry: RuntimeTelemetrySnapshot,
): void {
	if (!ctx || !ctx.hasUI) return;

	if (!ctx.model || ctx.model.provider !== ANTIGRAVITY_PROVIDER_ID) {
		ctx.ui.setStatus(STATUS_BAR_KEY, undefined);
		return;
	}

	let badge: string;
	if (config.toolMode === "pi-tools") {
		badge = "AGY:pi-tools";
	} else if (config.toolMode === "agy-tools") {
		badge = `AGY:native(${config.agyExecutionMode})`;
	} else {
		badge = `AGY:hybrid(${config.agyExecutionMode})`;
	}

	if (typeof telemetry.usedTokens === "number" && telemetry.usedTokens > 0) {
		if (typeof telemetry.windowSize === "number" && telemetry.windowSize > 0) {
			badge += ` ${formatCompactTokens(telemetry.usedTokens)}/${formatCompactTokens(telemetry.windowSize)}`;
		} else {
			badge += ` ${formatCompactTokens(telemetry.usedTokens)}`;
		}
	}

	if (telemetry.backendCompactedRecently) {
		badge += " [checkpointed]";
	}

	ctx.ui.setStatus(STATUS_BAR_KEY, badge);
}
