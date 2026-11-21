export interface ValidationIssue {
	path: string;
	message: string;
}

export interface SchemaValidationResult {
	valid: boolean;
	issues: ValidationIssue[];
	formattedError?: string;
}

function getJsonType(value: unknown): string {
	if (value === null) return "null";
	if (Array.isArray(value)) return "array";
	return typeof value;
}

function matchesSingleType(value: unknown, expectedType: string): boolean {
	switch (expectedType) {
		case "string":
			return typeof value === "string";
		case "number":
			return typeof value === "number" && Number.isFinite(value);
		case "integer":
			return typeof value === "number" && Number.isInteger(value);
		case "boolean":
			return typeof value === "boolean";
		case "null":
			return value === null;
		case "array":
			return Array.isArray(value);
		case "object":
			return typeof value === "object" && value !== null && !Array.isArray(value);
		default:
			return true;
	}
}

function deepEqualPrimitiveOrJson(a: unknown, b: unknown): boolean {
	if (Object.is(a, b)) return true;
	try {
		return JSON.stringify(a) === JSON.stringify(b);
	} catch {
		return false;
	}
}

function resolveLocalSchemaRef(
	ref: string,
	rootSchema: Record<string, unknown>,
): Record<string, unknown> | undefined {
	if (!ref.startsWith("#/")) return undefined;
	const parts = ref
		.slice(2)
		.split("/")
		.map((part) => part.replace(/~1/g, "/").replace(/~0/g, "~"));
	let current: unknown = rootSchema;
	for (const part of parts) {
		if (!current || typeof current !== "object" || Array.isArray(current)) {
			return undefined;
		}
		current = (current as Record<string, unknown>)[part];
	}
	if (current && typeof current === "object" && !Array.isArray(current)) {
		return current as Record<string, unknown>;
	}
	return undefined;
}

function collectDeclaredPropertyKeys(
	schema: Record<string, unknown>,
	rootSchema: Record<string, unknown>,
	depth = 0,
): Set<string> {
	const keys = new Set<string>();
	if (depth > 8) return keys;

	if (typeof schema.$ref === "string") {
		const target = resolveLocalSchemaRef(schema.$ref, rootSchema);
		if (target) {
			for (const k of collectDeclaredPropertyKeys(target, rootSchema, depth + 1)) {
				keys.add(k);
			}
		}
	}
	if (schema.properties && typeof schema.properties === "object" && !Array.isArray(schema.properties)) {
		for (const k of Object.keys(schema.properties as Record<string, unknown>)) {
			keys.add(k);
		}
	}
	for (const compositeKey of ["allOf", "anyOf", "oneOf"] as const) {
		const branches = schema[compositeKey];
		if (Array.isArray(branches)) {
			for (const branch of branches) {
				if (branch && typeof branch === "object" && !Array.isArray(branch)) {
					for (const k of collectDeclaredPropertyKeys(
						branch as Record<string, unknown>,
						rootSchema,
						depth + 1,
					)) {
						keys.add(k);
					}
				}
			}
		}
	}
	return keys;
}

function validateNode(
	value: unknown,
	schema: Record<string, unknown>,
	currentPath: string,
	issues: ValidationIssue[],
	rootSchema: Record<string, unknown>,
	refDepth = 0,
): void {
	if (typeof schema.$ref === "string" && refDepth < 8) {
		const resolved = resolveLocalSchemaRef(schema.$ref, rootSchema);
		if (resolved) {
			validateNode(value, resolved, currentPath, issues, rootSchema, refDepth + 1);
		}
	}

	if ("const" in schema && !deepEqualPrimitiveOrJson(value, schema.const)) {
		issues.push({
			path: currentPath,
			message: `期望固定值 ${JSON.stringify(schema.const)}，实际收到 ${JSON.stringify(value)}`,
		});
		return;
	}

	if (Array.isArray(schema.enum)) {
		const matched = schema.enum.some((candidate) => deepEqualPrimitiveOrJson(value, candidate));
		if (!matched) {
			issues.push({
				path: currentPath,
				message: `值必须属于枚举集合 ${JSON.stringify(schema.enum)}，实际收到 ${JSON.stringify(value)}`,
			});
			return;
		}
	}

	if (Array.isArray(schema.allOf) && schema.allOf.length > 0) {
		for (const branch of schema.allOf) {
			if (branch && typeof branch === "object" && !Array.isArray(branch)) {
				validateNode(
					value,
					branch as Record<string, unknown>,
					currentPath,
					issues,
					rootSchema,
					refDepth + 1,
				);
			}
		}
	}

	if (Array.isArray(schema.anyOf) && schema.anyOf.length > 0) {
		const branchValid = schema.anyOf.some((branch) => {
			if (!branch || typeof branch !== "object") return true;
			const subIssues: ValidationIssue[] = [];
			validateNode(
				value,
				branch as Record<string, unknown>,
				currentPath,
				subIssues,
				rootSchema,
				refDepth + 1,
			);
			return subIssues.length === 0;
		});
		if (!branchValid) {
			issues.push({
				path: currentPath,
				message: `参数未匹配 anyOf 中的任一候选结构（实际类型：${getJsonType(value)}）`,
			});
			return;
		}
	}

	if (Array.isArray(schema.oneOf) && schema.oneOf.length > 0) {
		const okCount = schema.oneOf.filter((branch) => {
			if (!branch || typeof branch !== "object") return true;
			const subIssues: ValidationIssue[] = [];
			validateNode(
				value,
				branch as Record<string, unknown>,
				currentPath,
				subIssues,
				rootSchema,
				refDepth + 1,
			);
			return subIssues.length === 0;
		}).length;
		if (okCount === 0) {
			issues.push({
				path: currentPath,
				message: `参数未匹配 oneOf 中的任一候选结构（实际类型：${getJsonType(value)}）`,
			});
			return;
		}
	}

	if (schema.type !== undefined) {
		const allowedTypes = Array.isArray(schema.type)
			? schema.type.filter((t): t is string => typeof t === "string")
			: typeof schema.type === "string"
				? [schema.type]
				: [];

		if (allowedTypes.length > 0 && !allowedTypes.some((t) => matchesSingleType(value, t))) {
			issues.push({
				path: currentPath,
				message: `期望类型 ${allowedTypes.join(" | ")}，实际收到 ${getJsonType(value)}`,
			});
			return;
		}
	}

	if (typeof value === "string") {
		if (typeof schema.minLength === "number" && value.length < schema.minLength) {
			issues.push({
				path: currentPath,
				message: `字符串长度至少为 ${schema.minLength}，实际为 ${value.length}`,
			});
		}
		if (typeof schema.maxLength === "number" && value.length > schema.maxLength) {
			issues.push({
				path: currentPath,
				message: `字符串长度上限为 ${schema.maxLength}，实际为 ${value.length}`,
			});
		}
	}

	if (typeof value === "number") {
		if (typeof schema.minimum === "number" && value < schema.minimum) {
			issues.push({
				path: currentPath,
				message: `数值不能小于 ${schema.minimum}，实际为 ${value}`,
			});
		}
		if (typeof schema.maximum === "number" && value > schema.maximum) {
			issues.push({
				path: currentPath,
				message: `数值不能大于 ${schema.maximum}，实际为 ${value}`,
			});
		}
	}

	if (Array.isArray(value)) {
		if (typeof schema.minItems === "number" && value.length < schema.minItems) {
			issues.push({
				path: currentPath,
				message: `数组元素数量至少为 ${schema.minItems}，实际为 ${value.length}`,
			});
		}
		if (schema.items && typeof schema.items === "object" && !Array.isArray(schema.items)) {
			const itemSchema = schema.items as Record<string, unknown>;
			value.forEach((item, index) => {
				validateNode(item, itemSchema, `${currentPath}[${index}]`, issues, rootSchema, refDepth);
			});
		}
	}

	if (typeof value === "object" && value !== null && !Array.isArray(value)) {
		const record = value as Record<string, unknown>;
		const properties =
			schema.properties && typeof schema.properties === "object"
				? (schema.properties as Record<string, unknown>)
				: undefined;
		const hasComposite =
			Array.isArray(schema.allOf) || Array.isArray(schema.anyOf) || Array.isArray(schema.oneOf);
		const compositeKeys = hasComposite
			? collectDeclaredPropertyKeys(schema, rootSchema)
			: undefined;

		if (Array.isArray(schema.required)) {
			for (const reqKey of schema.required) {
				if (typeof reqKey === "string" && !(reqKey in record)) {
					issues.push({
						path: currentPath === "$" ? `$.${reqKey}` : `${currentPath}.${reqKey}`,
						message: `缺少必填字段 "${reqKey}"`,
					});
				}
			}
		}

		if (properties || schema.additionalProperties === false) {
			for (const [key, propVal] of Object.entries(record)) {
				if (propVal === undefined) continue;
				const propSchema = properties?.[key];
				if (propSchema && typeof propSchema === "object") {
					validateNode(
						propVal,
						propSchema as Record<string, unknown>,
						currentPath === "$" ? `$.${key}` : `${currentPath}.${key}`,
						issues,
						rootSchema,
						refDepth,
					);
				} else if (schema.additionalProperties === false && !compositeKeys?.has(key)) {
					issues.push({
						path: currentPath === "$" ? `$.${key}` : `${currentPath}.${key}`,
						message: `包含未声明的多余字段 "${key}"`,
					});
				}
			}
		}
	}
}

/**
 * 在进入宿主工具循环前对 MCP 工具调用参数进行 JSON Schema 预校验。
 * 校验未通过时生成包含具体错误路径与期望 Schema 的自愈提示，引导模型在同一 ACP 轮次内自动修正重试。
 */
export function validateToolArguments(
	toolName: string,
	args: unknown,
	schema: unknown,
): SchemaValidationResult {
	if (!schema || typeof schema !== "object") {
		return { valid: true, issues: [] };
	}

	const normalizedArgs = args === undefined ? {} : args;
	const issues: ValidationIssue[] = [];
	const root = schema as Record<string, unknown>;
	validateNode(normalizedArgs, root, "$", issues, root, 0);

	if (issues.length === 0) {
		return { valid: true, issues: [] };
	}

	const issueLines = issues.map((item) => `- ${item.path}: ${item.message}`).join("\n");
	const formattedError = [
		`Invalid arguments for tool "${toolName}":`,
		issueLines,
		"",
		"Expected JSON Schema:",
		JSON.stringify(schema, null, 2),
		"",
		"Please correct the arguments according to the schema and call the tool again.",
	].join("\n");

	return {
		valid: false,
		issues,
		formattedError,
	};
}
