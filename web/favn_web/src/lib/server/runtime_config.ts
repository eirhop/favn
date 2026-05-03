import { building, dev } from '$app/environment';
import { env } from '$env/dynamic/private';

const REQUIRED_SECRET_LENGTH = 32;
export const DEFAULT_ORCHESTRATOR_BASE_URL = 'http://127.0.0.1:4101';
export const DEFAULT_ORCHESTRATOR_TIMEOUT_MS = 2000;
const MIN_ORCHESTRATOR_TIMEOUT_MS = 100;
const MAX_ORCHESTRATOR_TIMEOUT_MS = 30_000;

export type WebProductionRuntimeConfig = {
	orchestratorBaseUrl: string;
	orchestratorServiceToken: string;
	orchestratorTimeoutMs: number;
	publicWebOrigin: string;
	sessionSecret: string;
};

export type WebProductionRuntimeConfigIssue = {
	variable: string;
	message: string;
	value: string;
};

export class WebProductionRuntimeConfigError extends Error {
	readonly issues: WebProductionRuntimeConfigIssue[];

	constructor(issues: WebProductionRuntimeConfigIssue[]) {
		super(formatRuntimeConfigIssues(issues));
		this.name = 'WebProductionRuntimeConfigError';
		this.issues = issues;
	}
}

type RuntimeEnv = Record<string, string | undefined>;
type RuntimeMode = {
	building: boolean;
	dev: boolean;
};

let cachedProductionRuntimeConfig: WebProductionRuntimeConfig | null | undefined;

export function currentWebRuntimeEnv(): RuntimeEnv {
	return {
		NODE_ENV: env.NODE_ENV ?? process.env.NODE_ENV,
		FAVN_WEB_ORCHESTRATOR_BASE_URL:
			env.FAVN_WEB_ORCHESTRATOR_BASE_URL ?? process.env.FAVN_WEB_ORCHESTRATOR_BASE_URL,
		FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN:
			env.FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN ?? process.env.FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN,
		FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS:
			env.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS ?? process.env.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS,
		FAVN_WEB_PUBLIC_ORIGIN: env.FAVN_WEB_PUBLIC_ORIGIN ?? process.env.FAVN_WEB_PUBLIC_ORIGIN,
		FAVN_WEB_SESSION_SECRET: env.FAVN_WEB_SESSION_SECRET ?? process.env.FAVN_WEB_SESSION_SECRET
	};
}

function isPresent(value: string | undefined): value is string {
	return value !== undefined && value.length > 0;
}

function redacted(value: string | undefined): string {
	return isPresent(value) ? '[redacted]' : '[missing]';
}

function validateAbsoluteHttpUrl(variable: string, value: string | undefined) {
	if (!isPresent(value)) {
		return {
			variable,
			message: 'is required and must be an absolute http:// or https:// URL',
			value: redacted(value)
		};
	}

	let parsed: URL;
	try {
		parsed = new URL(value);
	} catch {
		return {
			variable,
			message: 'must be an absolute http:// or https:// URL',
			value: redacted(value)
		};
	}

	if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
		return {
			variable,
			message: 'must use the http:// or https:// scheme',
			value: redacted(value)
		};
	}

	if (parsed.username || parsed.password) {
		return {
			variable,
			message: 'must not include embedded credentials',
			value: redacted(value)
		};
	}

	return null;
}

function validateAbsoluteOrigin(variable: string, value: string | undefined) {
	const baseIssue = validateAbsoluteHttpUrl(variable, value);
	if (baseIssue) return baseIssue;

	const parsed = new URL(value as string);

	if (parsed.username || parsed.password) {
		return {
			variable,
			message: 'must not include embedded credentials',
			value: redacted(value)
		};
	}

	const hasPath = parsed.pathname !== '' && parsed.pathname !== '/';
	if (hasPath || parsed.search || parsed.hash) {
		return {
			variable,
			message: 'must be an origin only, without path, query, or fragment',
			value: redacted(value)
		};
	}

	return null;
}

function validateRequiredSecret(variable: string, value: string | undefined) {
	if (!isPresent(value)) {
		return {
			variable,
			message: `is required and must be at least ${REQUIRED_SECRET_LENGTH} characters`,
			value: redacted(value)
		};
	}

	if (value.length < REQUIRED_SECRET_LENGTH) {
		return {
			variable,
			message: `must be at least ${REQUIRED_SECRET_LENGTH} characters`,
			value: redacted(value)
		};
	}

	return null;
}

function parseInteger(value: string): number | null {
	if (!/^\d+$/.test(value)) return null;

	const parsed = Number(value);
	return Number.isSafeInteger(parsed) ? parsed : null;
}

function validateOptionalTimeout(variable: string, value: string | undefined) {
	if (!isPresent(value)) return null;

	const parsed = parseInteger(value);

	if (
		parsed === null ||
		parsed < MIN_ORCHESTRATOR_TIMEOUT_MS ||
		parsed > MAX_ORCHESTRATOR_TIMEOUT_MS
	) {
		return {
			variable,
			message: `must be an integer between ${MIN_ORCHESTRATOR_TIMEOUT_MS} and ${MAX_ORCHESTRATOR_TIMEOUT_MS}`,
			value
		};
	}

	return null;
}

function formatRuntimeConfigIssues(issues: WebProductionRuntimeConfigIssue[]): string {
	const details = issues
		.map((issue) => `${issue.variable} ${issue.message} (value: ${issue.value})`)
		.join('; ');

	return `Invalid favn_web production runtime config: ${details}`;
}

export function validateWebProductionRuntimeConfig(
	runtimeEnv: RuntimeEnv
): WebProductionRuntimeConfig {
	const issues = [
		validateAbsoluteHttpUrl(
			'FAVN_WEB_ORCHESTRATOR_BASE_URL',
			runtimeEnv.FAVN_WEB_ORCHESTRATOR_BASE_URL
		),
		validateRequiredSecret(
			'FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN',
			runtimeEnv.FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN
		),
		validateOptionalTimeout(
			'FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS',
			runtimeEnv.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS
		),
		validateAbsoluteOrigin('FAVN_WEB_PUBLIC_ORIGIN', runtimeEnv.FAVN_WEB_PUBLIC_ORIGIN),
		validateRequiredSecret('FAVN_WEB_SESSION_SECRET', runtimeEnv.FAVN_WEB_SESSION_SECRET)
	].filter((issue): issue is WebProductionRuntimeConfigIssue => issue !== null);

	if (issues.length > 0) {
		throw new WebProductionRuntimeConfigError(issues);
	}

	return {
		orchestratorBaseUrl: runtimeEnv.FAVN_WEB_ORCHESTRATOR_BASE_URL as string,
		orchestratorServiceToken: runtimeEnv.FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN as string,
		orchestratorTimeoutMs:
			runtimeEnv.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS === undefined ||
			runtimeEnv.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS.length === 0
				? DEFAULT_ORCHESTRATOR_TIMEOUT_MS
				: Number(runtimeEnv.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS),
		publicWebOrigin: new URL(runtimeEnv.FAVN_WEB_PUBLIC_ORIGIN as string).origin,
		sessionSecret: runtimeEnv.FAVN_WEB_SESSION_SECRET as string
	};
}

export function shouldValidateWebProductionRuntimeConfig(
	runtimeEnv: RuntimeEnv,
	runtimeMode: RuntimeMode = { building, dev }
): boolean {
	return !runtimeMode.dev && !runtimeMode.building && runtimeEnv.NODE_ENV !== 'test';
}

export function validateCurrentWebProductionRuntimeConfig(): WebProductionRuntimeConfig | null {
	const runtimeEnv = currentWebRuntimeEnv();

	if (!shouldValidateWebProductionRuntimeConfig(runtimeEnv)) {
		return null;
	}

	return validateWebProductionRuntimeConfig(runtimeEnv);
}

export function ensureCurrentWebProductionRuntimeConfig(): WebProductionRuntimeConfig | null {
	if (cachedProductionRuntimeConfig !== undefined) return cachedProductionRuntimeConfig;

	cachedProductionRuntimeConfig = validateCurrentWebProductionRuntimeConfig();
	return cachedProductionRuntimeConfig;
}

export function currentWebRuntimeConfig(): WebProductionRuntimeConfig {
	const productionConfig = ensureCurrentWebProductionRuntimeConfig();

	if (productionConfig) return productionConfig;

	const runtimeEnv = currentWebRuntimeEnv();
	const orchestratorServiceToken = runtimeEnv.FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN;
	if (!isPresent(orchestratorServiceToken)) {
		throw new Error('Missing FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN for orchestrator service auth');
	}

	const timeoutIssue = validateOptionalTimeout(
		'FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS',
		runtimeEnv.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS
	);

	if (timeoutIssue) {
		throw new WebProductionRuntimeConfigError([timeoutIssue]);
	}

	return {
		orchestratorBaseUrl: runtimeEnv.FAVN_WEB_ORCHESTRATOR_BASE_URL || DEFAULT_ORCHESTRATOR_BASE_URL,
		orchestratorServiceToken,
		orchestratorTimeoutMs:
			runtimeEnv.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS === undefined ||
			runtimeEnv.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS.length === 0
				? DEFAULT_ORCHESTRATOR_TIMEOUT_MS
				: Number(runtimeEnv.FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS),
		publicWebOrigin: runtimeEnv.FAVN_WEB_PUBLIC_ORIGIN
			? new URL(runtimeEnv.FAVN_WEB_PUBLIC_ORIGIN).origin
			: '',
		sessionSecret: runtimeEnv.FAVN_WEB_SESSION_SECRET || ''
	};
}
