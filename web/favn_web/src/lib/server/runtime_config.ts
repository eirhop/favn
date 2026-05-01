import { dev } from '$app/environment';
import { env } from '$env/dynamic/private';

const REQUIRED_SECRET_LENGTH = 32;

export type WebProductionRuntimeConfig = {
	orchestratorBaseUrl: string;
	orchestratorServiceToken: string;
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
		validateRequiredSecret('FAVN_WEB_SESSION_SECRET', runtimeEnv.FAVN_WEB_SESSION_SECRET)
	].filter((issue): issue is WebProductionRuntimeConfigIssue => issue !== null);

	if (issues.length > 0) {
		throw new WebProductionRuntimeConfigError(issues);
	}

	return {
		orchestratorBaseUrl: runtimeEnv.FAVN_WEB_ORCHESTRATOR_BASE_URL as string,
		orchestratorServiceToken: runtimeEnv.FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN as string,
		sessionSecret: runtimeEnv.FAVN_WEB_SESSION_SECRET as string
	};
}

export function shouldValidateWebProductionRuntimeConfig(runtimeEnv: RuntimeEnv): boolean {
	return !dev && runtimeEnv.NODE_ENV !== 'test';
}

export function validateCurrentWebProductionRuntimeConfig(): WebProductionRuntimeConfig | null {
	if (!shouldValidateWebProductionRuntimeConfig(env)) {
		return null;
	}

	return validateWebProductionRuntimeConfig(env);
}
