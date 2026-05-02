import {
	currentWebRuntimeEnv,
	validateWebProductionRuntimeConfig,
	WebProductionRuntimeConfigError
} from './runtime_config';

type ReadinessStatus = 'ok' | 'degraded';

export type WebReadinessCheck = {
	check: 'web_config' | 'orchestrator';
	status: ReadinessStatus;
	reason?: string;
	details?: Record<string, unknown>;
};

export type WebReadinessReport = {
	service: 'favn_web';
	status: 'ready' | 'not_ready';
	checks: WebReadinessCheck[];
};

function configFailureDetails(error: unknown): Record<string, unknown> | undefined {
	if (!(error instanceof WebProductionRuntimeConfigError)) return undefined;

	return {
		issues: error.issues.map((issue) => ({
			variable: issue.variable,
			message: issue.message,
			value: issue.value
		}))
	};
}

export async function checkWebReadiness(): Promise<WebReadinessReport> {
	const checks: WebReadinessCheck[] = [];

	let config: ReturnType<typeof validateWebProductionRuntimeConfig>;

	try {
		config = validateWebProductionRuntimeConfig(currentWebRuntimeEnv());
		checks.push({ check: 'web_config', status: 'ok' });
	} catch (error) {
		checks.push({
			check: 'web_config',
			status: 'degraded',
			reason: 'invalid_config',
			details: configFailureDetails(error)
		});

		return { service: 'favn_web', status: 'not_ready', checks };
	}

	const controller = new AbortController();
	const timeout = setTimeout(() => controller.abort(), config.orchestratorTimeoutMs);

	try {
		const response = await fetch(
			new URL('/api/orchestrator/v1/health/ready', config.orchestratorBaseUrl),
			{
				method: 'GET',
				headers: {
					accept: 'application/json',
					authorization: `Bearer ${config.orchestratorServiceToken}`,
					'x-favn-service': 'favn_web'
				},
				signal: controller.signal
			}
		);

		if (response.ok) {
			await response.body?.cancel().catch(() => undefined);
			checks.push({ check: 'orchestrator', status: 'ok' });
			return { service: 'favn_web', status: 'ready', checks };
		}

		await response.body?.cancel().catch(() => undefined);
		checks.push({
			check: 'orchestrator',
			status: 'degraded',
			reason: 'orchestrator_not_ready',
			details: { status: response.status }
		});
	} catch {
		checks.push({
			check: 'orchestrator',
			status: 'degraded',
			reason: controller.signal.aborted ? 'timeout' : 'unreachable'
		});
	} finally {
		clearTimeout(timeout);
	}

	return { service: 'favn_web', status: 'not_ready', checks };
}
