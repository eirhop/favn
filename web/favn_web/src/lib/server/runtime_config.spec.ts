import { describe, expect, it } from 'vitest';
import {
	DEFAULT_ORCHESTRATOR_TIMEOUT_MS,
	shouldValidateWebProductionRuntimeConfig,
	validateWebProductionRuntimeConfig,
	WebProductionRuntimeConfigError
} from './runtime_config';

const validEnv = {
	FAVN_WEB_ORCHESTRATOR_BASE_URL: 'https://orchestrator.internal:4101',
	FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN: 'orchestrator-service-token-32-char-minimum',
	FAVN_WEB_SESSION_SECRET: 'web-session-secret-32-char-minimum'
};

function validationErrorFor(
	env: Record<string, string | undefined>
): WebProductionRuntimeConfigError {
	try {
		validateWebProductionRuntimeConfig(env);
	} catch (error) {
		expect(error).toBeInstanceOf(WebProductionRuntimeConfigError);
		return error as WebProductionRuntimeConfigError;
	}

	throw new Error('Expected production runtime config validation to fail');
}

describe('validateWebProductionRuntimeConfig', () => {
	it('accepts the web production runtime deployment contract', () => {
		expect(validateWebProductionRuntimeConfig(validEnv)).toEqual({
			orchestratorBaseUrl: 'https://orchestrator.internal:4101',
			orchestratorServiceToken: 'orchestrator-service-token-32-char-minimum',
			orchestratorTimeoutMs: DEFAULT_ORCHESTRATOR_TIMEOUT_MS,
			sessionSecret: 'web-session-secret-32-char-minimum'
		});
	});

	it('accepts a bounded orchestrator timeout override', () => {
		expect(
			validateWebProductionRuntimeConfig({
				...validEnv,
				FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS: '1500'
			})
		).toMatchObject({ orchestratorTimeoutMs: 1500 });
	});

	it('requires an absolute http or https orchestrator URL', () => {
		expect(() =>
			validateWebProductionRuntimeConfig({
				...validEnv,
				FAVN_WEB_ORCHESTRATOR_BASE_URL: '/api/orchestrator'
			})
		).toThrow(WebProductionRuntimeConfigError);

		const error = validationErrorFor({
			...validEnv,
			FAVN_WEB_ORCHESTRATOR_BASE_URL: 'ftp://orchestrator.internal'
		});
		expect(error.issues).toContainEqual({
			variable: 'FAVN_WEB_ORCHESTRATOR_BASE_URL',
			message: 'must use the http:// or https:// scheme',
			value: '[redacted]'
		});
	});

	it('rejects embedded credentials in the orchestrator URL', () => {
		const error = validationErrorFor({
			...validEnv,
			FAVN_WEB_ORCHESTRATOR_BASE_URL: 'https://user:password@orchestrator.internal'
		});
		expect(error.issues).toContainEqual({
			variable: 'FAVN_WEB_ORCHESTRATOR_BASE_URL',
			message: 'must not include embedded credentials',
			value: '[redacted]'
		});
		expect(String(error)).not.toContain('password');
	});

	it('requires long production secrets and redacts diagnostics', () => {
		const error = validationErrorFor({
			FAVN_WEB_ORCHESTRATOR_BASE_URL: 'https://orchestrator.internal',
			FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN: 'short-token',
			FAVN_WEB_SESSION_SECRET: undefined
		});
		expect(error.issues).toEqual([
			{
				variable: 'FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN',
				message: 'must be at least 32 characters',
				value: '[redacted]'
			},
			{
				variable: 'FAVN_WEB_SESSION_SECRET',
				message: 'is required and must be at least 32 characters',
				value: '[missing]'
			}
		]);
		expect(String(error)).not.toContain('short-token');
	});

	it('rejects invalid orchestrator timeout values', () => {
		const error = validationErrorFor({
			...validEnv,
			FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS: '0'
		});

		expect(error.issues).toContainEqual({
			variable: 'FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS',
			message: 'must be an integer between 100 and 30000',
			value: '0'
		});
	});

	it('validates production runtime config at runtime but not during tests/builds/dev', () => {
		expect(
			shouldValidateWebProductionRuntimeConfig(
				{ NODE_ENV: 'production' },
				{ dev: false, building: false }
			)
		).toBe(true);
		expect(
			shouldValidateWebProductionRuntimeConfig(
				{ NODE_ENV: 'test' },
				{ dev: false, building: false }
			)
		).toBe(false);
		expect(
			shouldValidateWebProductionRuntimeConfig(
				{ NODE_ENV: 'production' },
				{ dev: true, building: false }
			)
		).toBe(false);
		expect(
			shouldValidateWebProductionRuntimeConfig(
				{ NODE_ENV: 'production' },
				{ dev: false, building: true }
			)
		).toBe(false);
	});
});
