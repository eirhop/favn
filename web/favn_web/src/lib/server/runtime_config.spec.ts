import { describe, expect, it } from 'vitest';
import {
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
			sessionSecret: 'web-session-secret-32-char-minimum'
		});
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
});
