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
	FAVN_WEB_PUBLIC_ORIGIN: 'https://favn.example.com'
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
			publicWebOrigin: 'https://favn.example.com'
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

	it('requires the public web origin to be an exact origin', () => {
		const missing = validationErrorFor({
			...validEnv,
			FAVN_WEB_PUBLIC_ORIGIN: undefined
		});
		expect(missing.issues).toContainEqual({
			variable: 'FAVN_WEB_PUBLIC_ORIGIN',
			message: 'is required and must be an absolute http:// or https:// URL',
			value: '[missing]'
		});

		const withPath = validationErrorFor({
			...validEnv,
			FAVN_WEB_PUBLIC_ORIGIN: 'https://favn.example.com/app'
		});
		expect(withPath.issues).toContainEqual({
			variable: 'FAVN_WEB_PUBLIC_ORIGIN',
			message: 'must be an origin only, without path, query, or fragment',
			value: '[redacted]'
		});
	});

	it('rejects non-local http public web origins in production config', () => {
		const error = validationErrorFor({
			...validEnv,
			FAVN_WEB_PUBLIC_ORIGIN: 'http://favn.example.com'
		});

		expect(error.issues).toContainEqual({
			variable: 'FAVN_WEB_PUBLIC_ORIGIN',
			message: 'must use https:// unless the host is localhost, 127.0.0.1, or ::1',
			value: '[redacted]'
		});
	});

	it('allows local http public web origins for local-only production smoke tests', () => {
		for (const publicOrigin of [
			'http://localhost:4173',
			'http://127.0.0.1:4173',
			'http://[::1]:4173'
		]) {
			expect(
				validateWebProductionRuntimeConfig({
					...validEnv,
					FAVN_WEB_PUBLIC_ORIGIN: publicOrigin
				})
			).toMatchObject({ publicWebOrigin: publicOrigin });
		}
	});

	it('requires long production secrets and redacts diagnostics', () => {
		const error = validationErrorFor({
			FAVN_WEB_ORCHESTRATOR_BASE_URL: 'https://orchestrator.internal',
			FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN: 'short-token',
			FAVN_WEB_PUBLIC_ORIGIN: 'https://favn.example.com'
		});
		expect(error.issues).toEqual([
			{
				variable: 'FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN',
				message: 'must be at least 32 characters',
				value: '[redacted]'
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
