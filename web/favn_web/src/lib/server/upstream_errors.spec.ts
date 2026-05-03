import { describe, expect, it } from 'vitest';
import { sanitizeUpstreamPayload } from './upstream_errors';

describe('sanitizeUpstreamPayload', () => {
	it('passes through successful upstream payloads', () => {
		const payload = { data: { ok: true } };
		expect(sanitizeUpstreamPayload(200, payload)).toEqual({ status: 200, payload });
	});

	it('maps raw upstream diagnostics to safe browser errors', () => {
		const raw = {
			error: {
				code: 'validation_failed',
				message: 'SQL failed at /var/lib/favn/prod.db with token secret-token and stack trace'
			}
		};

		const sanitized = sanitizeUpstreamPayload(422, raw);

		expect(sanitized).toEqual({
			status: 422,
			payload: { error: { code: 'validation_failed', message: 'Request validation failed' } }
		});
		expect(JSON.stringify(sanitized)).not.toContain('/var/lib');
		expect(JSON.stringify(sanitized)).not.toContain('secret-token');
	});

	it('preserves allowlisted local-safe validation messages', () => {
		const safe = {
			error: {
				code: 'validation_failed',
				message: 'Invalid JSON body'
			}
		};

		expect(sanitizeUpstreamPayload(422, safe)).toEqual({
			status: 422,
			payload: safe
		});
	});
});
