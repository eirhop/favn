import { afterEach, describe, expect, it, vi } from 'vitest';
import { checkSameOriginMutation } from './same_origin';

function event(method: string, headers: HeadersInit = {}) {
	return {
		request: new Request('https://favn.example.com/api/web/v1/runs', { method, headers }),
		url: new URL('https://favn.example.com/api/web/v1/runs')
	};
}

afterEach(() => {
	vi.unstubAllEnvs();
});

describe('checkSameOriginMutation', () => {
	it('allows safe methods without same-origin proof', () => {
		expect(checkSameOriginMutation(event('GET'))).toEqual({ allowed: true });
	});

	it('rejects cross-site unsafe requests using Fetch Metadata', () => {
		expect(
			checkSameOriginMutation(event('POST', { 'sec-fetch-site': 'cross-site' }))
		).toMatchObject({
			allowed: false,
			code: 'csrf_rejected'
		});
	});

	it('allows same-origin unsafe requests using Fetch Metadata', () => {
		expect(checkSameOriginMutation(event('POST', { 'sec-fetch-site': 'same-origin' }))).toEqual({
			allowed: true
		});
	});

	it('rejects same-site unsafe requests conservatively', () => {
		expect(checkSameOriginMutation(event('POST', { 'sec-fetch-site': 'same-site' }))).toMatchObject(
			{
				allowed: false,
				code: 'csrf_rejected'
			}
		);
	});

	it('falls back to exact Origin validation when Fetch Metadata is absent', () => {
		vi.stubEnv('FAVN_WEB_PUBLIC_ORIGIN', 'https://favn.example.com');

		expect(checkSameOriginMutation(event('POST', { origin: 'https://favn.example.com' }))).toEqual({
			allowed: true
		});
		expect(
			checkSameOriginMutation(event('POST', { origin: 'https://favn.example.com.attacker.test' }))
		).toMatchObject({ allowed: false, code: 'csrf_rejected' });
	});

	it('falls back to exact Referer validation when Origin is absent', () => {
		vi.stubEnv('FAVN_WEB_PUBLIC_ORIGIN', 'https://favn.example.com');

		expect(
			checkSameOriginMutation(event('POST', { referer: 'https://favn.example.com/runs' }))
		).toEqual({ allowed: true });
		expect(
			checkSameOriginMutation(event('POST', { referer: 'https://attacker.test/runs' }))
		).toMatchObject({ allowed: false, code: 'csrf_rejected' });
	});

	it('rejects unsafe requests missing reliable same-origin headers', () => {
		expect(checkSameOriginMutation(event('POST'))).toMatchObject({
			allowed: false,
			code: 'csrf_rejected'
		});
	});
});
