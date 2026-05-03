import { describe, expect, it } from 'vitest';
import {
	applyLogoutCacheClearHeaders,
	applyNoStoreHeaders,
	applySecurityHeaders,
	shouldApplyStrictTransportSecurity
} from './security_headers';

describe('applySecurityHeaders', () => {
	it('adds explicit browser security headers', () => {
		const response = applySecurityHeaders(new Response('ok'));

		expect(response.headers.get('x-content-type-options')).toBe('nosniff');
		expect(response.headers.get('referrer-policy')).toBe('strict-origin-when-cross-origin');
		expect(response.headers.get('x-frame-options')).toBe('DENY');
		expect(response.headers.get('permissions-policy')).toContain('camera=()');
		expect(response.headers.get('cross-origin-opener-policy')).toBe('same-origin');
		expect(response.headers.get('cross-origin-resource-policy')).toBe('same-origin');
		expect(response.headers.get('x-dns-prefetch-control')).toBe('off');
	});

	it('adds fallback CSP with frame protection when SvelteKit did not set one', () => {
		const response = applySecurityHeaders(new Response('ok'));
		const csp = response.headers.get('content-security-policy') ?? '';

		expect(csp).toContain("default-src 'self'");
		expect(csp).toContain("frame-ancestors 'none'");
		expect(csp).toContain("object-src 'none'");
	});

	it('preserves an existing SvelteKit CSP', () => {
		const response = applySecurityHeaders(
			new Response('ok', { headers: { 'content-security-policy': "default-src 'none'" } })
		);

		expect(response.headers.get('content-security-policy')).toBe("default-src 'none'");
	});

	it('does not add HSTS by default in tests/dev mode', () => {
		const response = applySecurityHeaders(new Response('ok'));

		expect(response.headers.has('strict-transport-security')).toBe(false);
	});

	it('enables HSTS only for production HTTPS public origins', () => {
		expect(
			shouldApplyStrictTransportSecurity(
				{ NODE_ENV: 'production', FAVN_WEB_PUBLIC_ORIGIN: 'https://favn.example' },
				{ dev: false }
			)
		).toBe(true);
		expect(
			shouldApplyStrictTransportSecurity(
				{ NODE_ENV: 'production', FAVN_WEB_PUBLIC_ORIGIN: 'http://127.0.0.1:5173' },
				{ dev: false }
			)
		).toBe(false);
		expect(
			shouldApplyStrictTransportSecurity(
				{ NODE_ENV: 'development', FAVN_WEB_PUBLIC_ORIGIN: 'https://favn.example' },
				{ dev: false }
			)
		).toBe(false);
		expect(
			shouldApplyStrictTransportSecurity(
				{ NODE_ENV: 'production', FAVN_WEB_PUBLIC_ORIGIN: 'https://favn.example' },
				{ dev: true }
			)
		).toBe(false);
	});
});

describe('cache control security headers', () => {
	it('adds no-store headers for authenticated pages and BFF JSON', () => {
		const response = applyNoStoreHeaders(new Response('ok'));

		expect(response.headers.get('cache-control')).toBe('no-store');
		expect(response.headers.get('pragma')).toBe('no-cache');
		expect(response.headers.get('expires')).toBe('0');
	});

	it('adds logout cache clearing without clearing cookies or storage', () => {
		const response = applyLogoutCacheClearHeaders(new Response(null, { status: 303 }));

		expect(response.headers.get('cache-control')).toBe('no-store');
		expect(response.headers.get('clear-site-data')).toBe('"cache"');
		expect(response.headers.get('clear-site-data')).not.toContain('cookies');
		expect(response.headers.get('clear-site-data')).not.toContain('storage');
	});
});
