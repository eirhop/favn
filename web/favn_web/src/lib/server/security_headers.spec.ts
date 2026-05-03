import { describe, expect, it } from 'vitest';
import { applySecurityHeaders } from './security_headers';

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
});
