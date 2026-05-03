const FALLBACK_CSP = [
	"default-src 'self'",
	"base-uri 'self'",
	"object-src 'none'",
	"frame-ancestors 'none'",
	"form-action 'self'",
	"script-src 'self'",
	"style-src 'self'",
	"img-src 'self' data:",
	"font-src 'self' data:",
	"connect-src 'self'"
].join('; ');

function mutableResponse(response: Response): Response {
	try {
		response.headers.set('x-favn-header-mutability-check', '1');
		response.headers.delete('x-favn-header-mutability-check');
		return response;
	} catch {
		return new Response(response.body, response);
	}
}

export function applySecurityHeaders(input: Response): Response {
	const response = mutableResponse(input);

	if (!response.headers.has('content-security-policy')) {
		response.headers.set('content-security-policy', FALLBACK_CSP);
	}

	response.headers.set('x-content-type-options', 'nosniff');
	response.headers.set('referrer-policy', 'strict-origin-when-cross-origin');
	response.headers.set('x-frame-options', 'DENY');
	response.headers.set(
		'permissions-policy',
		'camera=(), microphone=(), geolocation=(), payment=(), usb=(), serial=(), bluetooth=(), interest-cohort=()'
	);
	response.headers.set('cross-origin-opener-policy', 'same-origin');
	response.headers.set('cross-origin-resource-policy', 'same-origin');
	response.headers.set('x-dns-prefetch-control', 'off');
	response.headers.delete('x-powered-by');

	return response;
}
