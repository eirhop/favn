import { dev } from '$app/environment';
import { currentWebRuntimeEnv } from './runtime_config';

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

const HSTS_ONE_YEAR = 'max-age=31536000';

export const NO_STORE_HEADERS = {
	'cache-control': 'no-store',
	pragma: 'no-cache',
	expires: '0'
};

export const LOGOUT_CACHE_CLEAR_HEADERS = {
	...NO_STORE_HEADERS,
	'clear-site-data': '"cache"'
};

type RuntimeEnv = {
	NODE_ENV?: string;
	FAVN_WEB_PUBLIC_ORIGIN?: string;
};

type RuntimeMode = {
	dev: boolean;
};

function mutableResponse(response: Response): Response {
	try {
		response.headers.set('x-favn-header-mutability-check', '1');
		response.headers.delete('x-favn-header-mutability-check');
		return response;
	} catch {
		return new Response(response.body, response);
	}
}

export function shouldApplyStrictTransportSecurity(
	runtimeEnv: RuntimeEnv = currentWebRuntimeEnv(),
	runtimeMode: RuntimeMode = { dev }
): boolean {
	if (runtimeMode.dev || runtimeEnv.NODE_ENV !== 'production') return false;
	if (!runtimeEnv.FAVN_WEB_PUBLIC_ORIGIN) return false;

	try {
		return new URL(runtimeEnv.FAVN_WEB_PUBLIC_ORIGIN).protocol === 'https:';
	} catch {
		return false;
	}
}

export function applyNoStoreHeaders(input: Response): Response {
	const response = mutableResponse(input);
	for (const [name, value] of Object.entries(NO_STORE_HEADERS)) {
		response.headers.set(name, value);
	}
	return response;
}

export function applyLogoutCacheClearHeaders(input: Response): Response {
	const response = mutableResponse(input);
	for (const [name, value] of Object.entries(LOGOUT_CACHE_CLEAR_HEADERS)) {
		response.headers.set(name, value);
	}
	return response;
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

	if (shouldApplyStrictTransportSecurity()) {
		response.headers.set('strict-transport-security', HSTS_ONE_YEAR);
	}

	response.headers.delete('x-powered-by');

	return response;
}
