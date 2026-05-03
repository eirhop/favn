import type { RequestEvent } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';

const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);
const VALID_FETCH_SITES = new Set(['same-origin', 'same-site', 'cross-site', 'none']);

export type SameOriginDecision =
	| { allowed: true }
	| { allowed: false; code: 'csrf_rejected'; message: string };

export function isUnsafeMethod(method: string): boolean {
	return !SAFE_METHODS.has(method.toUpperCase());
}

function configuredPublicOrigin(): string | null {
	const configured = env.FAVN_WEB_PUBLIC_ORIGIN ?? process.env.FAVN_WEB_PUBLIC_ORIGIN;
	if (!configured) return null;

	try {
		return new URL(configured).origin;
	} catch {
		return null;
	}
}

function expectedOrigin(event: Pick<RequestEvent, 'url'>): string {
	return configuredPublicOrigin() ?? event.url.origin;
}

function originOf(value: string): string | null {
	try {
		return new URL(value).origin;
	} catch {
		return null;
	}
}

function reject(message: string): SameOriginDecision {
	return { allowed: false, code: 'csrf_rejected', message };
}

export function checkSameOriginMutation(
	event: Pick<RequestEvent, 'request' | 'url'>
): SameOriginDecision {
	if (!isUnsafeMethod(event.request.method)) return { allowed: true };

	const headers = event.request.headers;
	const fetchSite = headers.get('sec-fetch-site');

	if (fetchSite && VALID_FETCH_SITES.has(fetchSite)) {
		if (fetchSite === 'same-origin') return { allowed: true };
		if (fetchSite === 'cross-site') return reject('Cross-site unsafe requests are not allowed');
		if (fetchSite === 'same-site') return reject('Same-site unsafe requests are not trusted');
		return reject('Unsafe navigation requests require same-origin proof');
	}

	const allowedOrigin = expectedOrigin(event);
	const origin = headers.get('origin');
	if (origin) {
		return originOf(origin) === allowedOrigin
			? { allowed: true }
			: reject('Request Origin does not match the web origin');
	}

	const referer = headers.get('referer');
	if (referer) {
		return originOf(referer) === allowedOrigin
			? { allowed: true }
			: reject('Request Referer does not match the web origin');
	}

	return reject('Unsafe request is missing same-origin headers');
}
