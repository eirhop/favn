import type { Handle, RequestEvent } from '@sveltejs/kit';
import {
	clearWebSessionCookie,
	FAVN_WEB_SESSION_COOKIE,
	readWebSessionCookie
} from '$lib/server/session';
import { ensureCurrentWebProductionRuntimeConfig } from '$lib/server/runtime_config';
import { checkSameOriginMutation, isUnsafeMethod } from '$lib/server/same_origin';
import { applyNoStoreHeaders, applySecurityHeaders } from '$lib/server/security_headers';
import { checkMutationRateLimit } from '$lib/server/mutation_rate_limit';
import { jsonError, rateLimitedResponse } from '$lib/server/web_api';
import {
	localDevTrustedAuthEnabled,
	localDevTrustedWebSession
} from '$lib/server/local_dev_trusted_auth';

ensureCurrentWebProductionRuntimeConfig();

export const PUBLIC_ROUTES = [
	{ method: 'GET', path: '/login' },
	{ method: 'POST', path: '/login' }
] as const;

export function isPublicRoute(event: RequestEvent): boolean {
	const method = event.request.method.toUpperCase();
	const path = event.url.pathname;
	return PUBLIC_ROUTES.some((route) => route.method === method && route.path === path);
}

export function isWebApiRoute(pathname: string): boolean {
	return pathname.startsWith('/api/web/v1/');
}

export function isPageRequest(event: RequestEvent): boolean {
	return !isWebApiRoute(event.url.pathname);
}

function loginRedirectPath(event: RequestEvent): string {
	const next = `${event.url.pathname}${event.url.search}`;
	return next === '/' ? '/login?next=%2F' : `/login?next=${encodeURIComponent(next)}`;
}

export function unauthenticatedResponse(event: RequestEvent): Response {
	if (isWebApiRoute(event.url.pathname)) {
		return jsonError(401, 'unauthorized', 'Authentication required');
	}

	return new Response(null, {
		status: 303,
		headers: { location: loginRedirectPath(event) }
	});
}

function shouldApplyNoStore(event: RequestEvent): boolean {
	const path = event.url.pathname;
	return Boolean(event.locals.session) || isWebApiRoute(path) || !isPublicRoute(event);
}

function finalizeResponse(event: RequestEvent, response: Response): Response {
	const secured = applySecurityHeaders(response);
	return shouldApplyNoStore(event) ? applyNoStoreHeaders(secured) : secured;
}

export const handle: Handle = async ({ event, resolve }) => {
	const cookieSession = readWebSessionCookie(event.cookies);

	if (!cookieSession && event.cookies.get(FAVN_WEB_SESSION_COOKIE)) {
		clearWebSessionCookie(event.cookies);
	}

	const session =
		cookieSession ?? (localDevTrustedAuthEnabled() ? localDevTrustedWebSession() : null);

	event.locals.session = session;

	const sameOrigin = checkSameOriginMutation(event);
	if (!sameOrigin.allowed) {
		return finalizeResponse(event, jsonError(403, sameOrigin.code, sameOrigin.message));
	}

	if (isUnsafeMethod(event.request.method) && event.url.pathname !== '/login') {
		const rateLimit = checkMutationRateLimit(event);
		if (!rateLimit.allowed) {
			return finalizeResponse(event, rateLimitedResponse(rateLimit.retryAfterSeconds));
		}
	}

	if (!isPublicRoute(event) && !event.locals.session) {
		return finalizeResponse(event, unauthenticatedResponse(event));
	}

	return finalizeResponse(event, await resolve(event));
};
