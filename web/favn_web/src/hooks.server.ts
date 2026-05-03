import type { Handle } from '@sveltejs/kit';
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

ensureCurrentWebProductionRuntimeConfig();

function shouldApplyNoStore(event: Parameters<Handle>[0]['event']): boolean {
	const path = event.url.pathname;
	return (
		Boolean(event.locals.session) ||
		path.startsWith('/api/web/v1/') ||
		path.startsWith('/runs') ||
		path.startsWith('/assets') ||
		path.startsWith('/backfills')
	);
}

function finalizeResponse(event: Parameters<Handle>[0]['event'], response: Response): Response {
	const secured = applySecurityHeaders(response);
	return shouldApplyNoStore(event) ? applyNoStoreHeaders(secured) : secured;
}

export const handle: Handle = async ({ event, resolve }) => {
	const session = readWebSessionCookie(event.cookies);

	if (!session && event.cookies.get(FAVN_WEB_SESSION_COOKIE)) {
		clearWebSessionCookie(event.cookies);
	}

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

	return finalizeResponse(event, await resolve(event));
};
