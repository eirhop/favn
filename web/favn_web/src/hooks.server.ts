import type { Handle } from '@sveltejs/kit';
import {
	clearWebSessionCookie,
	FAVN_WEB_SESSION_COOKIE,
	readWebSessionCookie
} from '$lib/server/session';
import { ensureCurrentWebProductionRuntimeConfig } from '$lib/server/runtime_config';
import { checkSameOriginMutation, isUnsafeMethod } from '$lib/server/same_origin';
import { applySecurityHeaders } from '$lib/server/security_headers';
import { checkMutationRateLimit } from '$lib/server/mutation_rate_limit';
import { jsonError, rateLimitedResponse } from '$lib/server/web_api';

ensureCurrentWebProductionRuntimeConfig();

export const handle: Handle = async ({ event, resolve }) => {
	const session = readWebSessionCookie(event.cookies);

	if (!session && event.cookies.get(FAVN_WEB_SESSION_COOKIE)) {
		clearWebSessionCookie(event.cookies);
	}

	event.locals.session = session;

	const sameOrigin = checkSameOriginMutation(event);
	if (!sameOrigin.allowed) {
		return applySecurityHeaders(jsonError(403, sameOrigin.code, sameOrigin.message));
	}

	if (isUnsafeMethod(event.request.method) && event.url.pathname !== '/login') {
		const rateLimit = checkMutationRateLimit(event);
		if (!rateLimit.allowed) {
			return applySecurityHeaders(rateLimitedResponse(rateLimit.retryAfterSeconds));
		}
	}

	return applySecurityHeaders(await resolve(event));
};
