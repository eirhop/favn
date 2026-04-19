import type { Handle } from '@sveltejs/kit';
import {
	clearWebSessionCookie,
	FAVN_WEB_SESSION_COOKIE,
	readWebSessionCookie
} from '$lib/server/session';

export const handle: Handle = async ({ event, resolve }) => {
	const session = readWebSessionCookie(event.cookies);

	if (!session && event.cookies.get(FAVN_WEB_SESSION_COOKIE)) {
		clearWebSessionCookie(event.cookies);
	}

	event.locals.session = session;

	return resolve(event);
};
