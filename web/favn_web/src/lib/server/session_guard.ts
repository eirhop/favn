import { redirect, type Cookies } from '@sveltejs/kit';
import { orchestratorGetMe } from './orchestrator';
import { clearWebSessionCookie, type WebSession } from './session';

type SessionContext = {
	locals: App.Locals;
	cookies: Cookies;
};

export function clearLocalSession(context: SessionContext): void {
	clearWebSessionCookie(context.cookies);
	context.locals.session = null;
}

export async function validateWebSession(context: SessionContext): Promise<WebSession | null> {
	const session = context.locals.session;
	if (!session) return null;

	const response = await orchestratorGetMe(session);

	if (response.status === 401) {
		clearLocalSession(context);
		return null;
	}

	return session;
}

export async function requireProtectedPageSession(context: SessionContext): Promise<WebSession> {
	const session = await validateWebSession(context);

	if (!session) {
		throw redirect(303, '/login');
	}

	return session;
}
