import { fail, redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { orchestratorLoginPassword } from '$lib/server/orchestrator';
import { setWebSessionCookie, webSessionFromLoginPayload } from '$lib/server/session';
import {
	checkLoginAllowed,
	clearLoginThrottleFor,
	recordFailedLogin
} from '$lib/server/login_throttle';

async function tryReadJson(response: Response): Promise<unknown> {
	try {
		return await response.json();
	} catch {
		return null;
	}
}

function safePostLoginRedirect(next: string | null): string {
	if (!next || !next.startsWith('/') || next.startsWith('//')) return '/runs';

	try {
		const parsed = new URL(next, 'http://favn.local');
		if (parsed.origin !== 'http://favn.local') return '/runs';
		return `${parsed.pathname}${parsed.search}${parsed.hash}`;
	} catch {
		return '/runs';
	}
}

export const load: PageServerLoad = async ({ locals }) => {
	if (locals.session) {
		throw redirect(303, '/runs');
	}

	return {};
};

export const actions: Actions = {
	default: async ({ request, cookies, locals, getClientAddress, setHeaders, url }) => {
		const formData = await request.formData();
		const username = String(formData.get('username') ?? '').trim();
		const password = String(formData.get('password') ?? '');

		if (!username || !password) {
			return fail(400, {
				message: 'Username and password are required',
				username
			});
		}

		const clientContext = { getClientAddress };
		const throttle = checkLoginAllowed(clientContext, username);
		if (!throttle.allowed) {
			setHeaders({ 'retry-after': String(throttle.retryAfterSeconds) });
			return fail(429, {
				message: 'Too many login attempts. Try again later.',
				username
			});
		}

		const response = await orchestratorLoginPassword({ username, password }).catch(
			() =>
				new Response(
					JSON.stringify({ error: { message: 'Login service is temporarily unavailable' } }),
					{
						status: 502,
						headers: { 'content-type': 'application/json; charset=utf-8' }
					}
				)
		);

		if (response.status >= 500) {
			await response.body?.cancel().catch(() => undefined);
			return fail(502, {
				message: 'Login service is temporarily unavailable',
				username
			});
		}

		const payload = await tryReadJson(response);

		if (response.ok) {
			const session = webSessionFromLoginPayload(payload);

			if (!session) {
				return fail(502, {
					message: 'Unexpected login response shape',
					username
				});
			}

			setWebSessionCookie(cookies, session);
			clearLoginThrottleFor(clientContext, username);
			locals.session = session;

			throw redirect(303, safePostLoginRedirect(url.searchParams.get('next')));
		}

		recordFailedLogin(clientContext, username);

		return fail(response.status === 401 ? 401 : 400, {
			message: 'Invalid username or password',
			username
		});
	}
};
