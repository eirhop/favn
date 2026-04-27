import { fail, redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { orchestratorLoginPassword } from '$lib/server/orchestrator';
import { setWebSessionCookie, webSessionFromLoginPayload } from '$lib/server/session';

async function tryReadJson(response: Response): Promise<unknown> {
	try {
		return await response.json();
	} catch {
		return null;
	}
}

function loginErrorMessage(payload: unknown): string {
	if (payload && typeof payload === 'object' && payload !== null && 'error' in payload) {
		const errorValue = (payload as { error?: unknown }).error;

		if (errorValue && typeof errorValue === 'object' && 'message' in errorValue) {
			const messageValue = (errorValue as { message?: unknown }).message;
			if (typeof messageValue === 'string') return messageValue;
		}
	}

	return 'Login failed';
}

export const load: PageServerLoad = async ({ locals }) => {
	if (locals.session) {
		throw redirect(303, '/runs');
	}

	return {};
};

export const actions: Actions = {
	default: async ({ request, cookies, locals }) => {
		const formData = await request.formData();
		const username = String(formData.get('username') ?? '').trim();
		const password = String(formData.get('password') ?? '');

		if (!username || !password) {
			return fail(400, {
				message: 'Username and password are required',
				username
			});
		}

		const response = await orchestratorLoginPassword({ username, password }).catch(
			() =>
				new Response(
					JSON.stringify({ error: { message: 'Unable to reach orchestrator service' } }),
					{
						status: 502,
						headers: { 'content-type': 'application/json; charset=utf-8' }
					}
				)
		);
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
			locals.session = session;

			throw redirect(303, '/runs');
		}

		return fail(response.status === 401 ? 401 : 400, {
			message: loginErrorMessage(payload),
			username
		});
	}
};
