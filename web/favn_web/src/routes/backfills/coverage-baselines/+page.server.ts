import { redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { clearWebSessionCookie } from '$lib/server/session';
import { orchestratorListCoverageBaselines } from '$lib/server/orchestrator';
import { clearLocalSession, requireProtectedPageSession } from '$lib/server/session_guard';
import { normalizeCoverageBaselines } from '$lib/server/backfill_views';

async function readJsonOr(response: Response, fallback: unknown): Promise<unknown> {
	try {
		return await response.json();
	} catch {
		return fallback;
	}
}

export const load: PageServerLoad = async (event) => {
	const { locals, cookies, url } = event;
	const session = await requireProtectedPageSession(event);
	const search = new URLSearchParams(url.searchParams);
	if (!search.has('limit')) search.set('limit', '50');
	if (!search.has('offset')) search.set('offset', '0');
	const response = await orchestratorListCoverageBaselines(session, search);
	if (response.status === 401) {
		clearLocalSession({ locals, cookies });
		throw redirect(303, '/login');
	}
	return {
		session: locals.session,
		activeManifestVersionId: null,
		baselinesPage: normalizeCoverageBaselines(response.ok ? await readJsonOr(response, []) : []),
		loadError: response.ok ? null : `HTTP ${response.status}`
	};
};

export const actions: Actions = {
	logout: async ({ cookies, locals }) => {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}
};
