import { redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { clearWebSessionCookie, publicWebSession } from '$lib/server/session';
import {
	orchestratorGetActiveManifest,
	orchestratorListAssetWindowStates,
	orchestratorRevokeSession
} from '$lib/server/orchestrator';
import { clearLocalSession, requireProtectedPageSession } from '$lib/server/session_guard';
import { normalizeAssetWindowStates } from '$lib/server/backfill_views';

type JsonRecord = Record<string, unknown>;
function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}
function asString(value: unknown): string | null {
	return typeof value === 'string' && value.length > 0 ? value : null;
}
function normalizeActiveManifest(payload: unknown): string | null {
	const dataObj = isRecord(payload) && isRecord(payload.data) ? payload.data : payload;
	if (!isRecord(dataObj)) return null;
	if (isRecord(dataObj.manifest))
		return asString(dataObj.manifest.manifest_version_id) ?? asString(dataObj.manifest.id);
	return asString(dataObj.manifest_version_id) ?? asString(dataObj.id);
}
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
	const [statesResponse, activeManifestResponse] = await Promise.all([
		orchestratorListAssetWindowStates(session, search),
		orchestratorGetActiveManifest(session)
	]);
	if (statesResponse.status === 401 || activeManifestResponse.status === 401) {
		clearLocalSession({ locals, cookies });
		throw redirect(303, '/login');
	}
	const activeManifestPayload = activeManifestResponse.ok
		? await readJsonOr(activeManifestResponse, null)
		: null;
	return {
		session: publicWebSession(session),
		activeManifestVersionId: normalizeActiveManifest(activeManifestPayload),
		statesPage: normalizeAssetWindowStates(
			statesResponse.ok ? await readJsonOr(statesResponse, []) : []
		),
		loadError: statesResponse.ok ? null : `HTTP ${statesResponse.status}`
	};
};

export const actions: Actions = {
	logout: async ({ cookies, locals }) => {
		if (locals.session) await orchestratorRevokeSession(locals.session).catch(() => null);
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}
};
