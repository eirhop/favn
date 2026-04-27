import { error, redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { clearWebSessionCookie } from '$lib/server/session';
import { orchestratorGetActiveManifest, orchestratorGetRun } from '$lib/server/orchestrator';
import { normalizeRunDetail } from '$lib/server/run_views';

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
	if (isRecord(dataObj.manifest)) {
		return asString(dataObj.manifest.manifest_version_id) ?? asString(dataObj.manifest.id);
	}
	return asString(dataObj.manifest_version_id) ?? asString(dataObj.id);
}

async function readJsonOr(response: Response, fallback: unknown): Promise<unknown> {
	try {
		return await response.json();
	} catch {
		return fallback;
	}
}

export const load: PageServerLoad = async ({ locals, cookies, params }) => {
	if (!locals.session) throw redirect(303, '/login');

	const [runResponse, activeManifestResponse] = await Promise.all([
		orchestratorGetRun(locals.session, params.run_id),
		orchestratorGetActiveManifest(locals.session)
	]);

	if (runResponse.status === 401) {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}

	if (!runResponse.ok) {
		throw error(runResponse.status, 'Failed to load run detail');
	}

	const runPayload = await readJsonOr(runResponse, {});
	const activeManifestPayload = activeManifestResponse.ok
		? await readJsonOr(activeManifestResponse, null)
		: null;

	return {
		session: locals.session,
		run: normalizeRunDetail(runPayload, params.run_id),
		activeManifestVersionId: normalizeActiveManifest(activeManifestPayload)
	};
};

export const actions: Actions = {
	logout: async ({ cookies, locals }) => {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}
};
