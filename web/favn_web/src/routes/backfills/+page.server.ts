import { redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { clearWebSessionCookie } from '$lib/server/session';
import {
	orchestratorGetActiveManifest,
	orchestratorListCoverageBaselines
} from '$lib/server/orchestrator';
import { clearLocalSession, requireProtectedPageSession } from '$lib/server/session_guard';
import { normalizePipelineTargets } from '$lib/pipeline_run_submission';
import { normalizeCoverageBaselines } from '$lib/server/backfill_views';

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

export const load: PageServerLoad = async (event) => {
	const { locals, cookies } = event;
	const session = await requireProtectedPageSession(event);
	const [activeManifestResponse, baselinesResponse] = await Promise.all([
		orchestratorGetActiveManifest(session),
		orchestratorListCoverageBaselines(session, new URLSearchParams({ limit: '50', offset: '0' }))
	]);

	if (activeManifestResponse.status === 401 || baselinesResponse.status === 401) {
		clearLocalSession({ locals, cookies });
		throw redirect(303, '/login');
	}

	const activeManifestPayload = activeManifestResponse.ok
		? await readJsonOr(activeManifestResponse, null)
		: null;
	const baselinesPayload = baselinesResponse.ok ? await readJsonOr(baselinesResponse, []) : [];

	return {
		session: locals.session,
		activeManifestVersionId: normalizeActiveManifest(activeManifestPayload),
		pipelineTargets: normalizePipelineTargets(activeManifestPayload),
		coverageBaselines: normalizeCoverageBaselines(baselinesPayload),
		loadError: activeManifestResponse.ok ? null : `HTTP ${activeManifestResponse.status}`
	};
};

export const actions: Actions = {
	logout: async ({ cookies, locals }) => {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}
};
