import { error, redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { clearWebSessionCookie } from '$lib/server/session';
import {
	orchestratorGetActiveManifest,
	orchestratorListRuns,
	orchestratorSubmitRun
} from '$lib/server/orchestrator';
import { normalizeAssetCatalogDetail } from '$lib/server/asset_catalog_views';
import { clearLocalSession, requireProtectedPageSession } from '$lib/server/session_guard';

async function readJsonOr(response: Response, fallback: unknown): Promise<unknown> {
	try {
		return await response.json();
	} catch {
		return fallback;
	}
}

function nonEmptyFormString(value: FormDataEntryValue | null): string | null {
	if (typeof value !== 'string') return null;
	const trimmed = value.trim();
	return trimmed.length > 0 ? trimmed : null;
}

async function loadDetail(
	locals: App.Locals,
	cookies: import('@sveltejs/kit').Cookies,
	assetRef: string
) {
	const session = await requireProtectedPageSession({ locals, cookies });

	const [activeManifestResponse, runsResponse] = await Promise.all([
		orchestratorGetActiveManifest(session),
		orchestratorListRuns(session)
	]);

	if (activeManifestResponse.status === 401 || runsResponse.status === 401) {
		clearLocalSession({ locals, cookies });
		throw redirect(303, '/login');
	}

	if (!activeManifestResponse.ok) {
		throw error(activeManifestResponse.status, 'Failed to load active manifest');
	}

	const activeManifestPayload = await readJsonOr(activeManifestResponse, null);
	const runsPayload = runsResponse.ok ? await readJsonOr(runsResponse, []) : [];
	const detail = normalizeAssetCatalogDetail(activeManifestPayload, runsPayload, assetRef);

	if (!detail) throw error(404, 'Asset was not found in the active manifest');

	return detail;
}

export const load: PageServerLoad = async ({ locals, cookies, params }) => {
	const detail = await loadDetail(locals, cookies, params.asset_ref);
	return {
		session: locals.session,
		detail,
		asset: {
			...detail.asset,
			recentRuns: detail.recentRuns,
			lineage: {
				upstream: detail.dependencies,
				downstream: detail.dependents,
				dependenciesAvailable: false
			},
			capabilities: {
				assetOnlyScopeAvailable: false,
				dependenciesAvailable: false,
				notes: detail.capabilityNotes.map((note) => note.message)
			},
			raw: detail.raw
		},
		activeManifestVersionId: detail.asset.manifestVersionId ?? null,
		recentRuns: detail.recentRuns,
		runActions: {
			withDependencies: '?/runWithUpstream',
			asset: null
		}
	};
};

export const actions: Actions = {
	logout: async ({ cookies, locals }) => {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	},
	runWithUpstream: async ({ request, cookies, locals, params }) => {
		const formData = await request.formData();
		const scope = nonEmptyFormString(formData.get('scope'));
		const targetId = nonEmptyFormString(formData.get('target'));
		const manifestVersionId = nonEmptyFormString(formData.get('manifest_version_id'));

		if (scope !== 'with_dependencies') {
			throw error(400, 'Expected with_dependencies run scope');
		}

		const detail = await loadDetail(locals, cookies, params.asset_ref);
		const submittedTargetId = targetId ?? detail.asset.targetId;
		if (!submittedTargetId) throw error(400, 'Asset target id is not available');

		const response = await orchestratorSubmitRun(locals.session!, {
			target: { type: 'asset', id: submittedTargetId },
			manifest_selection: manifestVersionId
				? { mode: 'version', manifest_version_id: manifestVersionId }
				: { mode: 'active' },
			dependencies: 'all'
		});

		if (response.status === 401) {
			clearLocalSession({ locals, cookies });
			throw redirect(303, '/login');
		}

		if (!response.ok) throw error(response.status, 'Failed to submit asset run');

		throw redirect(303, `/assets/${encodeURIComponent(params.asset_ref)}`);
	}
};
