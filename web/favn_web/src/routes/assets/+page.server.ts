import { redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { clearWebSessionCookie } from '$lib/server/session';
import { orchestratorGetActiveManifest, orchestratorListRuns } from '$lib/server/orchestrator';
import {
	filterAssetCatalogItems,
	normalizeAssetCatalogList
} from '$lib/server/asset_catalog_views';

async function readJsonOr(response: Response, fallback: unknown): Promise<unknown> {
	try {
		return await response.json();
	} catch {
		return fallback;
	}
}

export const load: PageServerLoad = async ({ locals, cookies, url }) => {
	if (!locals.session) throw redirect(303, '/login');

	const [activeManifestResponse, runsResponse] = await Promise.all([
		orchestratorGetActiveManifest(locals.session),
		orchestratorListRuns(locals.session)
	]);

	if (activeManifestResponse.status === 401 || runsResponse.status === 401) {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}

	const activeManifestPayload = activeManifestResponse.ok
		? await readJsonOr(activeManifestResponse, null)
		: null;
	const runsPayload = runsResponse.ok ? await readJsonOr(runsResponse, []) : [];
	const catalog = normalizeAssetCatalogList(activeManifestPayload, runsPayload);
	const filteredAssets = filterAssetCatalogItems(catalog.assets, {
		status: url.searchParams.get('status'),
		domain: url.searchParams.get('domain'),
		kind: url.searchParams.get('kind'),
		text: url.searchParams.get('q')
	});

	return {
		session: locals.session,
		catalog: { ...catalog, assets: filteredAssets },
		assetCatalog: {
			...catalog,
			activeManifestVersionId: catalog.manifest.versionId,
			assets: filteredAssets,
			loadError: activeManifestResponse.ok ? null : `HTTP ${activeManifestResponse.status}`
		},
		activeManifestVersionId: catalog.manifest.versionId,
		assets: filteredAssets,
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
