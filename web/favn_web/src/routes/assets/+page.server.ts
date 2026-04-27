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

function isNoActiveManifestResponse(response: Response, payload: unknown): boolean {
	if (response.status !== 404) return false;

	const body = JSON.stringify(payload).toLowerCase();
	return body.includes('active_manifest_not_set') || body.includes('not_found');
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

	const activeManifestPayload = await readJsonOr(activeManifestResponse, null);
	const activeManifestLoadError =
		activeManifestResponse.ok ||
		isNoActiveManifestResponse(activeManifestResponse, activeManifestPayload)
			? null
			: `HTTP ${activeManifestResponse.status}`;
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
			loadError: activeManifestLoadError
		},
		activeManifestVersionId: catalog.manifest.versionId,
		assets: filteredAssets,
		loadError: activeManifestLoadError
	};
};

export const actions: Actions = {
	logout: async ({ cookies, locals }) => {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}
};
