<script lang="ts">
	import AppShell from '$lib/components/favn/AppShell.svelte';
	import AssetCatalogPage from '$lib/components/favn/AssetCatalogPage.svelte';
	import type { AssetCatalogItem, AssetCatalogPageData } from '$lib/asset_catalog_types';

	type PageWithCatalog = import('./$types').PageData & {
		session: { actor_id: string; provider: string };
		activeManifestVersionId: string | null;
		catalog?: AssetCatalogPageData | null;
		assetCatalog?: AssetCatalogPageData | null;
		assets?: AssetCatalogItem[];
		loadError?: string | null;
		assetCatalogLoadError?: string | null;
	};

	let { data } = $props<{ data: import('./$types').PageData }>();

	let pageData = $derived(data as PageWithCatalog);
	let catalog = $derived(
		pageData.assetCatalog ??
			pageData.catalog ?? {
				activeManifestVersionId: pageData.activeManifestVersionId,
				assets: pageData.assets ?? [],
				loadError: pageData.assetCatalogLoadError ?? pageData.loadError ?? null
			}
	);
</script>

<AppShell session={pageData.session} activeManifestVersionId={pageData.activeManifestVersionId}>
	<AssetCatalogPage {catalog} />
</AppShell>
