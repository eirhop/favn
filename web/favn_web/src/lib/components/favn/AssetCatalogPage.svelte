<script lang="ts">
	import { Badge } from '$lib/components/ui/badge';
	import { Button } from '$lib/components/ui/button';
	import { Input } from '$lib/components/ui/input';
	import * as Alert from '$lib/components/ui/alert';
	import * as Card from '$lib/components/ui/card';
	import AssetCatalogTable from './AssetCatalogTable.svelte';
	import type { AssetCatalogItem, AssetCatalogPageData } from '$lib/asset_catalog_types';

	type AssetRecord = AssetCatalogItem & {
		ref?: string | null;
		targetId?: string | null;
		name?: string | null;
		friendlyName?: string | null;
		module?: string | null;
		status?: string | null;
		health?: string | null;
		kind?: string | null;
		type?: string | null;
		domain?: string | null;
		lastRunAt?: string | null;
		runsCount?: number | null;
		tags?: string[] | null;
		storagePath?: string | null;
		relation?: string | { path?: string | null; name?: string | null } | null;
	};

	type CatalogData = AssetCatalogPageData & {
		activeManifestVersionId?: string | null;
		assets?: AssetCatalogItem[];
		loadError?: string | null;
	};

	let { catalog } = $props<{
		catalog: CatalogData | null;
	}>();

	let query = $state('');
	let statusFilter = $state('all');
	let typeFilter = $state('all');
	let domainFilter = $state('all');

	let assets: AssetCatalogItem[] = $derived(catalog?.assets ?? []);
	let activeManifestVersionId = $derived(catalog?.activeManifestVersionId ?? null);
	let loadError = $derived(catalog?.loadError ?? null);

	let statusOptions = $derived(
		uniqueValues(assets.map((asset: AssetCatalogItem) => health(asset)))
	);
	let typeOptions = $derived(
		uniqueValues(assets.map((asset: AssetCatalogItem) => typeLabel(asset)))
	);
	let domainOptions = $derived(
		uniqueValues(
			assets
				.map((asset: AssetCatalogItem) => domainLabel(asset))
				.filter((value: string) => value !== '—')
		)
	);

	let filteredAssets = $derived.by(() => {
		const normalizedQuery = query.trim().toLowerCase();
		return assets.filter((asset: AssetCatalogItem) => {
			const matchesStatus = statusFilter === 'all' || health(asset) === statusFilter;
			const matchesType = typeFilter === 'all' || typeLabel(asset) === typeFilter;
			const matchesDomain = domainFilter === 'all' || domainLabel(asset) === domainFilter;
			return (
				matchesStatus && matchesType && matchesDomain && haystack(asset).includes(normalizedQuery)
			);
		});
	});

	let counts = $derived.by(() => {
		const summary = { total: assets.length, healthy: 0, failed: 0, running: 0, unknown: 0 };
		for (const asset of assets) {
			const value = health(asset);
			if (['healthy', 'succeeded', 'success', 'ok'].includes(value)) summary.healthy += 1;
			else if (value === 'failed' || value === 'failing') summary.failed += 1;
			else if (value === 'running') summary.running += 1;
			else summary.unknown += 1;
		}
		return summary;
	});

	function assetRef(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return item.ref ?? item.targetId ?? item.module ?? item.name ?? 'unknown-asset';
	}

	function assetName(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return (
			item.friendlyName ??
			item.name ??
			assetRef(asset).split('.').filter(Boolean).at(-1) ??
			assetRef(asset)
		);
	}

	function health(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return (item.health ?? item.status ?? 'unknown').toLowerCase();
	}

	function typeLabel(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return (item.kind ?? item.type ?? 'asset').toLowerCase();
	}

	function domainLabel(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return item.domain ?? '—';
	}

	function relationLabel(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		if (typeof item.relation === 'string') return item.relation;
		return item.relation?.path ?? item.relation?.name ?? item.storagePath ?? '';
	}

	function haystack(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return [
			assetName(asset),
			assetRef(asset),
			item.module,
			health(asset),
			typeLabel(asset),
			domainLabel(asset),
			relationLabel(asset),
			...(item.tags ?? [])
		]
			.filter(Boolean)
			.join(' ')
			.toLowerCase();
	}

	function uniqueValues(values: string[]): string[] {
		return [...new Set(values.filter(Boolean))].sort((left, right) => left.localeCompare(right));
	}

	function clearFilters() {
		query = '';
		statusFilter = 'all';
		typeFilter = 'all';
		domainFilter = 'all';
	}

	function updateQuery(event: Event) {
		query = (event.currentTarget as HTMLInputElement).value;
	}

	function selectClass(active: boolean) {
		return [
			'h-9 rounded-md border bg-white px-2 text-sm text-slate-700 shadow-sm',
			active && 'border-slate-950 text-slate-950'
		];
	}

	const summaryCards = $derived([
		{ label: 'Total', value: counts.total, status: 'all', tone: 'slate' },
		{ label: 'Healthy', value: counts.healthy, status: 'healthy', tone: 'emerald' },
		{ label: 'Failed', value: counts.failed, status: 'failed', tone: 'red' },
		{ label: 'Running', value: counts.running, status: 'running', tone: 'blue' },
		{ label: 'Never run / Unknown', value: counts.unknown, status: 'unknown', tone: 'amber' }
	]);
</script>

<section class="space-y-6">
	<div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
		<div>
			<div class="flex flex-wrap items-center gap-2">
				<h1 class="text-3xl font-semibold tracking-tight">Assets</h1>
				<Badge variant="outline">Active manifest: {activeManifestVersionId ?? 'none'}</Badge>
				<Badge variant="secondary">{assets.length} assets</Badge>
			</div>
			<p class="mt-1 max-w-2xl text-sm text-slate-600">
				Browse manifest assets, current run health, storage targets, and quick inspection links.
			</p>
		</div>
		<Button href="/assets" variant="outline">Refresh</Button>
	</div>

	{#if loadError}
		<Alert.Root variant="destructive">
			<Alert.Title>Failed to load assets</Alert.Title>
			<Alert.Description>{loadError}</Alert.Description>
		</Alert.Root>
	{/if}

	<div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
		{#each summaryCards as card (card.label)}
			<button
				type="button"
				class={[
					'rounded-xl border bg-white p-4 text-left shadow-sm transition hover:-translate-y-0.5 hover:border-slate-300 hover:shadow',
					statusFilter === card.status && 'border-slate-950 ring-1 ring-slate-950'
				]}
				onclick={() => (statusFilter = card.status)}
				aria-pressed={statusFilter === card.status}
			>
				<p class="text-xs font-medium tracking-wide text-slate-500 uppercase">{card.label}</p>
				<p class="mt-2 text-2xl font-semibold text-slate-950">{card.value}</p>
			</button>
		{/each}
	</div>

	<Card.Root>
		<Card.Header>
			<div class="grid gap-3 xl:grid-cols-[minmax(16rem,1fr)_auto] xl:items-center">
				<div>
					<label class="sr-only" for="asset-search">Search assets</label>
					<Input
						id="asset-search"
						placeholder="Search assets, modules, tags, or storage paths…"
						value={query}
						oninput={updateQuery}
					/>
				</div>
				<div class="flex flex-wrap items-center gap-2">
					<label class="text-xs font-medium text-slate-500" for="asset-status-filter">Status</label>
					<select
						id="asset-status-filter"
						class={selectClass(statusFilter !== 'all')}
						bind:value={statusFilter}
					>
						<option value="all">All statuses</option>
						{#each statusOptions as option (option)}
							<option value={option}>{option}</option>
						{/each}
					</select>

					<label class="text-xs font-medium text-slate-500" for="asset-type-filter">Kind</label>
					<select
						id="asset-type-filter"
						class={selectClass(typeFilter !== 'all')}
						bind:value={typeFilter}
					>
						<option value="all">All kinds</option>
						{#each typeOptions as option (option)}
							<option value={option}>{option}</option>
						{/each}
					</select>

					<label class="text-xs font-medium text-slate-500" for="asset-domain-filter">Domain</label>
					<select
						id="asset-domain-filter"
						class={selectClass(domainFilter !== 'all')}
						bind:value={domainFilter}
					>
						<option value="all">All domains</option>
						{#each domainOptions as option (option)}
							<option value={option}>{option}</option>
						{/each}
					</select>

					<Button variant="ghost" size="sm" onclick={clearFilters}>Clear filters</Button>
				</div>
			</div>
		</Card.Header>
		<Card.Content>
			{#if !activeManifestVersionId}
				<div class="rounded-xl border border-dashed bg-slate-50 p-8 text-center">
					<h2 class="text-lg font-semibold">No active manifest</h2>
					<p class="mx-auto mt-2 max-w-xl text-sm text-slate-600">
						Publish or activate a manifest before the asset catalog can show graph nodes.
					</p>
				</div>
			{:else if assets.length === 0}
				<div class="rounded-xl border border-dashed bg-slate-50 p-8 text-center">
					<h2 class="text-lg font-semibold">No assets in this manifest</h2>
					<p class="mx-auto mt-2 max-w-xl text-sm text-slate-600">
						The active manifest loaded, but it did not contain any asset definitions.
					</p>
				</div>
			{:else if filteredAssets.length === 0}
				<div class="rounded-xl border border-dashed bg-slate-50 p-8 text-center">
					<h2 class="text-lg font-semibold">No assets match these filters</h2>
					<p class="mx-auto mt-2 max-w-xl text-sm text-slate-600">
						Try a broader search term or clear the status, kind, and domain filters.
					</p>
					<Button class="mt-4" variant="outline" onclick={clearFilters}>Clear filters</Button>
				</div>
			{:else}
				<div class="mb-3 text-sm text-slate-500">
					Showing {filteredAssets.length} of {assets.length} assets
				</div>
				<AssetCatalogTable assets={filteredAssets} />
			{/if}
		</Card.Content>
	</Card.Root>
</section>
