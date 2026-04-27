<script lang="ts">
	import { resolve } from '$app/paths';
	import { Button } from '$lib/components/ui/button';
	import * as Table from '$lib/components/ui/table';
	import StatusBadge from './StatusBadge.svelte';
	import type { AssetCatalogItem } from '$lib/asset_catalog_types';

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
		lastRun?: { startedAt?: string | null; finishedAt?: string | null } | null;
		lastRunAt?: string | null;
		runsCount?: number | null;
		tags?: string[] | null;
		storagePath?: string | null;
		relation?: string | { path?: string | null; name?: string | null } | null;
	};

	let { assets } = $props<{ assets: AssetCatalogItem[] }>();

	function assetRef(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return item.ref ?? item.targetId ?? item.module ?? item.name ?? 'unknown-asset';
	}

	function assetName(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return item.friendlyName ?? item.name ?? compactRef(assetRef(asset));
	}

	function compactRef(ref: string): string {
		const parts = ref.split('.').filter(Boolean);
		return parts.length > 1 ? parts.at(-1)! : ref;
	}

	function health(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return item.health ?? item.status ?? 'unknown';
	}

	function typeLabel(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return item.kind ?? item.type ?? 'asset';
	}

	function domainLabel(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return item.domain ?? '—';
	}

	function lastRun(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return item.lastRunAt ?? item.lastRun?.finishedAt ?? item.lastRun?.startedAt ?? 'Never';
	}

	function runsCount(asset: AssetCatalogItem): string {
		const item = asset as AssetRecord;
		return typeof item.runsCount === 'number' ? item.runsCount.toLocaleString() : '0';
	}

	function copyRef(asset: AssetCatalogItem) {
		navigator.clipboard?.writeText(assetRef(asset));
	}
</script>

<div class="overflow-hidden rounded-xl border bg-white">
	<Table.Root>
		<Table.Header>
			<Table.Row>
				<Table.Head>Asset</Table.Head>
				<Table.Head>Health</Table.Head>
				<Table.Head>Type</Table.Head>
				<Table.Head>Domain</Table.Head>
				<Table.Head>Last run</Table.Head>
				<Table.Head>Runs count</Table.Head>
				<Table.Head>Actions</Table.Head>
			</Table.Row>
		</Table.Header>
		<Table.Body>
			{#each assets as asset (assetRef(asset))}
				<Table.Row>
					<Table.Cell>
						<div class="max-w-[28rem] min-w-0">
							<p class="truncate font-medium text-slate-950" title={assetName(asset)}>
								{assetName(asset)}
							</p>
							<p class="truncate font-mono text-xs text-slate-500" title={assetRef(asset)}>
								{assetRef(asset)}
							</p>
						</div>
					</Table.Cell>
					<Table.Cell><StatusBadge status={health(asset)} /></Table.Cell>
					<Table.Cell class="capitalize">{typeLabel(asset)}</Table.Cell>
					<Table.Cell>{domainLabel(asset)}</Table.Cell>
					<Table.Cell class="text-sm whitespace-nowrap text-slate-600">{lastRun(asset)}</Table.Cell>
					<Table.Cell>{runsCount(asset)}</Table.Cell>
					<Table.Cell>
						<div class="flex flex-wrap items-center gap-2">
							<Button
								href={resolve(`/assets/${encodeURIComponent(assetRef(asset))}`)}
								variant="ghost"
								size="sm"
							>
								Inspect
							</Button>
							<button
								type="button"
								class="rounded-md border px-2 py-1 text-xs text-slate-600 hover:bg-slate-50 hover:text-slate-950"
								onclick={() => copyRef(asset)}
							>
								Copy ref
							</button>
						</div>
					</Table.Cell>
				</Table.Row>
			{/each}
		</Table.Body>
	</Table.Root>
</div>
