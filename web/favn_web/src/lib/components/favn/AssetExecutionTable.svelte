<script lang="ts">
	import { Input } from '$lib/components/ui/input';
	import * as Table from '$lib/components/ui/table';
	import StatusBadge from './StatusBadge.svelte';
	import type { AssetExecutionView } from '$lib/run_view_types';

	let { assets, onselect } = $props<{
		assets: AssetExecutionView[];
		onselect?: (asset: AssetExecutionView) => void;
	}>();

	let assetQuery = $state('');
	let filteredAssets = $derived(
		assets.filter((asset: AssetExecutionView) =>
			`${asset.asset} ${asset.status} ${asset.type} ${asset.output ?? ''}`
				.toLowerCase()
				.includes(assetQuery.trim().toLowerCase())
		)
	);

	function updateAssetQuery(event: Event) {
		assetQuery = (event.currentTarget as HTMLInputElement).value;
	}
</script>

<div class="space-y-4">
	<Input placeholder="Search assets…" value={assetQuery} oninput={updateAssetQuery} />
	<Table.Root>
		<Table.Header>
			<Table.Row>
				<Table.Head>Status</Table.Head>
				<Table.Head>Stage</Table.Head>
				<Table.Head>Asset</Table.Head>
				<Table.Head>Type</Table.Head>
				<Table.Head>Started</Table.Head>
				<Table.Head>Duration</Table.Head>
				<Table.Head>Attempt</Table.Head>
				<Table.Head>Output</Table.Head>
				<Table.Head>Error</Table.Head>
			</Table.Row>
		</Table.Header>
		<Table.Body>
			{#each filteredAssets as asset (asset.id)}
				<Table.Row onclick={() => onselect?.(asset)} class="cursor-pointer">
					<Table.Cell><StatusBadge status={asset.status} /></Table.Cell>
					<Table.Cell>{asset.stageNumber ?? '—'}</Table.Cell>
					<Table.Cell>{asset.asset}</Table.Cell>
					<Table.Cell>{asset.type}</Table.Cell>
					<Table.Cell>{asset.startedAt ?? '—'}</Table.Cell>
					<Table.Cell>{asset.duration}</Table.Cell>
					<Table.Cell>{asset.attempt}</Table.Cell>
					<Table.Cell class="font-mono text-xs">{asset.output ?? '—'}</Table.Cell>
					<Table.Cell class="max-w-64 truncate text-red-700">{asset.error ?? '—'}</Table.Cell>
				</Table.Row>
			{/each}
		</Table.Body>
	</Table.Root>
</div>
