<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { resolve } from '$app/paths';
	import { Badge } from '$lib/components/ui/badge';
	import * as Card from '$lib/components/ui/card';
	import * as Table from '$lib/components/ui/table';
	import type { AssetWindowStateView, BackfillPage } from '$lib/backfill_view_types';

	let { statesPage, loadError = null } = $props<{
		statesPage: BackfillPage<AssetWindowStateView>;
		loadError?: string | null;
	}>();
</script>

<section class="space-y-6">
	<div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
		<div>
			<h1 class="text-3xl font-semibold tracking-tight">Asset window states</h1>
			<p class="mt-1 text-sm text-slate-600">Latest projected asset/window execution state.</p>
		</div>
		<Button href="/backfills" variant="outline">Backfills</Button>
	</div>
	{#if loadError}<p class="text-sm text-red-700">
			Failed to load asset window states: {loadError}
		</p>{/if}
	<h2 class="sr-only">Asset window state table</h2>
	<Card.Root>
		<Card.Header>
			<Card.Title>Window states</Card.Title>
			<Card.Description
				>{statesPage.pagination.total ?? statesPage.items.length} reported rows</Card.Description
			>
		</Card.Header>
		<Card.Content>
			{#if statesPage.items.length === 0}
				<div class="rounded-lg border border-dashed bg-white p-4 text-sm text-slate-600">
					No asset/window states are projected yet.
				</div>
			{:else}
				<div class="overflow-hidden rounded-lg border bg-white">
					<Table.Root>
						<Table.Header
							><Table.Row
								><Table.Head>Asset</Table.Head><Table.Head>Window</Table.Head><Table.Head
									>Status</Table.Head
								><Table.Head>Latest run</Table.Head><Table.Head>Updated</Table.Head></Table.Row
							></Table.Header
						>
						<Table.Body>
							{#each statesPage.items as state (state.windowKey + (state.assetRefName ?? ''))}
								<Table.Row>
									<Table.Cell
										>{state.assetRefModule ??
											'—'}{#if state.assetRefName}.{/if}{state.assetRefName ?? ''}</Table.Cell
									>
									<Table.Cell
										><p class="font-mono text-xs">{state.windowKey}</p>
										<p class="text-xs text-slate-500">
											{state.windowStartAt ?? '—'} → {state.windowEndAt ?? '—'}
										</p></Table.Cell
									>
									<Table.Cell
										><Badge variant="outline">{state.status ?? 'unknown'}</Badge></Table.Cell
									>
									<Table.Cell
										>{#if state.latestRunId}<a
												class="underline"
												href={resolve(`/runs/${state.latestRunId}`)}>{state.latestRunId}</a
											>{:else}—{/if}</Table.Cell
									>
									<Table.Cell>{state.updatedAt ?? '—'}</Table.Cell>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		</Card.Content>
	</Card.Root>
</section>
