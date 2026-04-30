<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';
	import * as Card from '$lib/components/ui/card';
	import * as Table from '$lib/components/ui/table';
	import type { BackfillPage, CoverageBaselineView } from '$lib/backfill_view_types';

	let { baselinesPage, loadError = null } = $props<{
		baselinesPage: BackfillPage<CoverageBaselineView>;
		loadError?: string | null;
	}>();
</script>

<section class="space-y-6">
	<div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
		<div>
			<h1 class="text-3xl font-semibold tracking-tight">Coverage baselines</h1>
			<p class="mt-1 text-sm text-slate-600">Projected baseline coverage for backfill planning.</p>
		</div>
		<Button href="/backfills" variant="outline">Backfills</Button>
	</div>
	{#if loadError}<p class="text-sm text-red-700">Failed to load baselines: {loadError}</p>{/if}
	<h2 class="sr-only">Coverage baseline table</h2>
	<Card.Root>
		<Card.Header>
			<Card.Title>Baselines</Card.Title>
			<Card.Description
				>{baselinesPage.pagination.total ?? baselinesPage.items.length} reported rows</Card.Description
			>
		</Card.Header>
		<Card.Content>
			{#if baselinesPage.items.length === 0}
				<div class="rounded-lg border border-dashed bg-white p-4 text-sm text-slate-600">
					No coverage baselines yet. Backfills can still use explicit ranges without a baseline.
				</div>
			{:else}
				<div class="overflow-hidden rounded-lg border bg-white">
					<Table.Root>
						<Table.Header
							><Table.Row
								><Table.Head>Baseline</Table.Head><Table.Head>Pipeline</Table.Head><Table.Head
									>Window</Table.Head
								><Table.Head>Coverage until</Table.Head><Table.Head>Status</Table.Head></Table.Row
							></Table.Header
						>
						<Table.Body>
							{#each baselinesPage.items as baseline (baseline.baselineId)}
								<Table.Row>
									<Table.Cell class="font-mono text-xs">{baseline.baselineId}</Table.Cell>
									<Table.Cell>{baseline.pipelineModule ?? '—'}</Table.Cell>
									<Table.Cell
										>{baseline.windowKind ?? '—'} · {baseline.timezone ?? 'tz?'}</Table.Cell
									>
									<Table.Cell>{baseline.coverageUntil ?? '—'}</Table.Cell>
									<Table.Cell
										><Badge variant="outline">{baseline.status ?? 'unknown'}</Badge></Table.Cell
									>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		</Card.Content>
	</Card.Root>
</section>
