<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';
	import * as Alert from '$lib/components/ui/alert';
	import * as Card from '$lib/components/ui/card';
	import BackfillWindowsTable from './BackfillWindowsTable.svelte';
	import PaginationControls from './PaginationControls.svelte';
	import type { BackfillPage, BackfillWindowView } from '$lib/backfill_view_types';
	import type { RunDetailView } from '$lib/run_view_types';

	let {
		run,
		windowsPage,
		loadError = null
	} = $props<{
		run: RunDetailView;
		windowsPage: BackfillPage<BackfillWindowView>;
		loadError?: string | null;
	}>();

	let statusCounts = $derived.by(() => {
		const counts: Record<string, number> = {};
		for (const window of windowsPage.items)
			counts[window.status] = (counts[window.status] ?? 0) + 1;
		return Object.entries(counts);
	});
</script>

<section class="space-y-6">
	<div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
		<div>
			<h1 class="text-3xl font-semibold tracking-tight">Backfill {run.id}</h1>
			<p class="mt-1 text-sm text-slate-600">Parent run status and child windows.</p>
		</div>
		<div class="flex gap-2">
			<Button href={`/runs/${run.id}`} variant="outline">Open run inspector</Button>
			<Button href="/backfills" variant="outline">Backfills</Button>
		</div>
	</div>

	<h2 class="sr-only">Parent summary</h2>
	<Card.Root>
		<Card.Header>
			<Card.Title>Parent summary</Card.Title>
			<Card.Description
				>{run.target} · manifest {run.manifestVersionId ?? 'not reported'}</Card.Description
			>
		</Card.Header>
		<Card.Content class="grid gap-3 text-sm md:grid-cols-4">
			<div>
				<p class="text-slate-500">Status</p>
				<Badge variant="outline">{run.status}</Badge>
			</div>
			<div>
				<p class="text-slate-500">Started</p>
				<p>{run.startedAt}</p>
			</div>
			<div>
				<p class="text-slate-500">Duration</p>
				<p>{run.duration}</p>
			</div>
			<div>
				<p class="text-slate-500">Loaded windows</p>
				<p>{windowsPage.items.length}</p>
			</div>
		</Card.Content>
	</Card.Root>

	<h2 class="sr-only">Window distribution</h2>
	<Card.Root>
		<Card.Header>
			<Card.Title>Window status distribution</Card.Title>
			<Card.Description>Counts are from the currently loaded page.</Card.Description>
		</Card.Header>
		<Card.Content class="flex flex-wrap gap-2">
			{#if statusCounts.length === 0}
				<p class="text-sm text-slate-600">No windows loaded.</p>
			{:else}
				{#each statusCounts as [status, count] (status)}
					<Badge variant="secondary">{status}: {count}</Badge>
				{/each}
			{/if}
		</Card.Content>
	</Card.Root>

	{#if loadError}
		<Alert.Root variant="destructive">
			<p class="mb-1 font-medium tracking-tight">Failed to load backfill windows</p>
			<Alert.Description>{loadError}</Alert.Description>
		</Alert.Root>
	{/if}

	<BackfillWindowsTable backfillRunId={run.id} windows={windowsPage.items} />
	<PaginationControls pagination={windowsPage.pagination} />
</section>
