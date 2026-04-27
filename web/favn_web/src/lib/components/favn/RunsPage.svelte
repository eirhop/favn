<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';
	import * as Alert from '$lib/components/ui/alert';
	import * as Card from '$lib/components/ui/card';
	import { Input } from '$lib/components/ui/input';
	import RunsTable from './RunsTable.svelte';
	import type { RunSummaryView } from '$lib/run_view_types';

	let { runs, loadError = null } = $props<{
		runs: RunSummaryView[];
		loadError?: string | null;
	}>();

	let query = $state('');
	let activeTab = $state('all');
	const tabs = ['all', 'running', 'failed', 'succeeded', 'cancelled'];
	const tabLabels: Record<string, string> = {
		all: 'All',
		running: 'Running',
		failed: 'Failed',
		succeeded: 'Succeeded',
		cancelled: 'Cancelled'
	};
	let liveUpdates = $state(true);

	let filteredRuns = $derived(
		runs.filter((run: RunSummaryView) => {
			const tabMatches = activeTab === 'all' || run.status === activeTab;
			const haystack =
				`${run.id} ${run.target} ${run.targetType} ${run.status} ${run.trigger} ${run.manifestVersionId ?? ''}`.toLowerCase();
			return tabMatches && haystack.includes(query.trim().toLowerCase());
		})
	);

	let counts = $derived.by(() => {
		const result = { all: runs.length, running: 0, failed: 0, succeeded: 0, cancelled: 0 };
		for (const run of runs) {
			if (run.status in result && run.status !== 'all')
				result[run.status as keyof typeof result] += 1;
		}
		return result;
	});

	function updateQuery(event: Event) {
		query = (event.currentTarget as HTMLInputElement).value;
	}

	function tabClass(tab: string) {
		return activeTab === tab
			? 'inline-flex h-8 items-center justify-center gap-2 rounded-md bg-slate-950 px-3 text-xs font-medium text-white shadow'
			: 'inline-flex h-8 items-center justify-center gap-2 rounded-md px-3 text-xs font-medium hover:bg-slate-100';
	}
</script>

<section class="space-y-6">
	<div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
		<div>
			<h1 class="text-3xl font-semibold tracking-tight">Runs</h1>
			<p class="mt-1 text-sm text-slate-600">Inspect local pipeline and asset executions.</p>
		</div>
		<div class="flex gap-2">
			<Button href="/runs" variant="outline">Refresh</Button>
			<Button
				variant={liveUpdates ? 'default' : 'outline'}
				onclick={() => (liveUpdates = !liveUpdates)}
			>
				Live updates: {liveUpdates ? 'on' : 'off'}
			</Button>
		</div>
	</div>

	{#if loadError}
		<Alert.Root variant="destructive">
			<Alert.Title>Failed to load runs</Alert.Title>
			<Alert.Description>{loadError}</Alert.Description>
		</Alert.Root>
	{/if}

	<Card.Root>
		<Card.Header>
			<div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
				<div class="flex flex-wrap gap-2" role="tablist" aria-label="Run status filters">
					{#each tabs as tab (tab)}
						<button
							type="button"
							class={tabClass(tab)}
							onclick={() => (activeTab = tab)}
							aria-pressed={activeTab === tab}
						>
							{tabLabels[tab]}
							<Badge variant="secondary">{counts[tab as keyof typeof counts]}</Badge>
						</button>
					{/each}
				</div>
				<div class="w-full lg:w-96">
					<label class="sr-only" for="run-search">Search runs</label>
					<Input
						id="run-search"
						placeholder="Search run id, pipeline, asset, status…"
						value={query}
						oninput={updateQuery}
					/>
				</div>
			</div>
		</Card.Header>
		<Card.Content>
			{#if runs.length === 0}
				<div class="rounded-xl border border-dashed bg-slate-50 p-8 text-center">
					<h2 class="text-lg font-semibold">No runs yet</h2>
					<p class="mt-2 text-sm text-slate-600">Start the local stack with:</p>
					<div class="mx-auto mt-4 grid max-w-2xl gap-2 text-left font-mono text-xs">
						<code class="rounded-md bg-white p-3">mix favn.dev</code>
						<code class="rounded-md bg-white p-3">mix favn.run MyApp.Pipelines.ImportCustomers</code
						>
					</div>
					<Button
						class="mt-4"
						variant="outline"
						onclick={() =>
							navigator.clipboard?.writeText('mix favn.run MyApp.Pipelines.ImportCustomers')}
						>Copy command</Button
					>
				</div>
			{:else if filteredRuns.length === 0}
				<div class="rounded-lg border border-dashed p-8 text-center text-sm text-slate-500">
					No runs match this filter.
				</div>
			{:else}
				<RunsTable runs={filteredRuns} />
			{/if}
		</Card.Content>
	</Card.Root>
</section>
