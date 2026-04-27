<script lang="ts">
	import { resolve } from '$app/paths';
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';
	import * as Card from '$lib/components/ui/card';
	import StatusBadge from './StatusBadge.svelte';
	import RunSummaryCards from './RunSummaryCards.svelte';
	import ErrorPanel from './ErrorPanel.svelte';
	import AssetExecutionTable from './AssetExecutionTable.svelte';
	import OutputRelationsTable from './OutputRelationsTable.svelte';
	import RunTimeline from './RunTimeline.svelte';
	import ManifestSummaryCard from './ManifestSummaryCard.svelte';
	import AssetDetailSheet from './AssetDetailSheet.svelte';
	import type { AssetExecutionView, RunDetailView } from '$lib/run_view_types';

	let { run } = $props<{ run: RunDetailView }>();
	let activeTab = $state('overview');
	let selectedAsset = $state<AssetExecutionView | null>(null);

	const tabs = ['overview', 'assets', 'timeline', 'outputs', 'manifest', 'raw'];
	const tabLabels: Record<string, string> = {
		overview: 'Overview',
		assets: 'Assets',
		timeline: 'Timeline',
		outputs: 'Outputs',
		manifest: 'Manifest',
		raw: 'Raw'
	};

	let rawJson = $derived(JSON.stringify(run.raw, null, 2));
	let isTerminal = $derived(['succeeded', 'failed', 'cancelled'].includes(run.status));
	let failedAsset = $derived(
		run.assets.find((asset: AssetExecutionView) => asset.id === run.failedAssetId) ??
			run.assets.find((asset: AssetExecutionView) => asset.error) ??
			null
	);
	let stages = $derived.by(() => {
		const groups: Array<{ stage: string; assets: AssetExecutionView[] }> = [];
		for (const asset of run.assets) {
			let group = groups.find((candidate) => candidate.stage === asset.stage);
			if (!group) {
				group = { stage: asset.stage, assets: [] };
				groups.push(group);
			}
			group.assets = [...group.assets, asset];
		}
		return groups;
	});

	function copy(text: string | null | undefined) {
		if (text) navigator.clipboard?.writeText(text);
	}

	function tabClass(tab: string) {
		return activeTab === tab
			? 'inline-flex h-8 items-center justify-center rounded-md bg-slate-950 px-3 text-xs font-medium text-white shadow'
			: 'inline-flex h-8 items-center justify-center rounded-md px-3 text-xs font-medium hover:bg-slate-100';
	}
</script>

<section class="space-y-6">
	<div class="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
		<div>
			<nav class="text-sm text-slate-500" aria-label="Breadcrumb">
				<a href={resolve('/runs')} class="hover:text-slate-950">Runs</a>
				<span class="mx-1">/</span><span>{run.id}</span>
			</nav>
			<div class="mt-3 flex flex-wrap items-center gap-3">
				<h1 class="text-3xl font-semibold tracking-tight">Run {run.id}</h1>
				<StatusBadge status={run.status} />
			</div>
			<p class="mt-2 text-sm font-medium text-slate-700">{run.target}</p>
			<div class="mt-3 flex flex-wrap gap-2 text-xs text-slate-600">
				<Badge variant="outline">Started {run.startedAt ?? '—'}</Badge>
				<Badge variant="outline">Duration {run.duration}</Badge>
				<Badge variant="outline">Triggered {run.trigger}</Badge>
				<Badge variant="outline">Manifest {run.manifestVersionId ?? '—'}</Badge>
			</div>
		</div>
		<div class="flex flex-wrap gap-2">
			{#if !isTerminal}
				<Button variant="destructive" disabled>Cancel</Button>
			{:else}
				<Button
					disabled
					title="Rerun command wiring will use the same manifest version as this run.">Rerun</Button
				>
			{/if}
			<Button variant="outline" onclick={() => copy(run.id)}>Copy run id</Button>
			<Button href={`/api/web/v1/streams/runs/${run.id}`} variant="outline">Open logs</Button>
			<details class="relative">
				<summary
					class="inline-flex h-9 cursor-pointer list-none items-center rounded-md border bg-white px-3 text-sm shadow-sm"
					>More</summary
				>
				<div
					class="absolute right-0 z-10 mt-2 w-56 rounded-md border bg-white p-2 text-sm shadow-lg"
				>
					<p class="px-2 py-1 text-xs text-slate-500">Secondary actions</p>
					<button
						class="block w-full rounded px-2 py-1 text-left hover:bg-slate-100"
						onclick={() => copy(rawJson)}>Copy raw JSON</button
					>
					<a class="block rounded px-2 py-1 hover:bg-slate-100" href={resolve('/runs')}
						>Back to runs</a
					>
				</div>
			</details>
		</div>
	</div>

	<div class="grid gap-6 xl:grid-cols-[minmax(0,7fr)_minmax(20rem,3fr)]">
		<div class="space-y-6">
			<RunSummaryCards {run} />

			{#if run.progressPercent !== null}
				<Card.Root>
					<Card.Content class="pt-6">
						<div class="mb-2 flex justify-between text-sm">
							<span>Progress</span><span
								>{run.assetsCompleted} / {run.assetsTotal} assets completed</span
							>
						</div>
						<div class="h-2 rounded-full bg-slate-100">
							<div
								class="h-2 rounded-full bg-slate-950"
								style:width={`${run.progressPercent}%`}
							></div>
						</div>
					</Card.Content>
				</Card.Root>
			{/if}

			{#if run.error}
				<ErrorPanel
					asset={run.error.asset}
					message={run.error.message}
					oninspect={() => (selectedAsset = failedAsset)}
				/>
			{/if}

			<div class="flex flex-wrap gap-2 border-b pb-2" role="tablist" aria-label="Run detail tabs">
				{#each tabs as tab (tab)}
					<button type="button" class={tabClass(tab)} onclick={() => (activeTab = tab)}
						>{tabLabels[tab]}</button
					>
				{/each}
			</div>

			{#if activeTab === 'overview'}
				<Card.Root>
					<Card.Header
						><Card.Title>Execution by stage</Card.Title><Card.Description
							>Status first, graph later.</Card.Description
						></Card.Header
					>
					<Card.Content class="space-y-3">
						{#each stages as group (group.stage)}
							<div class="rounded-lg border p-4">
								<h3 class="mb-3 text-sm font-semibold">{group.stage}</h3>
								<div class="space-y-2">
									{#each group.assets as asset (asset.id)}
										<button
											class="flex w-full items-center justify-between gap-3 rounded-md px-2 py-1.5 text-left hover:bg-slate-50"
											onclick={() => (selectedAsset = asset)}
										>
											<span class="truncate">{asset.asset}</span>
											<StatusBadge status={asset.status} />
										</button>
									{/each}
								</div>
							</div>
						{/each}
					</Card.Content>
				</Card.Root>
				<Card.Root>
					<Card.Header
						><Card.Title>Output relations</Card.Title><Card.Description
							>Relations created or attempted by this run.</Card.Description
						></Card.Header
					>
					<Card.Content class="flex flex-wrap gap-2">
						{#each run.outputs as output (`overview:${output.asset}:${output.relation}`)}
							<Badge variant={output.failed ? 'destructive' : 'outline'}>{output.relation}</Badge>
						{/each}
					</Card.Content>
				</Card.Root>
			{:else if activeTab === 'assets'}
				<Card.Root>
					<Card.Header
						><Card.Title>Assets</Card.Title><Card.Description
							>Click a row to open the asset detail inspector.</Card.Description
						></Card.Header
					>
					<Card.Content>
						<AssetExecutionTable
							assets={run.assets}
							onselect={(asset) => (selectedAsset = asset)}
						/>
					</Card.Content>
				</Card.Root>
			{:else if activeTab === 'outputs'}
				<Card.Root
					><Card.Header
						><Card.Title>Outputs</Card.Title><Card.Description
							>Copy a relation or a SELECT statement; data preview comes later.</Card.Description
						></Card.Header
					><Card.Content><OutputRelationsTable outputs={run.outputs} /></Card.Content></Card.Root
				>
			{:else if activeTab === 'timeline'}
				<Card.Root
					><Card.Content class="pt-6"
						><RunTimeline events={run.timeline} live={run.status === 'running'} /></Card.Content
					></Card.Root
				>
			{:else if activeTab === 'manifest'}
				<ManifestSummaryCard metadata={run.metadata} />
			{:else}
				<Card.Root
					><Card.Header
						><Card.Title>Raw</Card.Title><Card.Description
							>Normalized BFF source payload.</Card.Description
						></Card.Header
					><Card.Content
						><Button size="sm" variant="outline" onclick={() => copy(rawJson)}>Copy JSON</Button>
						<pre
							class="mt-3 max-h-[32rem] overflow-auto rounded-md bg-slate-950 p-4 text-xs text-slate-50">{rawJson}</pre></Card.Content
					></Card.Root
				>
			{/if}
		</div>

		<aside class="space-y-4">
			<Card.Root
				><Card.Header
					><Card.Title>Recent events</Card.Title><Card.Description
						>Run-scoped stream preview.</Card.Description
					></Card.Header
				><Card.Content class="space-y-3"
					>{#each run.timeline.slice(0, 5) as event (event.id)}<div
							class="rounded-md bg-slate-50 p-3"
						>
							<p class="text-sm font-medium">{event.label}</p>
							<p class="text-xs text-slate-500">{event.timestamp ?? '—'}</p>
							<p class="text-xs text-slate-600">{event.detail}</p>
						</div>{/each}</Card.Content
				></Card.Root
			>
			<Card.Root
				><Card.Header><Card.Title>What next?</Card.Title></Card.Header><Card.Content
					class="space-y-2 text-sm text-slate-600"
					><p>1. Check the failed asset and error.</p>
					<p>2. Copy relation or SQL for local debugging.</p>
					<p>3. Rerun from the same manifest when command support is enabled.</p></Card.Content
				></Card.Root
			>
		</aside>
	</div>

	{#if selectedAsset}
		<AssetDetailSheet
			asset={selectedAsset}
			runId={run.id}
			events={run.timeline}
			onclose={() => (selectedAsset = null)}
		/>
	{/if}
</section>
