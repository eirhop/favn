<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';
	import * as Card from '$lib/components/ui/card';
	import StatusBadge from './StatusBadge.svelte';
	import ErrorPanel from './ErrorPanel.svelte';
	import AssetExecutionTable from './AssetExecutionTable.svelte';
	import OutputRelationsTable from './OutputRelationsTable.svelte';
	import RunTimeline from './RunTimeline.svelte';
	import AssetDetailSheet from './AssetDetailSheet.svelte';
	import type { AssetExecutionView, RunDetailView } from '$lib/run_view_types';

	let { run } = $props<{ run: RunDetailView }>();
	let activeTab = $state('overview');
	let selectedAsset = $state<AssetExecutionView | null>(null);

	let rawJson = $derived(JSON.stringify(run.raw, null, 2));
	let compactRunId = $derived(shortId(run.id));
	let compactManifestVersionId = $derived(shortId(run.manifestVersionId));
	let compactManifestContentHash = $derived(shortHash(run.manifestContentHash));
	let failedAsset = $derived(
		run.assets.find((asset: AssetExecutionView) => asset.id === run.failedAssetId) ??
			run.assets.find((asset: AssetExecutionView) => asset.error) ??
			null
	);
	let windowItems = $derived(
		[
			{ label: 'Pipeline policy', value: run.windowInfo.pipelinePolicy },
			{ label: 'Requested anchor', value: run.windowInfo.requestedAnchorWindow },
			{ label: 'Resolved anchor', value: run.windowInfo.resolvedAnchorWindow }
		].filter((item) => item.value)
	);
	let hasWindowInfo = $derived(windowItems.length > 0 || run.windowInfo.assetWindows.length > 0);
	let summaryItems = $derived([
		{ label: 'Status', value: run.status, status: true },
		{ label: 'Target', value: run.target, title: run.target, wide: true },
		{ label: 'Submit kind', value: run.submitKind ?? run.trigger },
		{ label: 'Target type', value: run.targetType },
		{ label: 'Started', value: run.startedAt ?? '—' },
		{ label: 'Finished', value: run.finishedAt ?? '—' },
		{ label: 'Duration', value: run.duration },
		{ label: 'Assets', value: run.assetCount },
		{
			label: 'Manifest version',
			value: compactManifestVersionId,
			title: run.manifestVersionId ?? undefined,
			mono: true
		},
		{
			label: 'Content hash',
			value: compactManifestContentHash,
			title: run.manifestContentHash ?? undefined,
			mono: true
		}
	]);

	const tabs = ['overview', 'events', 'raw'];
	const tabLabels: Record<string, string> = {
		overview: 'Overview',
		events: 'Events',
		raw: 'Raw'
	};

	function copy(text: string | null | undefined) {
		if (text) navigator.clipboard?.writeText(text);
	}

	function shortId(value: string | null | undefined) {
		if (!value) return '—';
		if (value.length <= 18) return value;
		return `${value.slice(0, 11)}…${value.slice(-4)}`;
	}

	function shortHash(value: string | null | undefined) {
		if (!value) return '—';
		const [prefix, hash] = value.includes(':') ? value.split(':', 2) : ['', value];
		const compact = hash.length <= 18 ? hash : `${hash.slice(0, 12)}…${hash.slice(-4)}`;
		return prefix ? `${prefix}:${compact}` : compact;
	}

	function tabClass(tab: string) {
		return activeTab === tab
			? 'inline-flex h-8 items-center justify-center rounded-md bg-slate-950 px-3 text-xs font-medium text-white shadow'
			: 'inline-flex h-8 items-center justify-center rounded-md px-3 text-xs font-medium text-slate-600 hover:bg-slate-100 hover:text-slate-950';
	}
</script>

<section class="space-y-6">
	<div class="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
		<div class="min-w-0">
			<div class="flex flex-wrap items-center gap-3">
				<h1 class="text-2xl font-semibold tracking-tight">Run details</h1>
				<StatusBadge status={run.status} />
			</div>
			<div class="mt-2 flex min-w-0 flex-wrap items-center gap-2 text-sm text-slate-600">
				<span class="font-mono text-xs" title={run.id}>{compactRunId}</span>
				<Button size="sm" variant="ghost" class="h-7 px-2" onclick={() => copy(run.id)}>Copy</Button
				>
			</div>
			<p class="mt-2 max-w-3xl truncate text-sm font-medium text-slate-700" title={run.target}>
				{run.target}
			</p>
		</div>
		<div class="flex shrink-0 flex-wrap gap-2 xl:justify-end">
			<Button size="sm" variant="outline" onclick={() => copy(rawJson)}>Copy raw JSON</Button>
			<Button size="sm" href={`/api/web/v1/streams/runs/${run.id}`} variant="outline"
				>Open event stream</Button
			>
		</div>
	</div>

	<div class="flex flex-wrap gap-2 border-b pb-2" aria-label="Run detail tabs">
		{#each tabs as tab (tab)}
			<button type="button" class={tabClass(tab)} onclick={() => (activeTab = tab)}>
				{tabLabels[tab]}
			</button>
		{/each}
	</div>

	{#if activeTab === 'overview'}
		<div class="space-y-6">
			<Card.Root>
				<Card.Header>
					<h2 class="text-lg font-semibold tracking-tight">Run summary</h2>
					<Card.Description>Projected orchestrator state for this run.</Card.Description>
				</Card.Header>
				<Card.Content>
					<dl class="grid grid-cols-1 gap-3 text-sm sm:grid-cols-2 xl:grid-cols-3">
						{#each summaryItems as item (item.label)}
							<div
								class={[
									'min-w-0 rounded-lg border bg-slate-50/70 p-3',
									item.wide && 'sm:col-span-2 xl:col-span-3'
								]}
							>
								<dt class="text-xs font-medium tracking-wide text-slate-500 uppercase">
									{item.label}
								</dt>
								<dd
									class={[
										'mt-1 min-w-0 text-slate-900',
										item.mono && 'font-mono text-xs',
										item.wide ? 'break-words' : 'truncate'
									]}
									title={item.title}
								>
									{#if item.status}
										<StatusBadge status={run.status} />
									{:else}
										{item.value}
									{/if}
								</dd>
							</div>
						{/each}
					</dl>
				</Card.Content>
			</Card.Root>

			{#if hasWindowInfo}
				<Card.Root>
					<Card.Header>
						<h2 class="text-lg font-semibold tracking-tight">Window context</h2>
						<Card.Description>
							Pipeline anchor and runtime windows reported by the orchestrator.
						</Card.Description>
					</Card.Header>
					<Card.Content class="space-y-4">
						{#if windowItems.length > 0}
							<dl class="grid grid-cols-1 gap-3 text-sm md:grid-cols-3">
								{#each windowItems as item (item.label)}
									<div class="min-w-0 rounded-lg border bg-slate-50/70 p-3">
										<dt class="text-xs font-medium tracking-wide text-slate-500 uppercase">
											{item.label}
										</dt>
										<dd class="mt-1 break-words text-slate-900">{item.value}</dd>
									</div>
								{/each}
							</dl>
						{/if}

						{#if run.windowInfo.assetWindows.length > 0}
							<div class="space-y-2">
								<div class="flex items-center gap-2">
									<h3 class="text-sm font-semibold text-slate-900">Asset/runtime windows</h3>
									<Badge variant="outline">{run.windowInfo.assetWindows.length}</Badge>
								</div>
								<div class="overflow-hidden rounded-lg border">
									<table class="w-full text-left text-sm">
										<thead class="bg-slate-50 text-xs tracking-wide text-slate-500 uppercase">
											<tr>
												<th class="px-3 py-2 font-medium">Asset</th>
												<th class="px-3 py-2 font-medium">Window</th>
											</tr>
										</thead>
										<tbody class="divide-y">
											{#each run.windowInfo.assetWindows as item (item.asset + item.window)}
												<tr>
													<td class="px-3 py-2 font-medium text-slate-900">{item.asset}</td>
													<td class="px-3 py-2 text-slate-700">{item.window}</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							</div>
						{/if}
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

			<Card.Root>
				<Card.Header>
					<h2 class="text-lg font-semibold tracking-tight">Execution</h2>
					<Card.Description>Asset-level rows when the orchestrator reported them.</Card.Description>
				</Card.Header>
				<Card.Content class="space-y-5">
					{#if run.assets.length > 0}
						<div class="text-sm">
							<AssetExecutionTable
								assets={run.assets}
								onselect={(asset) => (selectedAsset = asset)}
							/>
						</div>
					{:else}
						<div class="rounded-lg border border-dashed bg-slate-50 p-4 text-sm text-slate-600">
							No asset-level execution rows were reported. Showing run-level events instead.
						</div>
						<RunTimeline events={run.timeline} live={run.status === 'running'} />
					{/if}

					<div class="space-y-2">
						<div class="flex items-center justify-between gap-2">
							<h2 class="text-lg font-semibold">Outputs</h2>
							{#if run.outputs.length > 0}<Badge variant="outline">{run.outputs.length}</Badge>{/if}
						</div>
						{#if run.outputs.length === 0}
							<p class="rounded-lg border border-dashed bg-slate-50 p-4 text-sm text-slate-600">
								No materialized outputs reported by this run.
							</p>
						{:else}
							<div class="text-sm"><OutputRelationsTable outputs={run.outputs} /></div>
						{/if}
					</div>
				</Card.Content>
			</Card.Root>
		</div>
	{:else if activeTab === 'events'}
		<Card.Root>
			<Card.Header>
				<h2 class="text-lg font-semibold tracking-tight">Events</h2>
				<Card.Description>Run-level event stream fallback and orchestrator events.</Card.Description
				>
			</Card.Header>
			<Card.Content
				><RunTimeline events={run.timeline} live={run.status === 'running'} /></Card.Content
			>
		</Card.Root>
	{:else}
		<Card.Root>
			<Card.Header>
				<h2 class="text-lg font-semibold tracking-tight">Debug</h2>
				<Card.Description>Raw BFF/orchestrator payload for diagnostics.</Card.Description>
			</Card.Header>
			<Card.Content>
				<div class="mb-3 flex flex-wrap gap-2">
					<Button size="sm" variant="outline" onclick={() => copy(rawJson)}>Copy JSON</Button>
					<Button size="sm" variant="outline" href={`/api/web/v1/streams/runs/${run.id}`}>
						Open event stream
					</Button>
				</div>
				<pre
					class="max-h-[32rem] overflow-auto rounded-md bg-slate-950 p-4 text-xs text-slate-50">{rawJson}</pre>
			</Card.Content>
		</Card.Root>
	{/if}

	{#if selectedAsset}
		<AssetDetailSheet
			asset={selectedAsset}
			runId={run.id}
			events={run.timeline}
			onclose={() => (selectedAsset = null)}
		/>
	{/if}
</section>
