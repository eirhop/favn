<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import * as Alert from '$lib/components/ui/alert';
	import * as Card from '$lib/components/ui/card';
	import StatusBadge from './StatusBadge.svelte';
	import type { AssetExecutionView, TimelineEventView } from '$lib/run_view_types';

	let { asset, runId, events, onclose } = $props<{
		asset: AssetExecutionView;
		runId: string;
		events: TimelineEventView[];
		onclose?: () => void;
	}>();

	function copy(text: string | null | undefined) {
		if (text) navigator.clipboard?.writeText(text);
	}

	let assetEvents = $derived(
		events.filter(
			(event: TimelineEventView) =>
				!event.assetId || event.assetId === asset.id || event.detail.includes(asset.asset)
		)
	);
</script>

<div
	class="fixed inset-0 z-30 bg-slate-950/30"
	onclick={() => onclose?.()}
	aria-hidden="true"
></div>
<aside
	class="fixed top-0 right-0 z-40 h-screen w-full max-w-xl overflow-auto border-l bg-white p-6 shadow-xl"
	aria-label="Asset detail sheet"
>
	<div class="flex items-start justify-between gap-4">
		<div>
			<h2 class="text-xl font-semibold">{asset.asset}</h2>
			<p class="text-sm text-slate-500">{asset.type} Asset · {asset.stage}</p>
		</div>
		<Button variant="ghost" onclick={() => onclose?.()}>Close</Button>
	</div>
	<div class="mt-6 grid gap-4">
		<Card.Root>
			<Card.Header><Card.Title>Status</Card.Title></Card.Header>
			<Card.Content class="grid grid-cols-2 gap-2 text-sm">
				<span>Status</span><StatusBadge status={asset.status} />
				<span>Started</span><span>{asset.startedAt ?? '—'}</span>
				<span>Finished</span><span>{asset.finishedAt ?? '—'}</span>
				<span>Duration</span><span>{asset.duration}</span>
				<span>Attempt</span><span>{asset.attempt}</span>
			</Card.Content>
		</Card.Root>
		{#if asset.error}
			<Alert.Root variant="destructive" class="bg-red-50">
				<Alert.Title>Error</Alert.Title>
				<Alert.Description>{asset.error}</Alert.Description>
				<div class="mt-3 grid gap-1 text-sm">
					<p>Operation: {asset.operation ?? 'materialize table'}</p>
					<p>Connection: {asset.connection ?? 'local'}</p>
					<p>Relation: {asset.relation ?? '—'}</p>
				</div>
				<div class="mt-3 flex gap-2">
					<Button size="sm" variant="outline" onclick={() => copy(asset.error)}>Copy error</Button>
					<Button size="sm" variant="outline" onclick={() => copy(runId)}>Copy run id</Button>
				</div>
			</Alert.Root>
		{/if}
		<Card.Root>
			<Card.Header><Card.Title>SQL / Operation</Card.Title></Card.Header>
			<Card.Content>
				<pre
					class="max-h-72 overflow-auto rounded-md bg-slate-950 p-4 text-xs text-slate-50">{asset.sql ??
						asset.operation ??
						'Operation details unavailable.'}</pre>
				<Button
					class="mt-3"
					size="sm"
					variant="outline"
					onclick={() => copy(asset.sql ?? asset.operation)}
				>
					Copy SQL
				</Button>
			</Card.Content>
		</Card.Root>
		<Card.Root>
			<Card.Header><Card.Title>Materialization</Card.Title></Card.Header>
			<Card.Content class="grid grid-cols-2 gap-2 text-sm">
				<span>Type</span><span>{asset.outputs[0]?.type ?? 'table'}</span>
				<span>Relation</span><span class="font-mono">{asset.relation ?? '—'}</span>
				<span>Connection</span><span>{asset.connection ?? '—'}</span>
				<span>Database</span><span>{asset.database ?? '—'}</span>
				<span>Rows</span><span>{asset.outputs[0]?.rows ?? 'unavailable'}</span>
			</Card.Content>
		</Card.Root>
		<Card.Root>
			<Card.Header><Card.Title>Events</Card.Title></Card.Header>
			<Card.Content>
				<ol class="space-y-2 text-sm">
					{#each assetEvents as event (event.id)}
						<li><span class="text-slate-500">{event.timestamp ?? '—'}</span> {event.label}</li>
					{/each}
				</ol>
			</Card.Content>
		</Card.Root>
	</div>
</aside>
