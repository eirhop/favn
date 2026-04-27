<script lang="ts">
	import { resolve } from '$app/paths';
	import { Button } from '$lib/components/ui/button';
	import * as Table from '$lib/components/ui/table';
	import StatusBadge from './StatusBadge.svelte';
	import type { RunSummaryView } from '$lib/run_view_types';

	let { runs } = $props<{ runs: RunSummaryView[] }>();

	function shortId(value: string | null | undefined) {
		if (!value) return '—';
		if (value.length <= 18) return value;
		return `${value.slice(0, 11)}…${value.slice(-4)}`;
	}
</script>

<Table.Root class="table-fixed">
	<Table.Header>
		<Table.Row>
			<Table.Head class="w-28">Status</Table.Head>
			<Table.Head class="w-36">Run</Table.Head>
			<Table.Head>Target</Table.Head>
			<Table.Head class="w-24">Trigger</Table.Head>
			<Table.Head class="w-24">Started</Table.Head>
			<Table.Head class="w-28">Duration</Table.Head>
			<Table.Head class="w-20">Assets</Table.Head>
			<Table.Head class="w-36">Manifest</Table.Head>
			<Table.Head class="w-24 text-right">Actions</Table.Head>
		</Table.Row>
	</Table.Header>
	<Table.Body>
		{#each runs as run (run.id)}
			<Table.Row>
				<Table.Cell><StatusBadge status={run.status} /></Table.Cell>
				<Table.Cell class="truncate font-mono text-xs font-medium text-blue-700" title={run.id}
					><a href={resolve(`/runs/${run.id}`)}>{shortId(run.id)}</a></Table.Cell
				>
				<Table.Cell class="min-w-0">
					<div class="flex min-w-0 items-center gap-2">
						<span
							class="shrink-0 rounded-md border bg-slate-50 px-1.5 py-0.5 text-[10px] font-medium text-slate-600"
							>{run.targetType}</span
						>
						<span class="truncate" title={run.target}>{run.target}</span>
					</div>
				</Table.Cell>
				<Table.Cell>{run.trigger}</Table.Cell>
				<Table.Cell>{run.startedAt ?? '—'}</Table.Cell>
				<Table.Cell>{run.duration}</Table.Cell>
				<Table.Cell>{run.assetCount}</Table.Cell>
				<Table.Cell
					class="truncate font-mono text-xs"
					title={run.manifestVersionId ?? 'No manifest version'}
					>{shortId(run.manifestVersionId)}</Table.Cell
				>
				<Table.Cell class="text-right">
					<div class="flex items-center justify-end gap-1">
						<Button href={`/runs/${run.id}`} variant="ghost" size="sm">Inspect</Button>
						<details class="relative hidden xl:block">
							<summary class="cursor-pointer rounded-md border px-2 py-1 text-xs">More</summary>
							<div
								class="absolute right-0 z-10 mt-1 w-36 rounded-md border bg-white p-1 text-xs shadow-lg"
							>
								<button
									class="block w-full rounded px-2 py-1 text-left hover:bg-slate-100"
									onclick={() => navigator.clipboard?.writeText(run.id)}>Copy run id</button
								>
								<a
									class="block rounded px-2 py-1 hover:bg-slate-100"
									href={resolve(`/api/web/v1/streams/runs/${run.id}`)}>Open stream</a
								>
							</div>
						</details>
					</div>
				</Table.Cell>
			</Table.Row>
		{/each}
	</Table.Body>
</Table.Root>
