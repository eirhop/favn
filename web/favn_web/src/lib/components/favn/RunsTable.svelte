<script lang="ts">
	import { resolve } from '$app/paths';
	import { Button } from '$lib/components/ui/button';
	import * as Table from '$lib/components/ui/table';
	import StatusBadge from './StatusBadge.svelte';
	import type { RunSummaryView } from '$lib/run_view_types';

	let { runs } = $props<{ runs: RunSummaryView[] }>();
</script>

<Table.Root>
	<Table.Header>
		<Table.Row>
			<Table.Head>Status</Table.Head>
			<Table.Head>Run</Table.Head>
			<Table.Head>Target</Table.Head>
			<Table.Head>Trigger</Table.Head>
			<Table.Head>Started</Table.Head>
			<Table.Head>Duration</Table.Head>
			<Table.Head>Assets</Table.Head>
			<Table.Head>Manifest</Table.Head>
			<Table.Head>Actions</Table.Head>
		</Table.Row>
	</Table.Header>
	<Table.Body>
		{#each runs as run (run.id)}
			<Table.Row>
				<Table.Cell><StatusBadge status={run.status} /></Table.Cell>
				<Table.Cell class="font-mono text-xs font-medium text-blue-700"
					><a href={resolve(`/runs/${run.id}`)}>{run.id}</a></Table.Cell
				>
				<Table.Cell>{run.target}</Table.Cell>
				<Table.Cell>{run.trigger}</Table.Cell>
				<Table.Cell>{run.startedAt ?? '—'}</Table.Cell>
				<Table.Cell>{run.duration}</Table.Cell>
				<Table.Cell>{run.assetCount}</Table.Cell>
				<Table.Cell class="font-mono text-xs">{run.manifestVersionId ?? '—'}</Table.Cell>
				<Table.Cell>
					<div class="flex items-center gap-2">
						<Button href={`/runs/${run.id}`} variant="ghost" size="sm">Inspect</Button>
						<details class="relative">
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
