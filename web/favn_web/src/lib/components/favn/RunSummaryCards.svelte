<script lang="ts">
	import * as Card from '$lib/components/ui/card';
	import StatusBadge from './StatusBadge.svelte';
	import type { RunDetailView } from '$lib/run_view_types';

	let { run } = $props<{ run: RunDetailView }>();
</script>

<div class="grid gap-4 md:grid-cols-4">
	<Card.Root>
		<Card.Header>
			<Card.Description>Status</Card.Description>
			<Card.Title><StatusBadge status={run.status} /></Card.Title>
			<Card.Description>{run.status === 'failed' ? 'Run failed' : 'Latest state'}</Card.Description>
		</Card.Header>
	</Card.Root>
	<Card.Root>
		<Card.Header>
			<Card.Description>Duration</Card.Description>
			<Card.Title>{run.duration}</Card.Title>
			<Card.Description>Started {run.startedAt ?? '—'}</Card.Description>
		</Card.Header>
	</Card.Root>
	<Card.Root>
		<Card.Header>
			<Card.Description>Assets</Card.Description>
			<Card.Title
				>{run.assetCounts.succeeded} succeeded · {run.assetCounts.failed} failed</Card.Title
			>
			<Card.Description
				>{run.assetCounts.skipped} skipped · {run.assetCounts.running} running</Card.Description
			>
		</Card.Header>
	</Card.Root>
	<Card.Root>
		<Card.Header>
			<Card.Description>Outputs</Card.Description>
			<Card.Title>{run.outputs.length}</Card.Title>
			<Card.Description>Materialized relations</Card.Description>
		</Card.Header>
	</Card.Root>
</div>
