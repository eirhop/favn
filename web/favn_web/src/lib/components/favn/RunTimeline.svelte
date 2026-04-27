<script lang="ts">
	import { Badge } from '$lib/components/ui/badge';
	import type { TimelineEventView } from '$lib/run_view_types';

	let { events, live = false } = $props<{ events: TimelineEventView[]; live?: boolean }>();
</script>

<div>
	<div class="mb-4 flex items-center gap-2">
		<h3 class="text-lg font-semibold">Timeline</h3>
		{#if live}<Badge variant="secondary">Live</Badge>{/if}
	</div>
	<ol class="space-y-4">
		{#each events as event (event.id)}
			<li class="border-l-2 border-slate-200 pl-4">
				<p class="font-medium">{event.label}</p>
				<p class="text-xs text-slate-500">{event.timestamp ?? '—'}</p>
				<p class="text-sm text-slate-600">{event.detail}</p>
			</li>
		{/each}
	</ol>
</div>
