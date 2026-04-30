<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import * as Card from '$lib/components/ui/card';
	import BackfillSubmitForm from './BackfillSubmitForm.svelte';
	import type { CoverageBaselineView } from '$lib/backfill_view_types';
	import type { PipelineTargetView } from '$lib/pipeline_run_submission';

	let {
		pipelineTargets = [],
		coverageBaselines = [],
		loadError = null
	} = $props<{
		pipelineTargets?: PipelineTargetView[];
		coverageBaselines?: CoverageBaselineView[];
		loadError?: string | null;
	}>();
</script>

<section class="space-y-6">
	<div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
		<div>
			<h1 class="text-3xl font-semibold tracking-tight">Backfills</h1>
			<p class="mt-1 text-sm text-slate-600">
				Submit operational range backfills and inspect window-level progress.
			</p>
		</div>
		<div class="flex gap-2">
			<Button href="/backfills/coverage-baselines" variant="outline">Coverage baselines</Button>
			<Button href="/assets/window-states" variant="outline">Asset window states</Button>
		</div>
	</div>

	{#if loadError}<p class="text-sm text-red-700">
			Failed to load backfill context: {loadError}
		</p>{/if}

	<h2 class="sr-only">Submit backfill command</h2>
	<BackfillSubmitForm {pipelineTargets} {coverageBaselines} />

	<h2 class="sr-only">Backfill operator notes</h2>
	<Card.Root>
		<Card.Header>
			<Card.Title>Operator notes</Card.Title>
			<Card.Description>Backfills always use the active manifest in this UI.</Card.Description>
		</Card.Header>
		<Card.Content class="space-y-2 text-sm text-slate-600">
			<p>
				Use an explicit from/to range. Lookback and idempotency controls are intentionally absent.
			</p>
			<p>After submission, follow the accepted parent run id to inspect child windows.</p>
		</Card.Content>
	</Card.Root>
</section>
