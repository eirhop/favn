<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { resolve } from '$app/paths';
	import { Input } from '$lib/components/ui/input';
	import * as Card from '$lib/components/ui/card';
	import { Badge } from '$lib/components/ui/badge';
	import { buildBackfillSubmitPayload, extractSubmittedBackfill } from '$lib/backfill_submission';
	import type { CoverageBaselineView } from '$lib/backfill_view_types';
	import type { PipelineTargetView, WindowKind } from '$lib/pipeline_run_submission';

	let { pipelineTargets = [], coverageBaselines = [] } = $props<{
		pipelineTargets?: PipelineTargetView[];
		coverageBaselines?: CoverageBaselineView[];
	}>();

	let selectedPipelineId = $state('');
	let from = $state('');
	let to = $state('');
	let kind = $state<WindowKind | ''>('');
	let timezone = $state('');
	let coverageBaselineId = $state('');
	let isSubmitting = $state(false);
	let submissionError = $state<string | null>(null);
	let submittedRun = $state<{ id: string | null; status: string | null } | null>(null);

	let selectedPipeline = $derived(
		pipelineTargets.find(
			(pipeline: PipelineTargetView) => pipeline.targetId === selectedPipelineId
		) ??
			pipelineTargets[0] ??
			null
	);
	let effectiveKind = $derived<WindowKind>(kind || selectedPipeline?.windowPolicy?.kind || 'day');
	let effectiveTimezone = $derived(
		timezone || selectedPipeline?.windowPolicy?.timezone || 'Etc/UTC'
	);

	function updatePipeline(event: Event) {
		selectedPipelineId = (event.currentTarget as HTMLSelectElement).value;
		const pipeline = pipelineTargets.find(
			(target: PipelineTargetView) => target.targetId === selectedPipelineId
		);
		kind = pipeline?.windowPolicy?.kind ?? 'day';
		timezone = pipeline?.windowPolicy?.timezone ?? 'Etc/UTC';
		submissionError = null;
		submittedRun = null;
	}

	function updateFrom(event: Event) {
		from = (event.currentTarget as HTMLInputElement).value;
	}

	function updateTo(event: Event) {
		to = (event.currentTarget as HTMLInputElement).value;
	}

	function updateKind(event: Event) {
		kind = (event.currentTarget as HTMLSelectElement).value as WindowKind;
	}

	function updateTimezone(event: Event) {
		timezone = (event.currentTarget as HTMLInputElement).value;
	}

	function responseMessage(payload: unknown, fallback: string): string {
		if (typeof payload === 'object' && payload !== null && 'error' in payload) {
			const error = (payload as { error?: { message?: unknown } }).error;
			if (typeof error?.message === 'string' && error.message.length > 0) return error.message;
		}
		return fallback;
	}

	async function submitBackfill() {
		submissionError = null;
		submittedRun = null;

		const result = buildBackfillSubmitPayload({
			pipeline: selectedPipeline,
			from,
			to,
			kind: effectiveKind,
			timezone: effectiveTimezone,
			coverageBaselineId
		});

		if (!result.ok) {
			submissionError = result.error;
			return;
		}

		isSubmitting = true;
		try {
			const response = await fetch('/api/web/v1/backfills', {
				method: 'POST',
				headers: { accept: 'application/json', 'content-type': 'application/json' },
				body: JSON.stringify(result.payload)
			});
			const payload = await response.json().catch(() => null);
			if (!response.ok) {
				submissionError = responseMessage(
					payload,
					`Backfill submission failed with HTTP ${response.status}.`
				);
				return;
			}
			submittedRun = extractSubmittedBackfill(payload);
		} catch {
			submissionError = 'Backfill submission failed because the web API was unreachable.';
		} finally {
			isSubmitting = false;
		}
	}

	function rangeHelp(value: WindowKind): string {
		return value === 'hour'
			? 'Use values accepted by the backend, for example 2026-04-01T10.'
			: value === 'day'
				? 'Use dates such as 2026-04-01 through 2026-04-07.'
				: value === 'month'
					? 'Use months such as 2026-04 through 2026-06.'
					: 'Use years such as 2025 through 2026.';
	}
</script>

<Card.Root class="border-slate-200 bg-slate-50/60">
	<Card.Header>
		<div class="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
			<div>
				<Card.Title>Submit operational backfill</Card.Title>
				<Card.Description>
					Plan child windows for an active-manifest pipeline over an explicit range.
				</Card.Description>
			</div>
			<Badge variant="secondary">Operator</Badge>
		</div>
	</Card.Header>
	<Card.Content class="space-y-4">
		{#if pipelineTargets.length === 0}
			<div class="rounded-lg border border-dashed bg-white p-4 text-sm text-slate-600">
				No active-manifest pipeline targets are available. Start or register a manifest before
				backfilling.
			</div>
		{:else}
			<div class="grid gap-4 md:grid-cols-2">
				<label class="space-y-1 text-sm font-medium">
					<span>Pipeline</span>
					<select
						class="h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm"
						value={selectedPipeline?.targetId ?? ''}
						onchange={updatePipeline}
					>
						{#each pipelineTargets as pipeline (pipeline.targetId)}
							<option value={pipeline.targetId}>{pipeline.label}</option>
						{/each}
					</select>
				</label>
				<label class="space-y-1 text-sm font-medium">
					<span>Kind</span>
					<select
						class="h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm"
						value={effectiveKind}
						onchange={updateKind}
					>
						<option value="hour">hour</option>
						<option value="day">day</option>
						<option value="month">month</option>
						<option value="year">year</option>
					</select>
				</label>
				<label class="space-y-1 text-sm font-medium">
					<span>From</span>
					<Input value={from} oninput={updateFrom} placeholder="2026-04-01" />
				</label>
				<label class="space-y-1 text-sm font-medium">
					<span>To</span>
					<Input value={to} oninput={updateTo} placeholder="2026-04-07" />
				</label>
				<label class="space-y-1 text-sm font-medium">
					<span>Timezone</span>
					<Input value={effectiveTimezone} oninput={updateTimezone} placeholder="Etc/UTC" />
				</label>
				<label class="space-y-1 text-sm font-medium">
					<span>Coverage baseline (optional)</span>
					<select
						class="h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm disabled:bg-slate-100"
						bind:value={coverageBaselineId}
						disabled={coverageBaselines.length === 0}
					>
						<option value="">No baseline</option>
						{#each coverageBaselines as baseline (baseline.baselineId)}
							<option value={baseline.baselineId}>{baseline.baselineId}</option>
						{/each}
					</select>
				</label>
			</div>
			<p class="text-xs text-slate-500">{rangeHelp(effectiveKind)}</p>
			{#if coverageBaselines.length === 0}
				<p class="text-xs text-slate-500">
					No coverage baselines are available yet; explicit range submission is still allowed.
				</p>
			{/if}
			<div class="flex flex-col gap-3 md:flex-row md:items-center">
				<Button type="button" disabled={isSubmitting} onclick={submitBackfill}>
					{isSubmitting ? 'Submitting…' : 'Submit backfill'}
				</Button>
				{#if submissionError}
					<p class="text-sm text-red-700">{submissionError}</p>
				{/if}
				{#if submittedRun}
					<p class="text-sm text-emerald-700">
						Accepted {submittedRun.status ?? 'backfill'} ·
						{#if submittedRun.id}<a
								class="underline"
								href={resolve(`/backfills/${submittedRun.id}`)}>{submittedRun.id}</a
							>{/if}
					</p>
				{/if}
			</div>
		{/if}
	</Card.Content>
</Card.Root>
