<script lang="ts">
	import { resolve } from '$app/paths';
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';
	import * as Alert from '$lib/components/ui/alert';
	import * as Card from '$lib/components/ui/card';
	import { Input } from '$lib/components/ui/input';
	import RunsTable from './RunsTable.svelte';
	import type { RunSummaryView } from '$lib/run_view_types';
	import {
		buildPipelineRunPayload,
		extractSubmittedRun,
		type PipelineTargetView,
		type WindowKind
	} from '$lib/pipeline_run_submission';

	let {
		runs,
		loadError = null,
		pipelineTargets = []
	} = $props<{
		runs: RunSummaryView[];
		loadError?: string | null;
		pipelineTargets?: PipelineTargetView[];
	}>();

	let query = $state('');
	let activeTab = $state('all');
	let selectedPipelineId = $state('');
	let windowValue = $state('');
	let timezone = $state('');
	let fullLoad = $state(false);
	let isSubmitting = $state(false);
	let submissionError = $state<string | null>(null);
	let submittedRun = $state<{ id: string | null; status: string | null } | null>(null);
	const tabs = ['all', 'running', 'failed', 'succeeded', 'cancelled'];
	const tabLabels: Record<string, string> = {
		all: 'All',
		running: 'Running',
		failed: 'Failed',
		succeeded: 'Succeeded',
		cancelled: 'Cancelled'
	};
	let liveUpdates = $state(true);
	let selectedPipeline = $derived(
		pipelineTargets.find(
			(pipeline: PipelineTargetView) => pipeline.targetId === selectedPipelineId
		) ??
			pipelineTargets[0] ??
			null
	);
	let selectedWindowPolicy = $derived(selectedPipeline?.windowPolicy ?? null);

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

	function selectPipeline(event: Event) {
		selectedPipelineId = (event.currentTarget as HTMLSelectElement).value;
		const pipeline =
			pipelineTargets.find(
				(target: PipelineTargetView) => target.targetId === selectedPipelineId
			) ?? null;
		windowValue = '';
		timezone = pipeline?.windowPolicy?.timezone ?? '';
		fullLoad = false;
		submissionError = null;
		submittedRun = null;
	}

	function updateWindowValue(event: Event) {
		windowValue = (event.currentTarget as HTMLInputElement).value;
	}

	function updateTimezone(event: Event) {
		timezone = (event.currentTarget as HTMLInputElement).value;
	}

	function windowLabel(kind: WindowKind): string {
		return kind === 'hour'
			? 'Hour window'
			: kind === 'day'
				? 'Day window'
				: kind === 'month'
					? 'Month window'
					: 'Year window';
	}

	function windowHelp(kind: WindowKind): string {
		return kind === 'hour'
			? 'Use YYYY-MM-DDTHH, for example 2026-04-27T10.'
			: kind === 'day'
				? 'Choose the calendar day to process.'
				: kind === 'month'
					? 'Choose the calendar month to process.'
					: 'Enter the four-digit year to process.';
	}

	function policySummary(policy: NonNullable<PipelineTargetView['windowPolicy']>): string {
		return [
			policy.kind,
			policy.anchor ?? 'anchor not reported',
			policy.timezone ?? 'timezone optional',
			policy.allowFullLoad ? 'full load allowed' : 'full load blocked'
		].join(' · ');
	}

	function responseMessage(payload: unknown, fallback: string): string {
		if (typeof payload === 'object' && payload !== null && 'error' in payload) {
			const error = (payload as { error?: { message?: unknown } }).error;
			if (typeof error?.message === 'string' && error.message.length > 0) return error.message;
		}
		return fallback;
	}

	async function submitPipelineRun() {
		submissionError = null;
		submittedRun = null;

		const result = buildPipelineRunPayload({
			pipeline: selectedPipeline,
			windowValue,
			timezone: timezone || selectedWindowPolicy?.timezone || '',
			fullLoad
		});

		if (!result.ok) {
			submissionError = result.error;
			return;
		}

		isSubmitting = true;
		try {
			const response = await fetch('/api/web/v1/runs', {
				method: 'POST',
				headers: { 'content-type': 'application/json', accept: 'application/json' },
				body: JSON.stringify(result.payload)
			});
			const payload = await response.json().catch(() => null);

			if (!response.ok) {
				submissionError = responseMessage(
					payload,
					`Run submission failed with HTTP ${response.status}.`
				);
				return;
			}

			submittedRun = extractSubmittedRun(payload);
		} catch {
			submissionError = 'Run submission failed because the web API was unreachable.';
		} finally {
			isSubmitting = false;
		}
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
			<Button href={resolve('/runs')} variant="outline">Refresh</Button>
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
			<p class="mb-1 font-medium tracking-tight">Failed to load runs</p>
			<Alert.Description>{loadError}</Alert.Description>
		</Alert.Root>
	{/if}

	<Card.Root class="border-slate-200 bg-slate-50/60">
		<Card.Header>
			<div class="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
				<div>
					<Card.Title>Submit pipeline run</Card.Title>
					<Card.Description>
						Run a pipeline from the active manifest. Windowed pipelines require one explicit window.
					</Card.Description>
				</div>
				<Badge variant="secondary">Manual</Badge>
			</div>
		</Card.Header>
		<Card.Content class="space-y-4">
			{#if pipelineTargets.length === 0}
				<div class="rounded-lg border border-dashed bg-white p-4 text-sm text-slate-600">
					No pipeline targets were reported by the active manifest.
				</div>
			{:else}
				<div class="grid gap-4 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)]">
					<div class="space-y-2">
						<label class="text-sm font-medium" for="pipeline-target">Pipeline</label>
						<select
							id="pipeline-target"
							class="border-input bg-background focus-visible:ring-ring flex h-10 w-full rounded-md border px-3 py-2 text-sm shadow-sm focus-visible:ring-1 focus-visible:outline-none"
							value={selectedPipeline?.targetId ?? ''}
							onchange={selectPipeline}
						>
							{#each pipelineTargets as pipeline (pipeline.targetId)}
								<option value={pipeline.targetId}>{pipeline.label}</option>
							{/each}
						</select>
						{#if selectedPipeline}
							<code class="block rounded-md bg-white px-2 py-1 text-xs break-all text-slate-600">
								{selectedPipeline.targetId}
							</code>
						{/if}
					</div>

					{#if selectedWindowPolicy}
						<div class="space-y-2 rounded-lg border bg-white p-3">
							<div class="flex flex-wrap items-center gap-2">
								<span class="text-sm font-medium">Window policy</span>
								<Badge variant="outline">{selectedWindowPolicy.kind}</Badge>
							</div>
							<p class="text-xs text-slate-600">{policySummary(selectedWindowPolicy)}</p>
						</div>
					{:else}
						<div class="rounded-lg border border-dashed bg-white p-3 text-sm text-slate-600">
							This pipeline has no window policy; the run will be submitted without a window.
						</div>
					{/if}
				</div>

				{#if selectedWindowPolicy}
					<div class="grid gap-4 md:grid-cols-2">
						<div class="space-y-2">
							<label class="text-sm font-medium" for="pipeline-window-value">
								{windowLabel(selectedWindowPolicy.kind)}
							</label>
							{#if selectedWindowPolicy.kind === 'day'}
								<Input
									id="pipeline-window-value"
									type="date"
									value={windowValue}
									oninput={updateWindowValue}
									disabled={fullLoad}
								/>
							{:else if selectedWindowPolicy.kind === 'month'}
								<Input
									id="pipeline-window-value"
									type="month"
									value={windowValue}
									oninput={updateWindowValue}
									disabled={fullLoad}
								/>
							{:else if selectedWindowPolicy.kind === 'year'}
								<Input
									id="pipeline-window-value"
									type="text"
									inputmode="numeric"
									pattern="[0-9]{4}"
									placeholder="2026"
									value={windowValue}
									oninput={updateWindowValue}
									disabled={fullLoad}
								/>
							{:else}
								<Input
									id="pipeline-window-value"
									placeholder="2026-04-27T10"
									value={windowValue}
									oninput={updateWindowValue}
									disabled={fullLoad}
								/>
							{/if}
							<p class="text-xs text-slate-500">{windowHelp(selectedWindowPolicy.kind)}</p>
						</div>
						<div class="space-y-2">
							<label class="text-sm font-medium" for="pipeline-window-timezone">Timezone</label>
							<Input
								id="pipeline-window-timezone"
								placeholder={selectedWindowPolicy.timezone ?? 'Etc/UTC'}
								value={timezone}
								oninput={updateTimezone}
								disabled={fullLoad}
							/>
							<p class="text-xs text-slate-500">Defaults from policy when available.</p>
						</div>
					</div>

					{#if selectedWindowPolicy.allowFullLoad}
						<label class="flex items-start gap-2 rounded-lg border bg-white p-3 text-sm">
							<input type="checkbox" class="mt-1" bind:checked={fullLoad} />
							<span>
								<span class="font-medium">Submit explicit full load instead</span>
								<span class="block text-slate-600">
									This omits the window and is only shown because the policy allows it.
								</span>
							</span>
						</label>
					{/if}
				{/if}

				{#if submissionError}
					<Alert.Root variant="destructive">
						<p class="mb-1 font-medium tracking-tight">Run submission failed</p>
						<Alert.Description>{submissionError}</Alert.Description>
					</Alert.Root>
				{/if}

				{#if submittedRun}
					<Alert.Root>
						<p class="mb-1 font-medium tracking-tight">Pipeline run submitted</p>
						<Alert.Description>
							{#if submittedRun.id}
								Run <a class="font-medium underline" href={resolve(`/runs/${submittedRun.id}`)}
									>{submittedRun.id}</a
								>
								{submittedRun.status ? `is ${submittedRun.status}` : 'was accepted'}.
							{:else}
								The run was accepted, but the response did not include a run id.
							{/if}
						</Alert.Description>
					</Alert.Root>
				{/if}

				<div class="flex flex-wrap gap-2">
					<Button onclick={submitPipelineRun} disabled={isSubmitting || !selectedPipeline}>
						{isSubmitting ? 'Submitting…' : 'Submit pipeline run'}
					</Button>
					<Button href={resolve('/runs')} variant="ghost">Refresh runs</Button>
				</div>
			{/if}
		</Card.Content>
	</Card.Root>

	<Card.Root>
		<Card.Header>
			<div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
				<div class="flex flex-wrap gap-2" aria-label="Run status filters">
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
