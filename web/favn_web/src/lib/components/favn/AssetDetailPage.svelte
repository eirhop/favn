<script lang="ts">
	import { resolve } from '$app/paths';
	import { Button } from '$lib/components/ui/button';
	import * as Card from '$lib/components/ui/card';
	import StatusBadge from './StatusBadge.svelte';
	import type {
		AssetDetailPageData,
		AssetDetailView,
		AssetMaterializationSummary,
		AssetRuntimeConfigEntry
	} from '$lib/asset_catalog_types';

	type RunScope = 'asset' | 'with_dependencies';
	type Tab = 'overview' | 'lineage' | 'runs' | 'raw';
	type RunLike = Record<string, unknown>;
	type ItemLike = string | Record<string, unknown>;
	type InspectionPayload = Record<string, unknown>;

	let {
		data,
		asset: assetProp,
		onrun
	} = $props<{
		data?: AssetDetailPageData;
		asset?: AssetDetailView;
		onrun?: (detail: {
			scope: RunScope;
			assetRef: string;
			manifestVersionId: string | null;
		}) => void;
	}>();

	let activeTab: Tab = $state('overview');
	let pendingScope: RunScope | null = $state(null);
	let copied = $state<string | null>(null);
	let inspection = $state.raw<InspectionPayload | null>(null);
	let inspectionLoading = $state(false);
	let inspectionError = $state<string | null>(null);

	let pageData = $derived((data ?? {}) as Record<string, unknown>);
	let detail = $derived((pageData.detail ?? null) as Record<string, unknown> | null);
	let asset = $derived(
		(assetProp ??
			pageData.asset ??
			(detail?.asset as Record<string, unknown> | undefined) ??
			pageData.view ??
			pageData.assetDetail ??
			{}) as Record<string, unknown>
	);
	let rawPayload = $derived(asset.raw ?? pageData.raw ?? asset);
	let rawJson = $derived(JSON.stringify(rawPayload, null, 2));
	let runActions = $derived(
		(pageData.runActions ?? pageData.actions ?? {}) as Record<string, unknown>
	);
	let lineage = $derived(
		(asset.lineage ?? pageData.lineage ?? detail ?? {}) as Record<string, unknown>
	);
	let capabilities = $derived(
		(asset.capabilities ?? pageData.capabilities ?? {}) as Record<string, unknown>
	);
	let lastRun = $derived((asset.lastRun ?? pageData.lastRun ?? null) as RunLike | null);
	let recentRuns = $derived(
		(asset.recentRuns ?? pageData.recentRuns ?? pageData.runs ?? []) as RunLike[]
	);
	let schemaColumns = $derived.by(() => {
		const schema = (asset.schema ?? asset.schemaMetadata ?? pageData.schema) as Record<
			string,
			unknown
		> | null;
		const columns = schema?.columns ?? schema?.fields;
		return Array.isArray(columns) ? (columns as Array<Record<string, unknown>>) : [];
	});
	let runtimeConfig = $derived.by(() => {
		const value = asset.runtimeConfig ?? asset.runtime_config ?? [];
		return Array.isArray(value) ? (value as AssetRuntimeConfigEntry[]) : [];
	});
	let latestMaterialization = $derived(
		(asset.latestMaterialization ??
			asset.latest_materialization ??
			null) as AssetMaterializationSummary | null
	);
	let inspectionColumns = $derived.by(() => {
		const schema = (inspection?.schema ?? inspection?.schema_metadata ?? inspection) as Record<
			string,
			unknown
		> | null;
		const columns = schema?.columns ?? schema?.fields;
		return Array.isArray(columns) ? (columns as Array<Record<string, unknown>>) : [];
	});
	let inspectionRows = $derived.by(() => {
		const rows =
			inspection?.sample_rows ?? inspection?.sampleRows ?? inspection?.rows ?? inspection?.samples;
		return Array.isArray(rows) ? rows.slice(0, 20) : [];
	});
	let inspectionRowKeys = $derived.by(() => {
		const keys: string[] = [];
		for (const row of inspectionRows) {
			if (row && typeof row === 'object' && !Array.isArray(row)) {
				for (const key of Object.keys(row)) {
					if (!keys.includes(key)) keys.push(key);
				}
			}
		}
		return keys.slice(0, 20);
	});
	let inspectionWarnings = $derived.by(() => {
		const warnings = inspection?.warnings ?? inspection?.diagnostics;
		return Array.isArray(warnings) ? warnings.map((warning) => String(warning)) : [];
	});
	let inspectionErrors = $derived.by(() => {
		const errors = inspection?.errors;
		return Array.isArray(errors) ? errors.map((entry) => String(entry)) : [];
	});
	let notes = $derived.by(() => {
		const source =
			capabilities.notes ??
			asset.capabilityNotes ??
			detail?.capabilityNotes ??
			asset.metadataCapabilityNotes;
		return Array.isArray(source)
			? source.map((note) => {
					if (typeof note === 'string') return note;
					if (note && typeof note === 'object' && 'message' in note) return String(note.message);
					return String(note);
				})
			: [];
	});

	let assetRef = $derived(
		String(
			asset.ref ??
				asset.assetRef ??
				asset.asset_ref ??
				asset.id ??
				asset.targetId ??
				'unknown asset'
		)
	);
	let targetId = $derived(String(asset.targetId ?? asset.target_id ?? asset.target ?? assetRef));
	let friendlyName = $derived(
		String(
			asset.name ?? asset.displayName ?? asset.title ?? assetRef.split(/[.:]/).at(-1) ?? assetRef
		)
	);
	let health = $derived(
		String(asset.health ?? asset.status ?? lastRun?.status ?? 'unknown').toLowerCase()
	);
	let manifestVersionId = $derived(
		(asset.manifestVersionId ??
			asset.manifest_version_id ??
			pageData.activeManifestVersionId ??
			pageData.manifestVersionId ??
			null) as string | null
	);
	let assetOnlyAvailable = $derived(
		capabilities.assetOnlyScopeAvailable ?? capabilities.asset_only_scope_available ?? false
	);
	let dependenciesAvailable = $derived(
		capabilities.dependenciesAvailable ?? lineage.dependenciesAvailable ?? true
	);
	let withDependenciesAction = $derived(
		(runActions.withDependencies ??
			runActions.with_dependencies ??
			runActions.runWithDependencies ??
			null) as string | null
	);
	let assetAction = $derived(
		(runActions.asset ?? runActions.assetOnly ?? runActions.runAsset ?? null) as string | null
	);
	let canSubmitAsset = $derived(Boolean(assetOnlyAvailable && (assetAction || onrun)));
	let canSubmitWithDependencies = $derived(Boolean(withDependenciesAction || onrun));
	let upstream = $derived(
		toItems(lineage.upstream ?? lineage.dependencies ?? asset.upstream ?? [])
	);
	let downstream = $derived(
		toItems(lineage.downstream ?? lineage.dependents ?? asset.downstream ?? [])
	);
	let currentItem = $derived({ label: friendlyName, detail: assetRef });

	const tabs: Array<{ id: Tab; label: string }> = [
		{ id: 'overview', label: 'Overview' },
		{ id: 'lineage', label: 'Lineage' },
		{ id: 'runs', label: 'Runs' },
		{ id: 'raw', label: 'Raw' }
	];

	function toItems(value: unknown): Array<{ label: string; detail: string | null }> {
		if (!Array.isArray(value)) return [];
		return value.map((item: ItemLike) => {
			if (typeof item === 'string') return { label: item, detail: null };
			return {
				label: String(item.name ?? item.ref ?? item.id ?? item.asset ?? 'unknown'),
				detail: item.detail
					? String(item.detail)
					: item.ref || item.id
						? String(item.ref ?? item.id)
						: null
			};
		});
	}

	function value(...candidates: unknown[]) {
		const found = candidates.find(
			(candidate) => candidate !== null && candidate !== undefined && candidate !== ''
		);
		return found === undefined ? '—' : String(found);
	}

	function jsonValue(candidate: unknown): string {
		if (candidate === null || candidate === undefined || candidate === '') return '—';
		if (typeof candidate === 'string') return candidate;
		if (typeof candidate === 'number' || typeof candidate === 'boolean') return String(candidate);
		return JSON.stringify(candidate);
	}

	function rowValue(row: unknown, key: string): string {
		if (!row || typeof row !== 'object' || Array.isArray(row)) return jsonValue(row);
		return jsonValue((row as Record<string, unknown>)[key]);
	}

	function inspectionFromPayload(payload: unknown): InspectionPayload | null {
		if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null;
		const record = payload as Record<string, unknown>;
		const data = record.data;
		if (data && typeof data === 'object' && !Array.isArray(data) && 'inspection' in data) {
			const inspectionPayload = (data as Record<string, unknown>).inspection;
			return inspectionPayload &&
				typeof inspectionPayload === 'object' &&
				!Array.isArray(inspectionPayload)
				? (inspectionPayload as InspectionPayload)
				: null;
		}
		const direct = record.inspection;
		if (direct && typeof direct === 'object' && !Array.isArray(direct)) {
			return direct as InspectionPayload;
		}
		return record;
	}

	async function loadInspection() {
		if (!manifestVersionId || !targetId) return;

		inspectionLoading = true;
		inspectionError = null;

		try {
			const response = await fetch(
				resolve(
					`/api/web/v1/manifests/${encodeURIComponent(manifestVersionId)}/assets/${encodeURIComponent(
						targetId
					)}/inspection?limit=20`
				),
				{ headers: { accept: 'application/json' } }
			);
			const payload = (await response.json()) as unknown;
			if (!response.ok) {
				const message =
					payload && typeof payload === 'object' && 'error' in payload
						? jsonValue((payload as Record<string, unknown>).error)
						: 'Inspection request failed';
				throw new Error(message);
			}
			inspection = inspectionFromPayload(payload);
		} catch (error) {
			inspection = null;
			inspectionError = error instanceof Error ? error.message : 'Inspection request failed';
		} finally {
			inspectionLoading = false;
		}
	}

	function runtimeStatusClass(status: string) {
		if (status === 'declared') return 'bg-blue-50 text-blue-700 ring-blue-200';
		return 'bg-slate-50 text-slate-700 ring-slate-200';
	}

	function tabClass(tab: Tab) {
		return activeTab === tab
			? 'inline-flex h-8 items-center justify-center rounded-md bg-slate-950 px-3 text-xs font-medium text-white shadow'
			: 'inline-flex h-8 items-center justify-center rounded-md px-3 text-xs font-medium hover:bg-slate-100';
	}

	function tabId(tab: Tab) {
		return `asset-detail-${tab}-tab`;
	}

	function tabPanelId(tab: Tab) {
		return `asset-detail-${tab}-panel`;
	}

	function copy(text: string, label: string) {
		navigator.clipboard?.writeText(text);
		copied = label;
	}

	function openConfirmation(scope: RunScope) {
		pendingScope = scope;
	}

	function confirmCallbackRun() {
		if (!pendingScope || !onrun) return;
		onrun({ scope: pendingScope, assetRef, manifestVersionId });
		pendingScope = null;
	}
</script>

<section class="space-y-6">
	<div class="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
		<div class="min-w-0">
			<nav class="text-sm text-slate-500" aria-label="Breadcrumb">
				<a href={resolve('/assets')} class="hover:text-slate-950">Assets</a>
				<span class="mx-1">/</span><span>{friendlyName}</span>
			</nav>
			<div class="mt-3 flex flex-wrap items-center gap-3">
				<h1 class="text-3xl font-semibold tracking-tight">{friendlyName}</h1>
				<StatusBadge status={health} />
			</div>
			<div class="mt-3 flex max-w-4xl flex-col gap-2 text-sm text-slate-600">
				<div class="flex min-w-0 flex-wrap items-center gap-2">
					<span class="text-xs font-medium tracking-wide text-slate-400 uppercase">Ref</span>
					<code class="max-w-full rounded-md bg-slate-100 px-2 py-1 text-xs break-all"
						>{assetRef}</code
					>
					<Button size="sm" variant="outline" onclick={() => copy(assetRef, 'ref')}>Copy ref</Button
					>
				</div>
				<div class="flex min-w-0 flex-wrap items-center gap-2">
					<span class="text-xs font-medium tracking-wide text-slate-400 uppercase">Target</span>
					<code class="max-w-full rounded-md bg-slate-100 px-2 py-1 text-xs break-all"
						>{targetId}</code
					>
					<Button size="sm" variant="outline" onclick={() => copy(targetId, 'target')}
						>Copy target</Button
					>
				</div>
				{#if copied}
					<p class="text-xs text-slate-500" role="status">Copied {copied}.</p>
				{/if}
			</div>
		</div>

		<div class="flex flex-wrap gap-2">
			<Button disabled={!canSubmitAsset} onclick={() => openConfirmation('asset')}>
				{assetOnlyAvailable ? 'Run asset' : 'Asset-only run unavailable'}
			</Button>
			<Button
				variant="outline"
				disabled={!canSubmitWithDependencies}
				onclick={() => openConfirmation('with_dependencies')}
			>
				Run with dependencies
			</Button>
		</div>
	</div>

	{#if pendingScope}
		<Card.Root
			class="border-slate-300 bg-slate-50"
			role="dialog"
			aria-labelledby="run-confirm-title"
		>
			<Card.Header>
				<h2 id="run-confirm-title" class="text-xl font-semibold tracking-tight">Confirm run</h2>
				<Card.Description>
					This will submit the selected asset target. No success state is shown until the server
					reports one.
				</Card.Description>
			</Card.Header>
			<Card.Content class="space-y-4">
				<div class="grid gap-2 text-sm md:grid-cols-2">
					<div>
						<span class="text-slate-500">Manifest</span>
						<p class="font-mono">{manifestVersionId ?? '—'}</p>
					</div>
					<div>
						<span class="text-slate-500">Scope</span>
						<p>{pendingScope === 'asset' ? 'Asset only' : 'With dependencies'}</p>
					</div>
				</div>
				{#if pendingScope === 'with_dependencies' && !dependenciesAvailable}
					<p class="rounded-md border border-dashed bg-white p-3 text-sm text-slate-600">
						Dependency metadata is not exposed for this asset. The run target can still be
						submitted, but this UI cannot preview expanded dependencies.
					</p>
				{/if}
				<div class="flex flex-wrap gap-2">
					{#if onrun}
						<Button onclick={confirmCallbackRun}>Submit run request</Button>
					{:else}
						<form
							method="POST"
							action={pendingScope === 'asset'
								? (assetAction ?? undefined)
								: (withDependenciesAction ?? undefined)}
						>
							<input type="hidden" name="target" value={targetId} />
							<input type="hidden" name="asset_ref" value={assetRef} />
							<input type="hidden" name="scope" value={pendingScope} />
							{#if manifestVersionId}<input
									type="hidden"
									name="manifest_version_id"
									value={manifestVersionId}
								/>{/if}
							<Button type="submit">Submit run request</Button>
						</form>
					{/if}
					<Button variant="ghost" onclick={() => (pendingScope = null)}>Cancel</Button>
				</div>
			</Card.Content>
		</Card.Root>
	{/if}

	<div class="flex flex-wrap gap-2 border-b pb-2" role="tablist" aria-label="Asset detail tabs">
		{#each tabs as tab (tab.id)}
			<button
				type="button"
				role="tab"
				id={tabId(tab.id)}
				class={tabClass(tab.id)}
				onclick={() => (activeTab = tab.id)}
				aria-selected={activeTab === tab.id}
				aria-controls={tabPanelId(tab.id)}
			>
				{tab.label}
			</button>
		{/each}
	</div>

	{#if activeTab === 'overview'}
		<div
			id={tabPanelId('overview')}
			role="tabpanel"
			aria-labelledby={tabId('overview')}
			class="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(18rem,1fr)]"
		>
			<Card.Root>
				<Card.Header
					><h2 class="text-xl font-semibold tracking-tight">Overview</h2>
					<Card.Description>Status and manifest context.</Card.Description></Card.Header
				>
				<Card.Content class="grid gap-3 text-sm sm:grid-cols-2">
					<div>
						<span class="text-slate-500">Status</span>
						<p class="font-medium capitalize">{health}</p>
					</div>
					<div>
						<span class="text-slate-500">Last run</span>
						<p>{lastRun ? value(lastRun.id, lastRun.status) : 'No recent run reported'}</p>
					</div>
					<div>
						<span class="text-slate-500">Manifest</span>
						<p class="font-mono text-xs break-all">{manifestVersionId ?? '—'}</p>
					</div>
					<div>
						<span class="text-slate-500">Type / kind</span>
						<p>{value(asset.type)} · {value(asset.kind)}</p>
					</div>
					<div>
						<span class="text-slate-500">Domain</span>
						<p>{value(asset.domain)}</p>
					</div>
				</Card.Content>
			</Card.Root>

			<Card.Root>
				<Card.Header
					><h2 class="text-xl font-semibold tracking-tight">Capabilities</h2>
					<Card.Description>Reported by the normalized asset view.</Card.Description></Card.Header
				>
				<Card.Content class="space-y-2 text-sm text-slate-600">
					<p>
						Asset-only runs: <strong>{assetOnlyAvailable ? 'available' : 'unavailable'}</strong>
					</p>
					<p>
						Dependency preview: <strong
							>{dependenciesAvailable ? 'available' : 'not exposed'}</strong
						>
					</p>
					{#each notes as note (note)}<p class="rounded-md bg-slate-50 p-2">{note}</p>{/each}
				</Card.Content>
			</Card.Root>
		</div>

		<Card.Root>
			<Card.Header
				><h2 class="text-xl font-semibold tracking-tight">
					Runtime config declarations
				</h2></Card.Header
			>
			<Card.Content>
				{#if runtimeConfig.length === 0}
					<p class="rounded-lg border border-dashed p-6 text-sm text-slate-500">
						No runtime config refs were reported for this asset.
					</p>
				{:else}
					<div class="overflow-hidden rounded-lg border">
						<table class="w-full text-sm">
							<thead class="bg-slate-50 text-left text-xs text-slate-500 uppercase">
								<tr>
									<th class="px-3 py-2">Config path</th>
									<th class="px-3 py-2">Provider</th>
									<th class="px-3 py-2">Key</th>
									<th class="px-3 py-2">Declaration</th>
									<th class="px-3 py-2">Flags</th>
								</tr>
							</thead>
							<tbody>
								{#each runtimeConfig as entry (`${entry.path}:${entry.key}`)}
									<tr class="border-t">
										<td class="px-3 py-2 font-mono text-xs break-all">{entry.path}</td>
										<td class="px-3 py-2 font-mono text-xs break-all">{entry.provider}</td>
										<td class="px-3 py-2 font-mono text-xs break-all">{entry.key}</td>
										<td class="px-3 py-2">
											<span
												class={[
													'inline-flex rounded-full px-2 py-1 text-xs font-medium ring-1',
													runtimeStatusClass(entry.status)
												]}
											>
												{entry.status}
											</span>
										</td>
										<td class="px-3 py-2 text-slate-600">
											{entry.required ? 'required' : 'optional'} · {entry.secret
												? 'secret'
												: 'not secret'}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
					<p class="mt-3 text-xs text-slate-500">
						Secret values are never displayed. Runtime values are resolved by the runner; this web
						view only shows declared config refs and does not preflight runner environment presence.
					</p>
				{/if}
			</Card.Content>
		</Card.Root>

		<Card.Root>
			<Card.Header
				><h2 class="text-xl font-semibold tracking-tight">Schema metadata</h2></Card.Header
			>
			<Card.Content>
				{#if schemaColumns.length === 0}
					<p class="rounded-lg border border-dashed p-6 text-sm text-slate-500">
						No schema metadata was reported
					</p>
				{:else}
					<div class="overflow-hidden rounded-lg border">
						<table class="w-full text-sm">
							<thead class="bg-slate-50 text-left text-xs text-slate-500 uppercase"
								><tr><th class="px-3 py-2">Column</th><th class="px-3 py-2">Type</th></tr></thead
							>
							<tbody>
								{#each schemaColumns as column (String(column.name ?? column.field ?? column.id))}
									<tr class="border-t"
										><td class="px-3 py-2 font-mono text-xs"
											>{value(column.name, column.field, column.id)}</td
										><td class="px-3 py-2">{value(column.type, column.dataType)}</td></tr
									>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</Card.Content>
		</Card.Root>

		<Card.Root>
			<Card.Header>
				<div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
					<div>
						<h2 class="text-xl font-semibold tracking-tight">Latest materialization</h2>
						<Card.Description>
							Relation metadata and a safe preview loaded through the web BFF.
						</Card.Description>
					</div>
					<Button
						size="sm"
						variant="outline"
						disabled={!manifestVersionId || !targetId || inspectionLoading}
						onclick={loadInspection}
					>
						{inspectionLoading ? 'Loading preview…' : 'Load data preview'}
					</Button>
				</div>
			</Card.Header>
			<Card.Content class="space-y-4">
				{#if latestMaterialization}
					<div class="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
						<div>
							<span class="text-slate-500">Relation</span>
							<p class="font-mono text-xs break-all">{jsonValue(latestMaterialization.relation)}</p>
						</div>
						<div>
							<span class="text-slate-500">Rows written / affected</span>
							<p>
								{value(latestMaterialization.rowsWritten)} / {value(
									latestMaterialization.rowsAffected
								)}
							</p>
						</div>
						<div>
							<span class="text-slate-500">Loaded / materialized</span>
							<p>
								{value(latestMaterialization.loadedAt)} / {value(
									latestMaterialization.materializedAt
								)}
							</p>
						</div>
						<div>
							<span class="text-slate-500">Window</span>
							<p class="font-mono text-xs break-all">{jsonValue(latestMaterialization.window)}</p>
						</div>
					</div>
					<details class="rounded-lg border bg-slate-50 p-3 text-sm">
						<summary class="cursor-pointer font-medium">Raw materialization metadata</summary>
						<pre class="mt-3 max-h-64 overflow-auto text-xs whitespace-pre-wrap">{jsonValue(
								latestMaterialization.metadata
							)}</pre>
					</details>
				{:else}
					<p class="rounded-lg border border-dashed p-6 text-sm text-slate-500">
						No materialization metadata was reported for matching recent runs.
					</p>
				{/if}

				{#if inspectionError}
					<p
						class="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700"
						role="alert"
					>
						{inspectionError}
					</p>
				{/if}

				{#if inspection}
					<div class="space-y-4 rounded-lg border p-4">
						<div class="flex flex-wrap gap-4 text-sm">
							<div>
								<span class="text-slate-500">Row count</span>
								<p class="font-medium">{value(inspection.row_count, inspection.rowCount)}</p>
							</div>
							<div>
								<span class="text-slate-500">Preview limit</span>
								<p class="font-medium">20 rows max</p>
							</div>
						</div>

						{#each inspectionWarnings as warning (warning)}
							<p class="rounded-md border border-amber-200 bg-amber-50 p-2 text-sm text-amber-800">
								{warning}
							</p>
						{/each}
						{#each inspectionErrors as entry (entry)}
							<p class="rounded-md border border-red-200 bg-red-50 p-2 text-sm text-red-700">
								{entry}
							</p>
						{/each}

						{#if inspectionColumns.length > 0}
							<div class="overflow-hidden rounded-lg border">
								<table class="w-full text-sm">
									<thead class="bg-slate-50 text-left text-xs text-slate-500 uppercase">
										<tr><th class="px-3 py-2">Column</th><th class="px-3 py-2">Type</th></tr>
									</thead>
									<tbody>
										{#each inspectionColumns as column (String(column.name ?? column.field ?? column.id))}
											<tr class="border-t">
												<td class="px-3 py-2 font-mono text-xs"
													>{value(column.name, column.field, column.id)}</td
												>
												<td class="px-3 py-2"
													>{value(column.type, column.dataType, column.data_type)}</td
												>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}

						{#if inspectionRows.length > 0}
							<div class="overflow-auto rounded-lg border">
								<table class="w-full text-sm">
									<thead class="bg-slate-50 text-left text-xs text-slate-500 uppercase">
										<tr>
											{#each inspectionRowKeys as key (key)}<th class="px-3 py-2">{key}</th>{/each}
										</tr>
									</thead>
									<tbody>
										{#each inspectionRows as row, rowIndex (`row-${rowIndex}`)}
											<tr class="border-t">
												{#each inspectionRowKeys as key (key)}
													<td class="px-3 py-2 font-mono text-xs whitespace-nowrap"
														>{rowValue(row, key)}</td
													>
												{/each}
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{:else}
							<p class="rounded-lg border border-dashed p-4 text-sm text-slate-500">
								No sample rows were returned for this preview.
							</p>
						{/if}
					</div>
				{/if}
			</Card.Content>
		</Card.Root>
	{:else if activeTab === 'lineage'}
		<div
			id={tabPanelId('lineage')}
			role="tabpanel"
			aria-labelledby={tabId('lineage')}
			class="grid gap-4 lg:grid-cols-3"
		>
			{@render LineageList('Upstream', upstream, 'No upstream dependencies exposed.')}
			{@render LineageList('Current', [currentItem], 'Current asset unavailable.')}
			{@render LineageList('Downstream', downstream, 'No downstream dependents exposed.')}
		</div>
		{#if !dependenciesAvailable}
			<p class="rounded-lg border border-dashed bg-slate-50 p-4 text-sm text-slate-600">
				Dependencies are not exposed by this asset detail payload yet.
			</p>
		{/if}
	{:else if activeTab === 'runs'}
		<Card.Root id={tabPanelId('runs')} role="tabpanel" aria-labelledby={tabId('runs')}>
			<Card.Header
				><h2 class="text-xl font-semibold tracking-tight">Recent runs</h2>
				<Card.Description>Asset-scoped history reported by the BFF.</Card.Description></Card.Header
			>
			<Card.Content>
				{#if recentRuns.length === 0}
					<p class="rounded-lg border border-dashed p-6 text-sm text-slate-500">
						No asset-scoped recent runs were reported.
					</p>
				{:else}
					<div class="overflow-hidden rounded-lg border">
						<table class="w-full text-sm">
							<thead class="bg-slate-50 text-left text-xs text-slate-500 uppercase"
								><tr
									><th class="px-3 py-2">Run</th><th class="px-3 py-2">Status</th><th
										class="px-3 py-2">Started</th
									><th class="px-3 py-2">Duration</th><th class="px-3 py-2">Inspect</th></tr
								></thead
							>
							<tbody>
								{#each recentRuns as run (String(run.id))}
									<tr class="border-t"
										><td class="px-3 py-2 font-mono text-xs">{value(run.id)}</td><td
											class="px-3 py-2"><StatusBadge status={String(run.status ?? 'unknown')} /></td
										><td class="px-3 py-2">{value(run.startedAt, run.started_at)}</td><td
											class="px-3 py-2">{value(run.duration)}</td
										><td class="px-3 py-2"
											><a
												class="text-sm font-medium underline"
												href={resolve(`/runs/${String(run.id)}`)}>Inspect</a
											></td
										></tr
									>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</Card.Content>
		</Card.Root>
	{:else}
		<Card.Root id={tabPanelId('raw')} role="tabpanel" aria-labelledby={tabId('raw')}>
			<Card.Header
				><h2 class="text-xl font-semibold tracking-tight">Raw</h2>
				<Card.Description>Normalized asset detail payload.</Card.Description></Card.Header
			>
			<Card.Content
				><Button size="sm" variant="outline" onclick={() => copy(rawJson, 'raw JSON')}
					>Copy JSON</Button
				>
				<pre
					class="mt-3 max-h-[34rem] overflow-auto rounded-md bg-slate-950 p-4 text-xs text-slate-50">{rawJson}</pre></Card.Content
			>
		</Card.Root>
	{/if}
</section>

{#snippet LineageList(
	title: string,
	items: Array<{ label: string; detail: string | null }>,
	empty: string
)}
	<Card.Root>
		<Card.Header><h2 class="text-xl font-semibold tracking-tight">{title}</h2></Card.Header>
		<Card.Content class="space-y-2">
			{#each items as item (item.label)}
				<div class="rounded-lg border bg-white p-3">
					<p class="text-sm font-medium">{item.label}</p>
					{#if item.detail}<p class="mt-1 font-mono text-xs break-all text-slate-500">
							{item.detail}
						</p>{/if}
				</div>
			{:else}
				<p class="rounded-lg border border-dashed p-4 text-sm text-slate-500">{empty}</p>
			{/each}
		</Card.Content>
	</Card.Root>
{/snippet}
