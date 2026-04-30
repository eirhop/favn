<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { resolve } from '$app/paths';
	import { Badge } from '$lib/components/ui/badge';
	import * as Table from '$lib/components/ui/table';
	import type { BackfillWindowView } from '$lib/backfill_view_types';

	let { windows = [], backfillRunId } = $props<{
		windows?: BackfillWindowView[];
		backfillRunId: string;
	}>();

	let rerunMessage = $state<string | null>(null);
	let rerunningWindowKey = $state<string | null>(null);

	async function rerunWindow(windowKey: string) {
		if (!confirm(`Rerun failed backfill window ${windowKey}?`)) return;
		rerunMessage = null;
		rerunningWindowKey = windowKey;
		try {
			const response = await fetch(
				`/api/web/v1/backfills/${encodeURIComponent(backfillRunId)}/windows/rerun`,
				{
					method: 'POST',
					headers: { accept: 'application/json', 'content-type': 'application/json' },
					body: JSON.stringify({ window_key: windowKey })
				}
			);
			const payload = await response.json().catch(() => null);
			if (!response.ok) {
				const message =
					typeof payload?.error?.message === 'string'
						? payload.error.message
						: `Rerun failed with HTTP ${response.status}.`;
				rerunMessage = message;
				return;
			}
			rerunMessage = `Rerun accepted for ${windowKey}.`;
		} catch {
			rerunMessage = 'Rerun failed because the web API was unreachable.';
		} finally {
			rerunningWindowKey = null;
		}
	}
</script>

<div class="space-y-3">
	{#if rerunMessage}<p class="text-sm text-slate-700">{rerunMessage}</p>{/if}
	{#if windows.length === 0}
		<div class="rounded-lg border border-dashed bg-white p-4 text-sm text-slate-600">
			No child windows are visible yet. The parent may still be planning windows or filters may
			exclude rows.
		</div>
	{:else}
		<div class="overflow-hidden rounded-lg border bg-white">
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>Window</Table.Head>
						<Table.Head>Status</Table.Head>
						<Table.Head>Attempts</Table.Head>
						<Table.Head>Latest run</Table.Head>
						<Table.Head>Updated</Table.Head>
						<Table.Head>Action</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each windows as window (window.windowKey)}
						<Table.Row>
							<Table.Cell>
								<p class="font-mono text-xs">{window.windowKey}</p>
								<p class="text-xs text-slate-500">
									{window.windowStartAt ?? '—'} → {window.windowEndAt ?? '—'} · {window.timezone ??
										'tz?'}
								</p>
							</Table.Cell>
							<Table.Cell><Badge variant="outline">{window.status}</Badge></Table.Cell>
							<Table.Cell>{window.attemptCount ?? '—'}</Table.Cell>
							<Table.Cell>
								{#if window.latestAttemptRunId}
									<a
										class="font-mono text-xs underline"
										href={resolve(`/runs/${window.latestAttemptRunId}`)}
										>{window.latestAttemptRunId}</a
									>
								{:else}—{/if}
							</Table.Cell>
							<Table.Cell>{window.updatedAt ?? '—'}</Table.Cell>
							<Table.Cell>
								{#if window.canRerun}
									<Button
										type="button"
										variant="outline"
										size="sm"
										disabled={rerunningWindowKey === window.windowKey}
										onclick={() => rerunWindow(window.windowKey)}
									>
										Rerun
									</Button>
								{:else}—{/if}
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</div>
	{/if}
</div>
