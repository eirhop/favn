<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import * as Table from '$lib/components/ui/table';
	import type { OutputView } from '$lib/run_view_types';

	let { outputs } = $props<{ outputs: OutputView[] }>();
</script>

{#if outputs.length === 0}
	<p class="text-sm text-slate-500">No materialized outputs reported by this run.</p>
{:else}
	<Table.Root>
		<Table.Header>
			<Table.Row>
				<Table.Head>Relation</Table.Head>
				<Table.Head>Type</Table.Head>
				<Table.Head>Asset</Table.Head>
				<Table.Head>Connection</Table.Head>
				<Table.Head>Rows</Table.Head>
				<Table.Head>Created/Updated</Table.Head>
				<Table.Head>Actions</Table.Head>
			</Table.Row>
		</Table.Header>
		<Table.Body>
			{#each outputs as output (`${output.asset}:${output.relation}`)}
				<Table.Row>
					<Table.Cell class="font-mono text-xs">{output.relation}</Table.Cell>
					<Table.Cell>{output.type}</Table.Cell>
					<Table.Cell>{output.asset}</Table.Cell>
					<Table.Cell>{output.connection}</Table.Cell>
					<Table.Cell>{output.failed ? 'failed' : (output.rows ?? 'unavailable')}</Table.Cell>
					<Table.Cell>{output.updatedAt ?? '—'}</Table.Cell>
					<Table.Cell>
						<div class="flex gap-2">
							<Button
								size="sm"
								variant="outline"
								onclick={() => navigator.clipboard?.writeText(output.relation)}
							>
								Copy relation
							</Button>
							<Button
								size="sm"
								variant="outline"
								onclick={() =>
									navigator.clipboard?.writeText(`select * from ${output.relation} limit 100;`)}
							>
								Copy SELECT
							</Button>
						</div>
					</Table.Cell>
				</Table.Row>
			{/each}
		</Table.Body>
	</Table.Root>
{/if}
