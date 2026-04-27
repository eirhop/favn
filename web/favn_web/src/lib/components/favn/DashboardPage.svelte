<script lang="ts">
	import * as Alert from '$lib/components/ui/alert';
	import { Badge } from '$lib/components/ui/badge';
	import { Button } from '$lib/components/ui/button';
	import * as Card from '$lib/components/ui/card';
	import * as Table from '$lib/components/ui/table';

	type Session = { actor_id: string; provider: string };
	type Run = { id: string; status: string | null; target: string | null };
	type Schedule = { id: string; enabled: boolean | null; target: string | null };

	let { session, runs, schedules, activeManifestVersionId, orchestratorWarning } = $props<{
		session: Session;
		runs: Run[];
		schedules: Schedule[];
		activeManifestVersionId: string | null;
		orchestratorWarning: string | null;
	}>();

	const statusVariant = (
		status: string | null
	): 'default' | 'secondary' | 'outline' | 'destructive' => {
		if (status === 'succeeded') return 'default';
		if (status === 'failed' || status === 'cancelled') return 'destructive';
		if (status === 'running' || status === 'queued') return 'secondary';
		return 'outline';
	};
</script>

<main class="mx-auto flex min-h-screen w-full max-w-6xl flex-col gap-8 px-6 py-8">
	<header
		class="flex flex-col gap-4 rounded-2xl border bg-white/80 p-6 shadow-sm backdrop-blur sm:flex-row sm:items-center sm:justify-between"
	>
		<div>
			<p class="text-sm font-medium tracking-[0.3em] text-slate-500 uppercase">Favn</p>
			<h1 class="mt-2 text-3xl font-semibold tracking-tight">Favn web prototype</h1>
			<p class="mt-2 text-sm text-slate-600">
				Signed in as <code>{session.actor_id}</code> via <code>{session.provider}</code>
			</p>
		</div>
		<form method="POST" action="?/logout">
			<Button type="submit" variant="outline">Log out</Button>
		</form>
	</header>

	{#if orchestratorWarning}
		<Alert.Root>
			<Alert.Title>Prototype local admin mode</Alert.Title>
			<Alert.Description>{orchestratorWarning}</Alert.Description>
		</Alert.Root>
	{/if}

	<section class="grid gap-4 md:grid-cols-3">
		<Card.Root>
			<Card.Header>
				<Card.Description>Active manifest</Card.Description>
				<Card.Title class="text-xl">{activeManifestVersionId ?? 'Not available'}</Card.Title>
			</Card.Header>
		</Card.Root>
		<Card.Root>
			<Card.Header>
				<Card.Description>Runs</Card.Description>
				<Card.Title class="text-xl">{runs.length}</Card.Title>
			</Card.Header>
		</Card.Root>
		<Card.Root>
			<Card.Header>
				<Card.Description>Schedules</Card.Description>
				<Card.Title class="text-xl">{schedules.length}</Card.Title>
			</Card.Header>
		</Card.Root>
	</section>

	<section class="grid gap-6 lg:grid-cols-[1fr_24rem]">
		<Card.Root>
			<Card.Header>
				<Card.Title>Runs</Card.Title>
				<Card.Description>Recent orchestrator work visible through the web BFF.</Card.Description>
			</Card.Header>
			<Card.Content>
				{#if runs.length === 0}
					<div class="rounded-lg border border-dashed p-8 text-center text-sm text-slate-500">
						No runs yet.
					</div>
				{:else}
					<Table.Root>
						<Table.Header>
							<Table.Row>
								<Table.Head>Run</Table.Head>
								<Table.Head>Status</Table.Head>
								<Table.Head>Target</Table.Head>
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each runs as run (run.id)}
								<Table.Row>
									<Table.Cell class="font-mono font-medium">{run.id}</Table.Cell>
									<Table.Cell
										><Badge variant={statusVariant(run.status)}>{run.status ?? 'unknown'}</Badge
										></Table.Cell
									>
									<Table.Cell class="text-slate-600">{run.target ?? '—'}</Table.Cell>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				{/if}
			</Card.Content>
		</Card.Root>

		<Card.Root>
			<Card.Header>
				<Card.Title>Schedules</Card.Title>
				<Card.Description>Active manifest schedule entries.</Card.Description>
			</Card.Header>
			<Card.Content>
				{#if schedules.length === 0}
					<p class="text-sm text-slate-500">No schedules available.</p>
				{:else}
					<ul class="space-y-3">
						{#each schedules as schedule (schedule.id)}
							<li class="rounded-lg border p-3">
								<div class="flex items-center justify-between gap-3">
									<span class="font-mono text-sm">{schedule.id}</span>
									<Badge variant={schedule.enabled ? 'default' : 'outline'}
										>{schedule.enabled ? 'enabled' : 'disabled'}</Badge
									>
								</div>
								<p class="mt-2 text-xs text-slate-500">{schedule.target ?? 'No target'}</p>
							</li>
						{/each}
					</ul>
				{/if}
			</Card.Content>
		</Card.Root>
	</section>
</main>
