<script lang="ts">
	let { data } = $props<{
		data: {
			session: { actor_id: string; provider: string };
			runs: Array<{ id: string; status: string | null }>;
		};
	}>();
</script>

<h1>Favn web prototype</h1>

<p>
	Signed in as <code>{data.session.actor_id}</code>
	via <code>{data.session.provider}</code>
</p>

<form method="POST" action="?/logout">
	<button type="submit">Log out</button>
</form>

<h2>Runs</h2>

{#if data.runs.length === 0}
	<p>No runs yet.</p>
{:else}
	<ul>
		{#each data.runs as run (run.id)}
			<li>
				<code>{run.id}</code>
				{#if run.status}
					- {run.status}
				{/if}
			</li>
		{/each}
	</ul>
{/if}
