<script lang="ts">
	let { form } = $props();
</script>

<h1>Login</h1>

{#if form?.message}
	<p>{form.message}</p>
{/if}

<form method="POST">
	<label>
		Username
		<input
			type="text"
			name="username"
			autocomplete="username"
			required
			value={form?.username ?? ''}
		/>
	</label>

	<label>
		Password
		<input type="password" name="password" autocomplete="current-password" required />
	</label>

	<button type="submit">Log in</button>
</form>
