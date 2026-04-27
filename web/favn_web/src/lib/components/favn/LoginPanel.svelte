<script lang="ts">
	import * as Alert from '$lib/components/ui/alert';
	import { Button } from '$lib/components/ui/button';
	import * as Card from '$lib/components/ui/card';
	import { Input } from '$lib/components/ui/input';
	import { Label } from '$lib/components/ui/label';

	type LoginFormState = {
		message?: string;
		username?: string;
	};

	let { form, localAdminConfigured = false } = $props<{
		form?: LoginFormState | null;
		localAdminConfigured?: boolean;
	}>();
</script>

<main class="grid min-h-screen place-items-center px-6 py-12">
	<div class="w-full max-w-md space-y-6">
		<div class="space-y-2 text-center">
			<p class="text-sm font-medium tracking-[0.3em] text-slate-500 uppercase">Favn</p>
			<h1 class="text-3xl font-semibold tracking-tight">Admin sign in</h1>
			<p class="text-sm text-slate-600">Open the local control-plane prototype.</p>
		</div>

		<Card.Root>
			<Card.Header>
				<Card.Title>Login</Card.Title>
				<Card.Description>
					Use orchestrator credentials or the web-local admin configured in <code>.env</code>.
				</Card.Description>
			</Card.Header>
			<Card.Content>
				{#if form?.message}
					<Alert.Root variant="destructive" class="mb-6">
						<Alert.Title>Unable to sign in</Alert.Title>
						<Alert.Description>{form.message}</Alert.Description>
					</Alert.Root>
				{/if}

				<form method="POST" class="space-y-5">
					<div class="grid gap-2">
						<Label for="username">Username</Label>
						<Input
							id="username"
							type="text"
							name="username"
							autocomplete="username"
							required
							value={form?.username ?? ''}
						/>
					</div>

					<div class="grid gap-2">
						<Label for="password">Password</Label>
						<Input
							id="password"
							type="password"
							name="password"
							autocomplete="current-password"
							required
						/>
					</div>

					<Button type="submit" class="w-full">Log in</Button>
				</form>
			</Card.Content>
			<Card.Footer>
				<p class="text-xs text-slate-500">
					Web-local admin is {localAdminConfigured ? 'configured' : 'not configured'}.
				</p>
			</Card.Footer>
		</Card.Root>
	</div>
</main>
