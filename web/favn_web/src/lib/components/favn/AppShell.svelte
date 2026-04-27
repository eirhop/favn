<script lang="ts">
	import { page } from '$app/state';
	import { resolve } from '$app/paths';
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';

	type Session = { actor_id: string; provider: string };

	let { session, activeManifestVersionId, children } = $props<{
		session: Session;
		activeManifestVersionId: string | null;
		children?: import('svelte').Snippet;
	}>();

	const navItems = [
		{ label: 'Runs', href: '/runs', complete: true },
		{ label: 'Manifests', href: null, complete: false },
		{ label: 'Schedules', href: null, complete: false },
		{ label: 'Settings', href: null, complete: false }
	];

	let currentPath = $derived(page.url.pathname);
</script>

<div class="min-h-screen bg-slate-50 text-slate-950">
	<div class="flex min-h-screen">
		<aside class="hidden w-64 border-r bg-white lg:block" aria-label="Primary">
			<div class="border-b p-5">
				<p class="text-lg font-semibold">Favn</p>
				<p class="text-xs text-slate-500">Local development</p>
			</div>
			<nav class="space-y-1 p-3">
				{#each navItems as item (item.label)}
					{@const active =
						item.href !== null &&
						(currentPath === item.href || currentPath.startsWith(`${item.href}/`))}
					<svelte:element
						this={item.href ? 'a' : 'span'}
						href={item.href === '/runs' ? resolve('/runs') : undefined}
						class={[
							'flex items-center justify-between rounded-md px-3 py-2 text-sm font-medium',
							active
								? 'bg-slate-950 text-white'
								: item.href
									? 'text-slate-600 hover:bg-slate-100 hover:text-slate-950'
									: 'cursor-not-allowed text-slate-400'
						]}
						aria-current={active ? 'page' : undefined}
						aria-disabled={item.href ? undefined : 'true'}
					>
						<span>{item.label}</span>
						{#if !item.complete}<span class="text-[10px] opacity-70">soon</span>{/if}
					</svelte:element>
				{/each}
			</nav>
		</aside>

		<div class="flex min-w-0 flex-1 flex-col">
			<header class="sticky top-0 z-10 border-b bg-white/90 backdrop-blur">
				<div
					class="flex flex-col gap-3 px-4 py-3 lg:flex-row lg:items-center lg:justify-between lg:px-6"
				>
					<div>
						<div class="flex items-center gap-2 text-sm font-medium">
							<span>Favn</span>
							<span class="h-4 w-px bg-slate-200" aria-hidden="true"></span>
							<span class="text-slate-500">Local development</span>
						</div>
						<nav class="mt-1 text-xs text-slate-500" aria-label="Breadcrumb">
							<a href={resolve('/runs')} class="hover:text-slate-950">Runs</a>
							{#if currentPath !== '/runs'}
								<span class="mx-1">/</span><span
									>{currentPath.split('/').filter(Boolean).at(-1)}</span
								>
							{/if}
						</nav>
					</div>

					<div class="flex flex-wrap items-center gap-2 text-xs text-slate-600">
						<Badge variant="outline">Active manifest: {activeManifestVersionId ?? 'none'}</Badge>
						<Badge variant="secondary">Storage: local</Badge>
						<Badge variant="outline">Scheduler: local</Badge>
						<Button
							href="/api/web/v1/streams/runs"
							variant="outline"
							size="sm"
							title="Open raw runs event stream">Open logs</Button
						>
						<Button href={currentPath} variant="outline" size="sm" title="Reload this page"
							>Refresh</Button
						>
						<details class="relative">
							<summary
								class="flex cursor-pointer list-none items-center gap-2 rounded-md border bg-white px-2 py-1.5 text-sm shadow-sm"
							>
								<span
									class="grid size-6 place-items-center rounded-full bg-slate-950 text-[10px] font-bold text-white"
									>LO</span
								>
								<span>local-operator</span>
							</summary>
							<div class="absolute right-0 mt-2 w-56 rounded-md border bg-white p-2 shadow-lg">
								<p class="px-2 py-1 text-xs text-slate-500">Signed in as</p>
								<p class="px-2 pb-2 font-mono text-xs">{session.actor_id}</p>
								<form method="POST" action="?/logout">
									<Button type="submit" variant="ghost" size="sm" class="w-full justify-start"
										>Log out</Button
									>
								</form>
							</div>
						</details>
					</div>
				</div>
			</header>

			<main class="flex-1 px-4 py-6 lg:px-6">{@render children?.()}</main>
		</div>
	</div>
</div>
