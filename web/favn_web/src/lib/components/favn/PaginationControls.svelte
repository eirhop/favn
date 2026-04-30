<script lang="ts">
	import { page } from '$app/state';
	import { Button } from '$lib/components/ui/button';
	import type { PaginationView } from '$lib/backfill_view_types';
	import { buildPaginationLinks } from '$lib/backfill_pagination';

	let { pagination } = $props<{ pagination: PaginationView }>();

	let links = $derived(buildPaginationLinks(page.url.pathname, page.url.searchParams, pagination));
	let rangeLabel = $derived(
		links.limit === null
			? `Offset ${links.currentOffset}`
			: `Offset ${links.currentOffset} · limit ${links.limit}`
	);
</script>

<nav
	class="flex flex-col gap-2 text-sm text-slate-600 sm:flex-row sm:items-center sm:justify-between"
	aria-label="Pagination"
>
	<p>{rangeLabel}</p>
	<div class="flex gap-2">
		{#if links.previousHref}
			<Button href={links.previousHref} variant="outline" size="sm">Previous</Button>
		{:else}
			<Button type="button" variant="outline" size="sm" disabled>Previous</Button>
		{/if}
		{#if links.nextHref}
			<Button href={links.nextHref} variant="outline" size="sm">Next</Button>
		{:else}
			<Button type="button" variant="outline" size="sm" disabled>Next</Button>
		{/if}
	</div>
</nav>
