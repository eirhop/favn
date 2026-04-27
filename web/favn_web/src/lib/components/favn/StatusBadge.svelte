<script lang="ts">
	import { Badge } from '$lib/components/ui/badge';
	import type { RunStatus } from '$lib/run_view_types';

	let { status } = $props<{ status: RunStatus | string | null }>();

	let normalized = $derived((status ?? 'unknown').toString().toLowerCase());
	let variant = $derived<'default' | 'secondary' | 'outline' | 'destructive'>(
		normalized === 'failed'
			? 'destructive'
			: normalized === 'cancelled'
				? 'secondary'
				: normalized === 'pending' || normalized === 'queued'
					? 'outline'
					: normalized === 'running'
						? 'secondary'
						: 'default'
	);
</script>

<Badge {variant} class="capitalize">
	{#if normalized === 'running'}
		<span class="size-2 animate-pulse rounded-full bg-current"></span>
	{/if}
	{normalized}
</Badge>
