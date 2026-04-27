<script lang="ts">
	import { cn } from '$lib/utils';

	let {
		class: className,
		type = 'text',
		value,
		...rest
	} = $props<{
		class?: string;
		type?: string;
		value?: string;
		[key: string]: unknown;
	}>();
</script>

<input
	class={cn(
		'flex h-9 w-full rounded-md border border-slate-200 bg-transparent px-3 py-1 text-base shadow-sm transition-colors placeholder:text-slate-500 focus-visible:ring-1 focus-visible:ring-slate-950 focus-visible:outline-none disabled:cursor-not-allowed disabled:opacity-50 md:text-sm',
		className
	)}
	{type}
	{value}
	{...rest}
/>
