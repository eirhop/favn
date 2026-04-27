<script lang="ts">
	import { cn } from '$lib/utils';

	let {
		children,
		class: className,
		for: forId,
		...rest
	} = $props<{
		children?: import('svelte').Snippet;
		class?: string;
		for?: string;
		[key: string]: unknown;
	}>();
</script>

<label
	for={forId}
	class={cn(
		'text-sm leading-none font-medium peer-disabled:cursor-not-allowed peer-disabled:opacity-70',
		className
	)}
	{...rest}
>
	{@render children?.()}
</label>
