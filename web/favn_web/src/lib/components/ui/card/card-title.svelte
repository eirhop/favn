<script lang="ts">
	import { cn } from '$lib/utils';
	let {
		children,
		class: className,
		...rest
	} = $props<{ children?: import('svelte').Snippet; class?: string; [key: string]: unknown }>();
</script>

<h3 class={cn('text-2xl leading-none font-semibold tracking-tight', className)} {...rest}>
	{@render children?.()}
</h3>
