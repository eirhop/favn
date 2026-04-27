<script lang="ts">
	import { cn } from '$lib/utils';
	let {
		children,
		class: className,
		...rest
	} = $props<{ children?: import('svelte').Snippet; class?: string; [key: string]: unknown }>();
</script>

<h5 class={cn('mb-1 leading-none font-medium tracking-tight', className)} {...rest}>
	{@render children?.()}
</h5>
