<script lang="ts">
	import { cn } from '$lib/utils';
	let {
		children,
		class: className,
		...rest
	} = $props<{ children?: import('svelte').Snippet; class?: string; [key: string]: unknown }>();
</script>

<caption class={cn('mt-4 text-sm text-slate-500', className)} {...rest}
	>{@render children?.()}</caption
>
