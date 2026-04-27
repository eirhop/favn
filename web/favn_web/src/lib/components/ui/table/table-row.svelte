<script lang="ts">
	import { cn } from '$lib/utils';
	let {
		children,
		class: className,
		...rest
	} = $props<{ children?: import('svelte').Snippet; class?: string; [key: string]: unknown }>();
</script>

<tr class={cn('border-b transition-colors hover:bg-slate-50', className)} {...rest}
	>{@render children?.()}</tr
>
