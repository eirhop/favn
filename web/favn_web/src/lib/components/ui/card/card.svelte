<script lang="ts">
	import { cn } from '$lib/utils';
	let {
		children,
		class: className,
		...rest
	} = $props<{ children?: import('svelte').Snippet; class?: string; [key: string]: unknown }>();
</script>

<div class={cn('rounded-xl border bg-white text-slate-950 shadow-sm', className)} {...rest}>
	{@render children?.()}
</div>
